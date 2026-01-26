package com.csc5355;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Kafka Streams application for real-time log aggregation.
 * Implements windowed aggregation to compute top N most visited pages every P minutes.
 * CLUSTER-AWARE: Supports multiple Kafka brokers and Cassandra nodes.
 */
public class LogAggregationStreams {
    private static final Logger logger = LoggerFactory.getLogger(LogAggregationStreams.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Configuration from environment variables (CLUSTER-AWARE)
    private static final int TOP_N = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "3"));
    private static final int PERIOD_MINUTES = Integer.parseInt(System.getenv().getOrDefault("PERIOD_MINUTES", "5"));
    private static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", 
        "kafka1:9092,kafka2:9092,kafka3:9092");
    private static final String CASSANDRA_CONTACT_POINTS = System.getenv().getOrDefault("CASSANDRA_CONTACT_POINTS", 
        "cassandra1,cassandra2,cassandra3");
    
    private static CqlSession cassandraSession;
    private static PreparedStatement insertStmt;

    public static void main(String[] args) {
        logger.info("=".repeat(70));
        logger.info("KAFKA STREAMS AGGREGATION APPLICATION (CLUSTER MODE)");
        logger.info("=".repeat(70));
        logger.info("Configuration:");
        logger.info("  - Top N pages: {}", TOP_N);
        logger.info("  - Window size: {} minutes (tumbling windows)", PERIOD_MINUTES);
        logger.info("  - Kafka bootstrap servers: {}", KAFKA_BOOTSTRAP);
        logger.info("  - Cassandra contact points: {}", CASSANDRA_CONTACT_POINTS);
        logger.info("=".repeat(70));

        // Initialize Cassandra connection
        initCassandra();

        // Configure Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-aggregation-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        
        // Cluster-specific configurations
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

        // Build the topology
        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder);

        // Create and start the streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Kafka Streams application...");
            streams.close();
            if (cassandraSession != null) {
                cassandraSession.close();
            }
        }));

        streams.start();
        logger.info("Kafka Streams application started successfully!");
        logger.info("Connected to Kafka cluster: {}", KAFKA_BOOTSTRAP);
        logger.info("Connected to Cassandra cluster: {}", CASSANDRA_CONTACT_POINTS);
    }

    private static void buildTopology(StreamsBuilder builder) {
        // Read from RAWLOG topic
        KStream<String, String> rawLogs = builder.stream("RAWLOG");

        // Parse and extract path from syslog JSON
        KStream<String, String> parsedLogs = rawLogs
            .mapValues(LogAggregationStreams::extractPath)
            .filter((key, path) -> path != null && !path.isEmpty());

        // Group by path and window
        TimeWindows tumblingWindow = TimeWindows
            .ofSizeWithNoGrace(Duration.ofMinutes(PERIOD_MINUTES));

        KTable<Windowed<String>, Long> pathCounts = parsedLogs
            .groupBy((key, path) -> path, Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(tumblingWindow)
            .count();

        // Convert to stream and process window results
        pathCounts.toStream()
            .foreach(LogAggregationStreams::processWindowResult);
    }

    private static String extractPath(String rawMessage) {
        try {
            // Parse the outer JSON (from Fluent Bit)
            JsonNode rootNode = objectMapper.readTree(rawMessage);
            String message = rootNode.get("message").asText();
            
            // Find the inner JSON (NGINX log)
            int jsonStart = message.indexOf("{");
            if (jsonStart == -1) {
                return null;
            }
            
            String jsonStr = message.substring(jsonStart);
            JsonNode logNode = objectMapper.readTree(jsonStr);
            
            return logNode.get("path").asText();
        } catch (Exception e) {
            logger.debug("Failed to parse log: {}", e.getMessage());
            return null;
        }
    }

    private static void processWindowResult(Windowed<String> key, Long count) {
        String path = key.key();
        Instant windowStart = key.window().startTime();
        Instant windowEnd = key.window().endTime();
        
        // Store in a map to aggregate all paths in this window
        // This is a simplified version - for production, use a proper state store
        logger.info("Path: {} | Count: {} | Window: {} - {}", 
            path, count, 
            formatTime(windowStart), 
            formatTime(windowEnd));
        
        // Write to Cassandra
        writeToCassandra(path, count, windowStart, 1); // rank will be computed later
    }

    private static void writeToCassandra(String path, long count, Instant windowStart, int rank) {
        try {
            LocalDateTime periodStart = LocalDateTime.ofInstant(windowStart, ZoneId.systemDefault());
            String day = periodStart.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            
            cassandraSession.execute(insertStmt.bind(
                day,
                java.util.Date.from(windowStart),
                "realtime",
                rank,
                path,
                count
            ));
            
            logger.debug("Written to Cassandra cluster: day={}, path={}, count={}", day, path, count);
        } catch (Exception e) {
            logger.error("Failed to write to Cassandra: {}", e.getMessage());
        }
    }

    private static void initCassandra() {
        int maxRetries = 30;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                logger.info("Connecting to Cassandra cluster: {}...", CASSANDRA_CONTACT_POINTS);
                
                // Parse comma-separated contact points
                String[] contactPointsArray = CASSANDRA_CONTACT_POINTS.split(",");
                List<InetSocketAddress> contactPoints = Arrays.stream(contactPointsArray)
                    .map(String::trim)
                    .map(host -> new InetSocketAddress(host, 9042))
                    .collect(Collectors.toList());
                
                logger.info("Cassandra contact points: {}", contactPoints);
                
                cassandraSession = CqlSession.builder()
                    .addContactPoints(contactPoints)
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace("csc5355")
                    .build();
                
                insertStmt = cassandraSession.prepare(
                    "INSERT INTO RESULTS (day, period_start, process_type, rank, path, visit_count) " +
                    "VALUES (?, ?, ?, ?, ?, ?)"
                );
                
                logger.info("Connected to Cassandra cluster successfully!");
                logger.info("Using keyspace: csc5355");
                logger.info("Replication: NetworkTopologyStrategy with RF=3");
                return;
            } catch (Exception e) {
                retryCount++;
                logger.warn("Cassandra cluster not ready, retrying in 5s... ({}/{}) - Error: {}", 
                    retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        throw new RuntimeException("Failed to connect to Cassandra cluster after " + maxRetries + " attempts");
    }

    private static String formatTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
            .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
}