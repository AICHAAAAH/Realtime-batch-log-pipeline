# Real-time & Batch Access Log Processing

## Project Overview
A scalable pipeline for real-time and batch processing of web server access logs using Kafka Streams for stream processing and scheduled batch jobs for daily aggregation.

## Architecture
```
Browser/JMeter → NGINX LB → Web Servers → Fluent Bit → Kafka
                                            ↓
                           Kafka Streams (Realtime)     consumer-log
                                            ↓                 ↓
                                      Cassandra RESULTS   Cassandra LOG
                                            ↓
                                   Batch Processor (8 PM)

```
## Components

### 1. Web Layer
- **NGINX Load Balancer** (port 8080) - Round-robin distribution
- **3x NGINX Web Servers** - Serving /product1 through /product5
- **Log Format:** Custom JSON via syslog

### 2. Message Broker
- **Kafka Brokers** - Topic: RAWLOG
- **Zookeepers** - Kafka coordination

### 3. Stream Processing
- **Fluent Bit** - Log collection and forwarding
- **Kafka Streams** (Java) - Real-time windowed aggregation
- **consumer-log** (Python) - Raw log writer

### 4. Data Storage
- **Cassandra** - Two tables:
  - `LOG` - Raw access logs (11,815+ records)
  - `RESULTS` - Aggregation results (batch + realtime)

### 5. Batch Processing
- **batch-processor** - Daily aggregation at 8 PM
- **batch-scheduler** - Python schedule library

## Technology Stack

- **Stream Processing:** Kafka Streams 3.6.1 (Java 11)
- **Message Broker:** Apache Kafka 7.6.1
- **Database:** Cassandra 4.1
- **Log Collection:** Fluent Bit 2.2
- **Web Servers:** NGINX Alpine
- **Batch Processing:** Python 3.11
- **Containerization:** Docker Compose

## Configuration

All components are configurable via environment variables:
```yaml
environment:
  - TOP_N=3              # Top N pages to track
  - PERIOD_MINUTES=5     # Aggregation window (minutes)
  - KAFKA_BOOTSTRAP=kafka:9092
  - CASSANDRA_HOST=cassandra
```

## Deployment

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM
- Ports available: 8080, 9042, 9092, 514

### Quick Start
```bash
# 1. Start all services
docker compose up -d

# 2. Wait 60 seconds for Cassandra
Start-Sleep -Seconds 60

# 3. Create Cassandra schema
docker compose exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS csc5355 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS csc5355.LOG (
    day text, ts timeuuid, path text, ip text,
    method text, status int, bytes int, rt float,
    PRIMARY KEY (day, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS csc5355.RESULTS (
    day text, period_start timestamp, process_type text,
    rank int, path text, visit_count bigint,
    PRIMARY KEY ((day, period_start, process_type), rank)
);
"

# 4. Verify all services running
docker compose ps
```

### Testing
```bash
# Generate test traffic (PowerShell)
.\scripts\generate_traffic.ps1 -TotalRequests 1000

# Verify data
docker compose exec cassandra cqlsh -e "SELECT COUNT(*) FROM csc5355.LOG;"
dSELECT * FROM csc5355.RESULTS WHERE process_type='batch' LIMIT 10 ALLOW FILTERING;
```

## Data Models

### LOG Table (Raw Logs)
```sql
CREATE TABLE LOG (
    day text,              -- Partition key (YYYY-MM-DD)
    ts timeuuid,           -- Time-ordered UUID
    path text,             -- Request path
    ip text,               -- Client IP
    method text,           -- HTTP method
    status int,            -- HTTP status
    bytes int,             -- Response size
    rt float,              -- Request time
    PRIMARY KEY (day, ts)
);
```

### RESULTS Table (Aggregations)
```sql
CREATE TABLE RESULTS (
    day text,
    period_start timestamp,
    process_type text,     -- 'realtime' or 'batch'
    rank int,
    path text,
    visit_count bigint,
    PRIMARY KEY ((day, period_start, process_type), rank)
);
```

## Project Structure
```
csc5355-pipeline/
+-- docker-compose.yml
+-- README.md
+-- fluent-bit/
   +-- fluent-bit.conf
   +-- parsers.conf
+-- nginx-lb/
   +-- nginx.conf
+-- nginx-web/
   +-- nginx.conf
+-- scripts/
   +-- consumer_log.py
   +-- batch_aggregation.py
   +-- batch_scheduler.py
   +-- generate_traffic.ps1
+-- kafka-streams-aggregation/
    +-- Dockerfile
    +-- pom.xml
    +-- src/main/java/com/csc5355/
        +-- LogAggregationStreams.java
```

## Key Features

- **Real-time Processing** - Kafka Streams with 5-minute tumbling windows  
- **Batch Processing** - Daily aggregation at 8 PM  
- **Scalable Architecture** - Horizontal scaling support  
- **Configurable Parameters** - N and P via environment variables  
- **Fault Tolerant** - Docker restart policies  
- **Persistent Storage** - Cassandra data volumes  

## Monitoring
```bash
# Check all services
docker compose ps

# View logs
docker compose logs <service-name> --tail 50

# Check Kafka Streams processing
docker compose logs kafka-streams-aggregation

# Check batch scheduler
docker compose logs batch-processor
```

## Troubleshooting

### Services not starting
```bash
docker compose down
docker compose up -d
```

### Cassandra connection issues
Wait 60-90 seconds after starting for Cassandra to fully initialize.

### No data in RESULTS
- Verify Kafka Streams is running: `docker compose ps kafka-streams-aggregation`
- Check logs: `docker compose logs kafka-streams-aggregation`
- Verify traffic generation: `docker compose logs consumer-log`

## Performance

**Current Capacity:**
- 11,815+ logs processed
- Real-time aggregation every 5 minutes
- Batch processing: 6,000+ records
- Throughput: ~200 requests/second tested

## Author
- Aicha Ajdid

