-- Create keyspace with replication factor 3
CREATE KEYSPACE IF NOT EXISTS csc5355
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
}
AND durable_writes = true;

-- Use keyspace
USE csc5355;

-- Create LOG table
CREATE TABLE IF NOT EXISTS LOG (
    day text,
    ts timeuuid,
    path text,
    ip text,
    method text,
    status int,
    bytes int,
    rt float,
    PRIMARY KEY (day, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Create RESULTS table
CREATE TABLE IF NOT EXISTS RESULTS (
    day text,
    period_start timestamp,
    process_type text,
    rank int,
    path text,
    visit_count bigint,
    PRIMARY KEY ((day, period_start, process_type), rank)
);