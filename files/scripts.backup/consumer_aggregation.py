from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from cassandra.cluster import Cluster, NoHostAvailable
import json
import time
import os
import re
from datetime import datetime
from collections import Counter

# CONFIGURABLE PARAMETERS via environment variables
N = int(os.getenv('TOP_N', '3'))  # Default: 3
P = int(os.getenv('PERIOD_MINUTES', '5'))  # Default: 5

def connect_kafka():
    while True:
        try:
            print("Connecting to Kafka...", flush=True)
            return KafkaConsumer(
                'RAWLOG',
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='aggregation_group',
            )
        except NoBrokersAvailable:
            print("Kafka not ready, retrying in 3s...", flush=True)
            time.sleep(3)

def connect_cassandra():
    while True:
        try:
            print("Connecting to Cassandra...", flush=True)
            cluster = Cluster(['cassandra'])
            session = cluster.connect('csc5355')
            return session
        except NoHostAvailable:
            print("Cassandra not ready, retrying in 5s...", flush=True)
            time.sleep(5)

print("Starting aggregation consumer...", flush=True)
consumer = connect_kafka()
session = connect_cassandra()

insert_stmt = session.prepare("""
INSERT INTO RESULTS (day, period_start, process_type, rank, path, visit_count)
VALUES (?, ?, ?, ?, ?, ?)
""")

print(f"Real-time aggregation started: Top {N} pages every {P} minutes", flush=True)

page_counter = Counter()
last_aggregation = datetime.now()

for msg in consumer:
    try:
        raw = msg.value
        message = raw["message"]
        
        json_start = message.find("{")
        if json_start == -1:
            continue
            
        json_str = message[json_start:]
        log = json.loads(json_str)
        path = log['path']
        
        page_counter[path] += 1
        
        now = datetime.now()
        elapsed = (now - last_aggregation).total_seconds() / 60
        
        if elapsed >= P:
            top_pages = page_counter.most_common(N)
            
            period_start = last_aggregation
            day = period_start.strftime("%Y-%m-%d")
            
            print(f"\n{'='*60}", flush=True)
            print(f"Period: {period_start.strftime('%H:%M')} - {now.strftime('%H:%M')}", flush=True)
            print(f"{'='*60}", flush=True)
            
            for rank, (page, count) in enumerate(top_pages, 1):
                print(f"  {rank}. {page}: {count} visits", flush=True)
                
                session.execute(insert_stmt, (
                    day,
                    period_start,
                    'realtime',
                    rank,
                    page,
                    count
                ))
            
            print(f"{'='*60}\n", flush=True)
            
            page_counter.clear()
            last_aggregation = now
            
    except Exception as e:
        print(f"Error: {e}", flush=True)
        continue