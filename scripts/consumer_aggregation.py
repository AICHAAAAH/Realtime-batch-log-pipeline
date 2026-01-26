from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
import os

def connect_kafka():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP', 'kafka1:9092,kafka2:9092,kafka3:9092').split(',')
    
    while True:
        try:
            print(f"Connecting to Kafka cluster: {bootstrap_servers}...")
            return KafkaConsumer(
                'RAWLOG',
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='aggregation-group'
            )
        except NoBrokersAvailable:
            print("Kafka cluster not ready, retrying in 3s...")
            time.sleep(3)

# Configuration
WINDOW_MINUTES = int(os.getenv('PERIOD_MINUTES', '5'))
TOP_N = int(os.getenv('TOP_N', '3'))

print("="*70)
print("AGGREGATION CONSUMER STARTING (CLUSTER MODE)")
print(f"Window: {WINDOW_MINUTES} minutes")
print(f"Top N: {TOP_N}")
print("="*70)

consumer = connect_kafka()
path_counts = defaultdict(int)
window_start = datetime.now()

print(f"Window started: {window_start.strftime('%Y-%m-%d %H:%M:%S')}")

for msg in consumer:
    try:
        raw = msg.value
        message = raw["message"]
        
        json_start = message.find("{")
        if json_start == -1:
            continue
            
        json_str = message[json_start:]
        log = json.loads(json_str)
        
        path_counts[log['path']] += 1
        
        # Check if window expired
        if (datetime.now() - window_start).total_seconds() >= (WINDOW_MINUTES * 60):
            # Sort and get top N
            top_paths = sorted(path_counts.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
            
            print(f"\n{'='*70}")
            print(f"Window: {window_start.strftime('%H:%M')} - {datetime.now().strftime('%H:%M')}")
            print(f"Top {TOP_N} pages:")
            for rank, (path, count) in enumerate(top_paths, 1):
                print(f"  {rank}. {path}: {count} visits")
            print(f"{'='*70}\n")
            
            # Reset
            path_counts.clear()
            window_start = datetime.now()
            
    except Exception as e:
        print(f"Error: {e}")
        continue