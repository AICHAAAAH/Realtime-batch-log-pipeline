from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from cassandra.cluster import Cluster, NoHostAvailable
import json
import uuid
from datetime import datetime
import time
import os

def connect_kafka():
    # Get Kafka brokers from environment (cluster-aware)
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
            )
        except NoBrokersAvailable:
            print("Kafka cluster not ready, retrying in 3s...")
            time.sleep(3)

def connect_cassandra():
    # Get Cassandra nodes from environment (cluster-aware)
    cassandra_hosts = os.getenv('CASSANDRA_HOSTS', 'cassandra1,cassandra2,cassandra3').split(',')
    
    while True:
        try:
            print(f"Connecting to Cassandra cluster: {cassandra_hosts}...")
            cluster = Cluster(cassandra_hosts)
            session = cluster.connect('csc5355')
            print(f"Connected to Cassandra cluster!")
            return session
        except NoHostAvailable as e:
            print(f"Cassandra cluster not ready, retrying in 5s... ({e})")
            time.sleep(5)

print("="*70)
print("LOG CONSUMER STARTING (CLUSTER MODE)")
print("="*70)

consumer = connect_kafka()
session = connect_cassandra()

insert_stmt = session.prepare("""
INSERT INTO LOG (day, ts, path, ip, method, status, bytes, rt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

print("="*70)
print("Consumer started: RAWLOG -> Cassandra LOG")
print("Reading from Kafka cluster, writing to Cassandra ring")
print("="*70)

for msg in consumer:
    try:
        raw = msg.value
        message = raw["message"]
        
        json_start = message.find("{")
        if json_start == -1:
            continue
            
        json_str = message[json_start:]
        log = json.loads(json_str)
        
        ts_str = log['ts']
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        day = ts.strftime("%Y-%m-%d")
        
        session.execute(insert_stmt, (
            day,
            uuid.uuid1(),
            log['path'],
            log['ip'],
            log['method'],
            log['status'],
            log.get('bytes', 0),
            log.get('rt', 0.0)
        ))
        
        print(f"Logged: {log['path']} - {log['status']}")
        
    except Exception as e:
        print(f"Error: {e}")
        continue