from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from cassandra.cluster import Cluster, NoHostAvailable
import json
import uuid
from datetime import datetime
import time

def connect_kafka():
    while True:
        try:
            print("Connecting to Kafka...")
            return KafkaConsumer(
                'RAWLOG',
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            print("Kafka not ready, retrying in 3s...")
            time.sleep(3)

def connect_cassandra():
    while True:
        try:
            print("Connecting to Cassandra...")
            cluster = Cluster(['cassandra'])
            session = cluster.connect('csc5355')
            return session
        except NoHostAvailable:
            print("Cassandra not ready, retrying in 5s...")
            time.sleep(5)

consumer = connect_kafka()
session = connect_cassandra()

insert_stmt = session.prepare("""
INSERT INTO LOG (day, ts, path, ip, method, status, bytes, rt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

print("Consumer started: RAWLOG -> Cassandra LOG")

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