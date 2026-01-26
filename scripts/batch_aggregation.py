from cassandra.cluster import Cluster, NoHostAvailable
from datetime import datetime, timedelta
import os
import time

def connect_cassandra():
    cassandra_hosts = os.getenv('CASSANDRA_HOSTS', 'cassandra1,cassandra2,cassandra3').split(',')
    
    while True:
        try:
            print(f"Connecting to Cassandra cluster: {cassandra_hosts}...")
            cluster = Cluster(cassandra_hosts)
            session = cluster.connect('csc5355')
            print(f"✅ Connected to Cassandra cluster!")
            return cluster, session
        except NoHostAvailable as e:
            print(f"Cassandra not ready, retrying in 5s... ({e})")
            time.sleep(5)

# Configuration
N = int(os.getenv('TOP_N', '3'))
today = datetime.now().strftime("%Y-%m-%d")

print("="*70)
print(f"BATCH AGGREGATION (CLUSTER MODE)")
print(f"Date: {today}")
print(f"Top N: {N}")
print("="*70)

cluster, session = connect_cassandra()

print(f"\nConnecting to Cassandra cluster...")
print(f"Running batch aggregation for {today}")

# Query all logs for today
query = f"""
    SELECT path, COUNT(*) as count
    FROM LOG
    WHERE day = '{today}'
    GROUP BY path
    ALLOW FILTERING
"""

print("Executing aggregation query...")
results = session.execute(query)

# Convert to list and sort
path_counts = [(row.path, row.count) for row in results]
path_counts.sort(key=lambda x: x[1], reverse=True)

# Get top N
top_n = path_counts[:N]

print("="*60)
print(f"Top {N} Pages for {today}")
print("="*60)

# Insert results
insert_query = session.prepare("""
    INSERT INTO RESULTS (day, period_start, process_type, rank, path, visit_count)
    VALUES (?, ?, ?, ?, ?, ?)
""")

period_start = datetime.now().replace(hour=20, minute=0, second=0, microsecond=0)

for rank, (path, count) in enumerate(top_n, start=1):
    print(f"  {rank}. {path}: {count} visits")
    session.execute(insert_query, (
        today,
        period_start,
        'batch',
        rank,
        path,
        count
    ))

print("="*60)
print("Batch aggregation complete!")

cluster.shutdown()