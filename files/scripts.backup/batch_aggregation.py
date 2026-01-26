from cassandra.cluster import Cluster
from datetime import datetime
from collections import Counter

# Configuration
N = 3  # Top N pages

def connect_cassandra():
    print("Connecting to Cassandra...")
    cluster = Cluster(['cassandra'])
    return cluster.connect('csc5355')

session = connect_cassandra()

today = datetime.now().strftime("%Y-%m-%d")
print(f"\nRunning batch aggregation for {today}")

query = "SELECT path FROM LOG WHERE day = %s ALLOW FILTERING"
rows = session.execute(query, (today,))

page_counter = Counter()
for row in rows:
    page_counter[row.path] += 1

top_pages = page_counter.most_common(N)

print(f"\n{'='*60}")
print(f"Top {N} Pages for {today}")
print(f"{'='*60}")

insert_stmt = session.prepare("""
INSERT INTO RESULTS (day, period_start, process_type, rank, path, visit_count)
VALUES (?, ?, ?, ?, ?, ?)
""")

period_start = datetime.strptime(f"{today} 00:00:00", "%Y-%m-%d %H:%M:%S")

for rank, (page, count) in enumerate(top_pages, 1):
    print(f"  {rank}. {page}: {count} visits")
    
    session.execute(insert_stmt, (
        today,
        period_start,
        'batch',
        rank,
        page,
        count
    ))

print(f"{'='*60}\n")
print("Batch aggregation complete!")
