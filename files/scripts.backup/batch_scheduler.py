import schedule
import time
from datetime import datetime
import subprocess
import os

# Get N from environment
N = int(os.getenv('TOP_N', '3'))

def run_batch_job():
    """Run the batch aggregation script at 8 PM daily"""
    print(f"\n{'='*70}")
    print(f"{datetime.now()}: Starting scheduled batch aggregation...")
    print(f"Configuration: Top N={N} pages")
    print(f"{'='*70}")
    try:
        result = subprocess.run(['python', '/app/batch_aggregation.py'], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(f"Errors: {result.stderr}")
        print(f"{'='*70}")
        print(f"{datetime.now()}: Batch aggregation completed.")
        print(f"{'='*70}\n")
    except Exception as e:
        print(f"Error running batch: {e}")

# Schedule the job to run at 8 PM every day (20:00 in 24-hour format)
schedule.every().day.at("20:00").do(run_batch_job)

print("=" * 70)
print("BATCH SCHEDULER STARTED")
print("=" * 70)
print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Configuration: Top N={N} pages")
print(f"Scheduled to run: DAILY at 20:00 (8:00 PM)")
print("=" * 70)
print("\nWaiting for scheduled time...")
print("(The batch will run automatically at 8:00 PM)")
print("=" * 70)

# Keep the script running and check schedule every minute
while True:
    schedule.run_pending()
    time.sleep(60)
