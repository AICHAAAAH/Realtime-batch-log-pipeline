#!/bin/bash
# Run batch aggregation at 8 PM every day

# Log file
LOG_FILE="/var/log/batch_aggregation.log"

# Run the batch script
echo "$(date): Starting batch aggregation..." >> $LOG_FILE
python /app/batch_aggregation.py >> $LOG_FILE 2>&1
echo "$(date): Batch aggregation completed." >> $LOG_FILE
