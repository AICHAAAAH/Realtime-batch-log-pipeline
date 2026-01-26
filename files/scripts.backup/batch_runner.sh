#!/bin/bash
# Batch processor that runs at 8 PM daily

echo "Batch processor started. Will run at 8 PM daily..."

while true; do
    current_hour=$(date +%H)
    current_min=$(date +%M)
    
    if [ "$current_hour" = "20" ] && [ "$current_min" = "00" ]; then
        echo "$(date): Running batch aggregation at 8 PM..."
        python /app/batch_aggregation.py
        echo "$(date): Batch aggregation completed."
        sleep 3600
    else
        sleep 60
    fi
done