#!/bin/bash
set -e

echo "Starting MetaLayer services..."

# Start Airflow webserver in background
echo "Starting Airflow webserver..."
airflow webserver &

# Wait a moment for webserver to initialize
sleep 5

# Start metrics exporter
echo "Starting MetaLayer metrics exporter..."
cd /opt/airflow
python scripts/start_metrics_exporter.py &

# Keep the container running
wait