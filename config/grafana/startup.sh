#!/bin/bash

# MetaLayer Grafana Startup Script
# Ensures proper Grafana initialization and prevents dashboard conflicts

echo "🚀 Starting MetaLayer Grafana with optimized configuration..."

# Wait for Prometheus to be ready
echo "⏳ Waiting for Prometheus to be ready..."
while ! nc -z prometheus 9090; do
  sleep 2
done
echo "✅ Prometheus is ready!"

# Start Grafana with clean configuration
echo "🎯 Starting Grafana server..."
exec /run.sh "$@"