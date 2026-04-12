#!/bin/bash

# Start Redis server in the background
redis-server /etc/redis/redis.conf --daemonize yes

# Wait for Redis to be ready
until redis-cli ping > /dev/null 2>&1; do
  echo "Waiting for Redis to be ready..."
  sleep 1
done

# Run the Python application
exec python -m src.main "$@" 