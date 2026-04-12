FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3.3.4 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Java home will depend on the architecture of the machine (arm64 or amd64)
# ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    redis-server \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Configure Redis for in-memory only
RUN sed -i 's/^save/# save/' /etc/redis/redis.conf && \
    echo "save \"\"" >> /etc/redis/redis.conf && \
    echo "appendonly no" >> /etc/redis/redis.conf

# Create app directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and pipeline config (YAML + SQL)
COPY src/ src/
COPY config/ config/

# Set Python path
ENV PYTHONPATH=/app

# Create startup script
COPY docker/startup.sh /startup.sh
RUN chmod +x /startup.sh

# Set entrypoint
ENTRYPOINT ["/startup.sh"] 