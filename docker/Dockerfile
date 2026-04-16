FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3.3.4

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    redis-server \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Normalize JAVA_HOME to an architecture-agnostic path.
RUN arch="$(dpkg --print-architecture)" && \
    ln -s "/usr/lib/jvm/java-21-openjdk-${arch}" /usr/lib/jvm/java-21-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk

# Configure Redis for in-memory only
RUN sed -i 's/^save/# save/' /etc/redis/redis.conf && \
    echo "save \"\"" >> /etc/redis/redis.conf && \
    echo "appendonly no" >> /etc/redis/redis.conf

# Use filesystem root so runtime paths map directly to /config and /src
WORKDIR /

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and pipeline config (YAML + SQL)
COPY src/ src/
COPY config/ config/

# Set Python path
ENV PYTHONPATH=/

# Create startup script
COPY docker/startup.sh /startup.sh
RUN chmod +x /startup.sh

# Set entrypoint
ENTRYPOINT ["/startup.sh"] 