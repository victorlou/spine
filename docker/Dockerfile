FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3.3.4 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/.venv/bin:$PATH"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    redis-server \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv

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

# Copy lock metadata first to leverage Docker cache
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

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
