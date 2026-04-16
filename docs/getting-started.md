# Getting Started

## Table of Contents

- [Docker Quick Try](#docker-quick-try)
- [Full Local Setup](#full-local-setup)
- [Usage](#usage)
- [CLI Options](#cli-options)

## Docker Quick Try

The easiest way to run the pipeline. Requires Docker and Docker Compose.

### Prerequisites

- Docker and Docker Compose

### Steps

1. **Clone and configure**
   ```bash
   git clone https://github.com/victorlou/spine.git
   cd spine
   cp config/defaults.example.yml config/defaults.yml
   cp config/examples/jsonplaceholder.yml config/sources/jsonplaceholder.yml
   cp .env.example .env
   # Edit .env with your API credentials
   ```

2. **Build and run**
   ```bash
   docker-compose up --build
   ```

### Passing CLI arguments

`docker-compose up` runs the default pipeline. For `--show-plan`, `--validate-only`, `--select`, etc., use `docker run`:

```bash
# Build the image first (or use an existing one from ghcr.io)
docker build --platform linux/amd64 -t spine -f docker/Dockerfile .

# Run with args
docker run --rm -v "$(pwd)/.env:/.env:ro" -v "$(pwd)/config:/config:ro" spine --show-plan
docker run --rm -v "$(pwd)/.env:/.env:ro" -v "$(pwd)/config:/config:ro" spine --validate-only
docker run --rm -v "$(pwd)/.env:/.env:ro" -v "$(pwd)/config:/config:ro" spine --select jsonplaceholder --limit 5
```

**Apple Silicon (M1/M2)**: Add `--platform linux/amd64` to the build for compatibility.

See [docker/README.md](../docker/README.md) for more Docker details.

---

## Full Local Setup

For active development: customizing configs, debugging, or contributing.

### Prerequisites

- Python 3.12+
- **Java 17** (required for Spark)
- Redis 7.0+ (for context management)
- AWS credentials (for S3 access)
- API credentials for your data sources

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/victorlou/spine.git
   cd spine
   cp config/defaults.example.yml config/defaults.yml
   cp config/examples/jsonplaceholder.yml config/sources/jsonplaceholder.yml
   ```

2. **Install Java 17** (required for Spark)
   ```bash
   # macOS
   brew install openjdk@17

   # Linux (Ubuntu/Debian)
   sudo apt-get update
   sudo apt-get install openjdk-17-jdk

   java -version  # Should show Java 17
   ```

3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install and start Redis**

   **macOS (using Homebrew)**
   ```bash
   brew install redis
   brew services start redis
   redis-cli ping  # Should return "PONG"
   ```

   **Linux (Ubuntu/Debian)**
   ```bash
   sudo apt-get update
   sudo apt-get install redis-server
   sudo systemctl start redis-server
   redis-cli ping  # Should return "PONG"
   ```

   **Windows (WSL2 recommended)**
   ```bash
   wsl --install
   # After WSL2, install Redis in Ubuntu WSL as above
   ```

   **Windows (Memurai alternative)**
   - Install [Memurai](https://www.memurai.com/get-memurai)
   - Verify: `"C:\Program Files\Memurai\memurai-cli.exe" ping`

   **To stop Redis**: macOS `brew services stop redis` | Linux `sudo systemctl stop redis-server` | Windows `net stop Memurai`

5. **Set up environment variables**
   ```bash
   cp .env.example .env
   ```
   Configure your `.env` with API credentials.

   **Production:** inject variables with your scheduler or platform (for example Kubernetes `envFrom`, ECS task definitions, or `docker run --env-file`) instead of relying on a mounted `.env` file.

---

## Usage

**Show execution plan** (recommended first step)
```bash
python -m src.main --show-plan
```

**Validate configuration**
```bash
python -m src.main --validate-only
```

**Run the full pipeline**
```bash
python -m src.main
```

**Run with specific options**
```bash
python -m src.main --log-level TRACE
python -m src.main --select jsonplaceholder
python -m src.main --select jsonplaceholder:posts
python -m src.main --limit 10
python -m src.main --backfill
python -m src.main --select jsonplaceholder --limit 5 --log-level TRACE
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--validate-only` | Validate configuration without executing |
| `--show-plan` | Show execution plan without validation or execution |
| `--log-level` | Set log level (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `--select` | Comma-separated source or `source:resource` selections |
| `--limit` | Limit API requests per resource (0 = skip). When used, data is NOT saved to S3 |
| `--backfill`, `-b` | Force backfill date ranges instead of default dates |
