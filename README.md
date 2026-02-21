# Email Ingestion Service

An HTTP service that ingests an Enron Email Dataset and exposes the top senders via a REST API.

## How It Works

The service streams the `.tar.gz` archive without extracting it to disk. A producer coroutine reads tar entries and sends raw email bytes into a bounded channel (capacity 1000). Ten worker coroutines consume from the channel, parse the `From:` header using Jakarta Mail, and flush sender counts to Redis in batches of 500 via pipelining.

Redis sorted sets handle the ranking — `ZINCRBY` on every sender, `ZREVRANGE` to query the top N. The channel provides backpressure: if workers fall behind, the producer suspends automatically.

## Setup

**Prerequisites:** Docker and Docker Compose.

```bash
git clone https://github.com/koinsaari/email-ingestion-service.git
cd email-ingestion-service
curl -O --output-dir data https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz
docker compose up --build
```

## API

**Start ingestion:**
```
curl -X POST http://localhost:8080/start
```
Returns `202 Accepted` or `409 Conflict` if already running.

**Check status:**
```
curl http://localhost:8080/status
```
```json
{"status": "running", "messagesProcessed": 142350}
```

**Get top senders:**
```
curl http://localhost:8080/top-senders
```
```json
[
  {"email": "jeff.dasovich@enron.com", "count": 2941},
  {"email": "vince.kaminski@enron.com", "count": 2672}
]
```

## Running Tests

Tests use [Testcontainers](https://testcontainers.com/) and require Docker running:

```
./gradlew test
```

## Configuration

| Environment Variable | Default                        | Description           |
|----------------------|--------------------------------|-----------------------|
| `REDIS_URL`          | `redis://localhost:6379`       | Redis connection URL  |
| `ARCHIVE_PATH`       | `enron_mail_20150507.tar.gz`   | Path to the `.tar.gz` |

## Tech Stack

- Kotlin 2.3, JDK 21
- Ktor 3.4 (Netty)
- Redis + Lettuce 7.4
- Apache Commons Compress
- Jakarta Mail
- Testcontainers 2.0
