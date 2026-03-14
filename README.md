# Event-Driven Notification System

A scalable, reliable notification system built with Go, PostgreSQL, and Redis. It exposes a REST API for creating, listing, and managing notifications; an outbox pattern for guaranteed enqueue; and a worker pool that delivers messages via a configurable webhook with priority queuing, rate limiting, and circuit breakers.

## Features

- **Asynchronous processing** — Redis Streams for reliable, at-least-once message queuing
- **Priority queues** — High, normal, and low priority with separate streams and ordered consumption
- **Rate limiting** — Per-channel throughput limits (default 100/sec) backed by Redis
- **Scheduled notifications** — Future delivery with a scheduler that enqueues due items
- **Inline templates** — Request-body templates with variable substitution (e.g. `Hello {{.name}}`)
- **Reliability** — Outbox pattern, retries with backoff, dead-letter stream after max attempts
- **Observability** — Health checks, JSON metrics (queue depth, delivery counts, latency, circuit breaker), structured logging (zerolog), OpenTelemetry tracing (stdout)
- **Real-time updates** — WebSocket at `GET /v1/notifications/stream` for status changes

## Tech Stack

| Component   | Choice                          |
|------------|----------------------------------|
| Language   | Go 1.25                         |
| Database   | PostgreSQL (notifications, outbox events) |
| Queue      | Redis (Streams for queues, plus rate limiting) |
| Deployment | Docker & Docker Compose         |

---

## Getting Started

### Prerequisites

- **Go** — version in `go.mod` (e.g. 1.25.x)
- **Docker & Docker Compose** — for running the app and for integration tests (Testcontainers)

### Run the application

From the project root:

```bash
make run
```

This builds and starts the API server, PostgreSQL, and Redis. The API is available at **http://localhost:8080**. No environment file is required; defaults are used.

To stop:

```bash
make stop
```

### Environment (optional)

The app reads configuration from the environment. With Docker Compose, `DB_*`, `REDIS_*`, `PORT`, and `WEBHOOK_URL` are set in `docker-compose.yml`; you can override them with a `.env` file (see `.env.example`).

To **deliver notifications** to a real endpoint (e.g. for testing):

1. Copy the example: `cp .env.example .env`
2. Create a unique URL at [webhook.site](https://webhook.site) and set in `.env`:  
   `WEBHOOK_URL=https://webhook.site/<your-uuid>`
3. Restart: `make stop && make run`

Without a valid `WEBHOOK_URL`, the API and queue operate normally but delivery attempts will eventually fail after retries.

### Tests

```bash
# Unit tests only (no Docker required)
make test-short

# Full suite: unit + integration (Docker required)
make test
```

Integration tests use Testcontainers for PostgreSQL and Redis; the reaper is disabled by default so tests run in CI and typical dev environments.

### Run server locally (without app in Docker)

Start only Postgres and Redis, then run the server on the host:

```bash
docker compose up -d postgres redis
make run-local
```

The Makefile exports `DATABASE_URL`, `REDIS_URL`, and `PORT`; use `.env` or set `WEBHOOK_URL` if you want delivery. Migrations are baked into the custom Postgres image (`docker/postgres/Dockerfile`) and run automatically on first boot when the data volume is empty.

### Makefile reference

| Target              | Description |
|---------------------|-------------|
| `make help`         | List targets |
| `make run`          | Start app, Postgres, Redis (Docker Compose) |
| `make run-local`    | Start Postgres + Redis only, then `go run ./cmd/server` |
| `make stop`         | Stop Docker Compose services |
| `make build`        | Build binary to `./server` |
| `make test`         | Full test suite |
| `make test-short`   | Unit tests only |
| `make test-integration` | Full tests with Ryuk disabled |
| `make lint`         | Run golangci-lint |
| `make lint-fix`     | Run golangci-lint with `--fix` (auto-fix safe issues) |
| `make swagger`      | Regenerate Swagger docs |
| `make clean`        | Stop containers, remove volumes and binary |

---

## Architecture

### Overview

1. **API** — REST and WebSocket. Create (single or batch), get by ID, list with filters and cursor pagination, cancel.
2. **Persistence** — Notifications and outbox events are written to PostgreSQL in a single transaction (outbox pattern).
3. **Outbox relay** — Background process polls for unpublished events, enqueues notifications to Redis by priority, then marks events as published.
4. **Priority queue (Redis)** — Three streams (high, normal, low). Workers consume in that order so high-priority messages are delivered first.
5. **Workers** — Per-channel worker pool: consume from Redis, apply rate limit and circuit breaker, call the webhook provider; on success acknowledge the message; on repeated failure, move to dead-letter stream after max retries.
6. **Scheduler** — Periodically loads due scheduled notifications and inserts corresponding outbox events so the relay can enqueue them.

### Redis as priority queue

Redis is used as a **priority queue** implemented with **Redis Streams** and **consumer groups**:

- **Three streams** — One per priority level:
  - `stream:high`
  - `stream:normal`
  - `stream:low`
- **Enqueue** — When the outbox relay publishes a notification, it chooses the stream from the notification’s `priority` (high → `stream:high`, normal → `stream:normal`, low → `stream:low`) and appends the serialized notification with `XADD`.
- **Consumer group** — A single group name (e.g. `workers`) is used for all three streams. Workers use `XREADGROUP` so each message is delivered to one consumer and must be acknowledged with `XACK` after successful processing.
- **Consumption order** — Workers always read in priority order: first from `stream:high`, then `stream:normal`, then `stream:low`. They block for a short time per stream (configurable) so that when high is empty, normal and low still get processed instead of being starved.
- **At-least-once** — Unacknowledged messages remain in the pending list and can be claimed by other consumers or retried after a timeout.
- **Dead-letter** — After the maximum number of delivery attempts, failed notifications are written to a fourth stream (`stream:dead`) for inspection, then the original message is acknowledged so it is not retried indefinitely.

This design gives strict priority (high before normal before low) while avoiding starvation of lower-priority streams and providing durability and observability via stream lengths and pending counts (exposed in `/metrics`).

---

## API Reference

Base URL: **http://localhost:8080**. All notification resources are under `/v1`. Interactive docs: **http://localhost:8080/swagger/index.html**.

### Create notification

`POST /v1/notifications`

Create a single notification. Optionally use `idempotency_key` to avoid duplicates on retries; use `template` and `template_vars` for inline rendering; set `scheduled_at` (RFC3339) for future delivery.

**Request**

```bash
curl -s -X POST http://localhost:8080/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "+905551234567",
    "channel": "sms",
    "content": "Your OTP is 1234",
    "priority": "high",
    "idempotency_key": "my-key-1"
  }'
```

**Response** (201 Created, or 200 OK when idempotent — same key returns existing notification)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "idempotency_key": "my-key-1",
  "batch_id": null,
  "channel": "sms",
  "priority": "high",
  "status": "pending",
  "recipient": "+905551234567",
  "content": "Your OTP is 1234",
  "scheduled_at": null,
  "attempt_count": 0,
  "provider_msg_id": null,
  "created_at": "2026-03-14T12:00:00Z",
  "updated_at": "2026-03-14T12:00:00Z"
}
```

### Batch create

`POST /v1/notifications/batch`

Create up to 1000 notifications in one request. All receive the same generated `batch_id`.

**Request**

```bash
curl -s -X POST http://localhost:8080/v1/notifications/batch \
  -H "Content-Type: application/json" \
  -d '{
    "notifications": [
      {"recipient": "+905551234567", "channel": "sms", "content": "Hi", "priority": "high"},
      {"recipient": "user@example.com", "channel": "email", "content": "Hello", "priority": "normal"}
    ]
  }'
```

**Response** (201 Created)

```json
{
  "batch_id": "660e8400-e29b-41d4-a716-446655440001",
  "count": 2,
  "notifications": [
    {
      "id": "...",
      "batch_id": "660e8400-e29b-41d4-a716-446655440001",
      "channel": "sms",
      "priority": "high",
      "status": "pending",
      "recipient": "+905551234567",
      "content": "Hi",
      "scheduled_at": null,
      "attempt_count": 0,
      "provider_msg_id": null,
      "created_at": "...",
      "updated_at": "..."
    },
    {
      "id": "...",
      "batch_id": "660e8400-e29b-41d4-a716-446655440001",
      "channel": "email",
      "priority": "normal",
      "status": "pending",
      "recipient": "user@example.com",
      "content": "Hello",
      "scheduled_at": null,
      "attempt_count": 0,
      "provider_msg_id": null,
      "created_at": "...",
      "updated_at": "..."
    }
  ]
}
```

### Get notification

`GET /v1/notifications/{id}`

Fetch a single notification by ID.

**Request**

```bash
curl -s http://localhost:8080/v1/notifications/550e8400-e29b-41d4-a716-446655440000
```

**Response** (200 OK) — Same shape as in create; `status` and `attempt_count` reflect current state (e.g. `delivered`, `failed`).

### List notifications

`GET /v1/notifications`

Paginated list with optional filters. Use `cursor` from the response for the next page.

| Query parameter   | Description |
|-------------------|-------------|
| `status`          | Filter by status: `pending`, `processing`, `delivered`, `failed`, `cancelled`, `scheduled` |
| `channel`         | Filter by channel: `sms`, `email`, `push` |
| `batch_id`        | Filter by batch ID (UUID) |
| `created_after`   | Only notifications created after this time (RFC3339) |
| `created_before`  | Only notifications created before this time (RFC3339) |
| `limit`           | Page size (default 20, max 100) |
| `cursor`          | Opaque cursor from previous response for next page |

**Request**

```bash
curl -s "http://localhost:8080/v1/notifications?status=delivered&channel=sms&limit=20"
```

**Response** (200 OK)

```json
{
  "notifications": [
    {
      "id": "...",
      "channel": "sms",
      "priority": "high",
      "status": "delivered",
      "recipient": "+905551234567",
      "content": "...",
      "attempt_count": 1,
      "provider_msg_id": "...",
      "created_at": "...",
      "updated_at": "..."
    }
  ],
  "next_cursor": "2026-03-14T12:00:00.123456789Z|last-id"
}
```

Omit or use an empty `next_cursor` when there are no more pages.

### Cancel notification

`DELETE /v1/notifications/{id}`

Cancel a pending or scheduled notification. Returns 404 if not found, 409 if the notification is not in a cancellable state.

**Request**

```bash
curl -s -X DELETE http://localhost:8080/v1/notifications/550e8400-e29b-41d4-a716-446655440000
```

**Response** — 204 No Content on success.

### WebSocket (real-time status)

`GET /v1/notifications/stream`

Upgrades to WebSocket. The server sends JSON lines when notification status changes:

```json
{"notification_id":"550e8400-e29b-41d4-a716-446655440000","status":"delivered"}
```

Possible `status` values: `processing`, `delivered`, `failed`, `cancelled`.

### Health

`GET /health`

**Request**

```bash
curl -s http://localhost:8080/health
```

**Response** (200 OK or 503) — JSON with `status` (`ok` / `unhealthy`), `postgres`, `redis`, and optionally `circuit_breaker` per channel.

### Metrics

`GET /metrics`

**Request**

```bash
curl -s http://localhost:8080/metrics
```

**Response** (200 OK) — JSON with:

- `queue_depth` — `stream_length` (total messages) and `pending` (unacknowledged) per stream (high, normal, low, dead)
- `delivered` / `failed` — Counts per channel (sms, email, push)
- `avg_latency_ms` — Average delivery latency per channel
- `circuit_breaker` — State per channel (e.g. closed, open, half-open)

---

## Monitoring

- **Health** — `GET /health` for PostgreSQL, Redis, and circuit breaker state.
- **Metrics** — `GET /metrics` for queue depth, delivery counts, average latency, and circuit breaker.
- **Logs** — Structured JSON (zerolog) with timestamps and context.
- **Tracing** — OpenTelemetry spans to stdout for handlers, service, worker, and provider.
