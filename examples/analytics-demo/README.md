# Analytics Server and Traffic Client Demo

`analytics-demo` now behaves like a customer-style e-commerce backend with analytics attached to it.

The stack is intentionally simple:

- `analytics-server` exposes HTTP APIs for catalog, carts, checkout, dashboards, and event ingest
- `traffic-client` generates realistic read and write traffic against that API
- PostgreSQL is the system of record for commerce and analytics reads when enabled
- Redis acts as the hot read cache for dashboard, storefront, catalog, and cart views
- Datadog is the only recommended observability target

## What It Simulates

The demo is meant to look like a SaaS commerce application with analytics built in:

- multi-tenant organizations and users
- product catalog and inventory
- shopping carts and cart item mutations
- checkout, orders, and payments
- web/app events such as page views, clicks, conversions, sign-ups, and purchases
- analytics dashboards, hourly trends, top pages, and storefront KPIs

That gives you a realistic story for:

- cache reads vs source-of-truth reads
- Redis + PostgreSQL together
- external traffic generators instead of self-driven server load
- end-to-end Datadog telemetry across clients and server

## Quick Start

```bash
# Default local run: analytics-server + traffic-client + Redis
make run

# Full e-commerce path: analytics-server + traffic-client + Redis + PostgreSQL
make run-all

# Datadog-enabled local run
DD_API_KEY=your_api_key DD_SITE=datadoghq.com make run-datadog
```

Useful checks:

```bash
curl -s http://localhost:3000/health | jq
docker compose ps traffic-client
```

Traffic-client replicas expose `/health` and `/config` on container port `3100`. Compose publishes those ports dynamically, so use `docker compose ps traffic-client` or `docker compose port --index <n> traffic-client 3100`.

## Backend Modes

```bash
# Redis-only
make run-redis

# PostgreSQL-only
make run-postgres

# Redis + PostgreSQL together
make run-all

# Server only, no external traffic clients
make run-server-only

# Legacy in-process workload mode
make run-internal
```

Behavior by mode:

- `Redis + PostgreSQL`: read-through caching. Reads check Redis first, then load from PostgreSQL and repopulate cache.
- `PostgreSQL-only`: all reads hit PostgreSQL directly.
- `Redis-only`: analytics and commerce read views can fall back to synthetic data, but durable writes like cart mutations, checkout, and event persistence are unavailable.

Publish recommendation:

- use `Redis + PostgreSQL` for the most believable e-commerce demo
- use `Redis-only` only for lightweight local or cache-centric demos

## Public API

Core tenant and analytics endpoints:

- `GET /api/v1/organizations`
- `GET /api/v1/organizations/:org_id/dashboard`
- `GET /api/v1/organizations/:org_id/analytics/overview?hours=24`
- `GET /api/v1/organizations/:org_id/analytics/top-pages?limit=10`
- `GET /api/v1/organizations/:org_id/analytics/hourly?points=6`
- `POST /api/v1/organizations/:org_id/events`

Core commerce endpoints:

- `GET /api/v1/organizations/:org_id/storefront`
- `GET /api/v1/organizations/:org_id/catalog?limit=12`
- `POST /api/v1/organizations/:org_id/carts`
- `GET /api/v1/organizations/:org_id/carts/:cart_id`
- `POST /api/v1/organizations/:org_id/carts/:cart_id/items`
- `POST /api/v1/organizations/:org_id/carts/:cart_id/checkout`

Example flow:

```bash
# list orgs
ORG_ID=$(curl -s http://localhost:3000/api/v1/organizations | jq -r '.[0].id')

# browse storefront
curl -s http://localhost:3000/api/v1/organizations/$ORG_ID/storefront | jq

# browse catalog
curl -s http://localhost:3000/api/v1/organizations/$ORG_ID/catalog?limit=5 | jq

# create a cart
CART_ID=$(curl -s -X POST http://localhost:3000/api/v1/organizations/$ORG_ID/carts \
  -H 'content-type: application/json' \
  -d '{"quantity":2}' | jq -r '.cart_id')

# add another item
curl -s -X POST http://localhost:3000/api/v1/organizations/$ORG_ID/carts/$CART_ID/items \
  -H 'content-type: application/json' \
  -d '{"quantity":1}' | jq

# inspect cart
curl -s http://localhost:3000/api/v1/organizations/$ORG_ID/carts/$CART_ID | jq

# checkout
curl -s -X POST http://localhost:3000/api/v1/organizations/$ORG_ID/carts/$CART_ID/checkout \
  -H 'content-type: application/json' \
  -d '{"payment_method":"credit_card"}' | jq
```

Write behavior:

- cart creation and cart item mutations write to PostgreSQL
- checkout creates an order, payment, and purchase event in PostgreSQL
- write paths update or invalidate Redis-backed cart/storefront/catalog views
- raw event ingest remains available for analytics/event simulation

## Traffic Client

`traffic-client` is the default load generator. It behaves like an external worker pool that calls the server over HTTP.

Built-in profiles:

- `balanced`
- `dashboard-heavy`
- `read-heavy`
- `write-heavy`

Each client exposes:

- `GET /health`
- `GET /config`
- `PATCH /config`
- `GET /control`
- `PATCH /control`

`/config` is the preferred runtime API. `/control` is a compatibility alias.

`analytics-server` does not expose `/control` in normal mode. That route only exists when `INTERNAL_WORKLOAD_ENABLED=true` for the legacy in-process workload path.

### Live Traffic Control

The client config plane now has three independent distribution knobs:

- `query_distribution`: which `GET` APIs it calls
- `write_distribution`: which `POST` APIs it calls
- `event_distribution`: which event types are used when the selected write is `event_ingest`

Example:

```bash
curl -s -X PATCH http://localhost:<client-port>/config \
  -H 'content-type: application/json' \
  -d '{
    "queries_per_second": 120,
    "events_per_second": 60,
    "query_distribution": {
      "storefront": 30,
      "catalog": 25,
      "dashboard": 10,
      "cart_detail": 15
    },
    "write_distribution": {
      "cart_create": 30,
      "cart_add_item": 30,
      "cart_checkout": 20,
      "event_ingest": 20
    },
    "event_distribution": {
      "page_view": 45,
      "click": 30,
      "conversion": 12,
      "sign_up": 8,
      "purchase": 5
    }
  }' | jq
```

Notes:

- `PATCH /config` is partial
- setting `queries_per_second` or `events_per_second` to `0` pauses that traffic class
- at least one `query_distribution`, `write_distribution`, and `event_distribution` weight must stay non-zero
- each replica is independent, so one client pool can simulate multiple traffic personalities

## Scaling Clients

The default client is a stateless worker pool.

```bash
# one replica
CLIENT_REPLICAS=1 make run

# six replicas
CLIENT_REPLICAS=6 make run

# rescale an already running pool
CLIENT_REPLICAS=3 make scale-clients

# inspect replica ports
make client-ports
```

Each replica emits a unique `client_instance_id`, so Datadog can distinguish them even when they share the same logical `CLIENT_NAME`.

## Testing and Verification

Recommended pre-publish gate:

```bash
cargo fmt --manifest-path examples/analytics-demo/Cargo.toml
cargo clippy --manifest-path examples/analytics-demo/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path examples/analytics-demo/Cargo.toml
```

The runtime coverage test boots real `analytics-server` and `traffic-client` binaries and verifies:

- `/health`
- `/config`
- analytics API reads
- storefront and catalog reads
- cart and event write-path behavior in no-backend mode
- the server does not expose `/control` in normal mode

Current note:

- the test suite verifies the expanded HTTP/runtime surface
- durable cart and checkout success still require a real PostgreSQL-backed run such as `make run-all`

### Live Runtime Validation

For a running stack, use the standalone validator:

```bash
# validate a local dual-backend run
REQUIRE_POSTGRES=true REQUIRE_REDIS=true make validate-runtime

# validate server + one traffic-client admin endpoint
REQUIRE_POSTGRES=true REQUIRE_REDIS=true \
CLIENT_BASE_URL=http://127.0.0.1:3100 \
make validate-runtime
```

The validator checks:

- server `/health`
- organization listing
- storefront, catalog, dashboard, and overview reads
- event ingest
- cart create, cart read, cart add-item, and checkout when PostgreSQL is enabled
- client `/health` and `/config` when `CLIENT_BASE_URL` is provided

It exits non-zero on failure, so it can be used as a deployment go/no-go step.

## Observability

The recommended observability path is Datadog only.

The demo exports:

- structured activity logs to stdout
- DogStatsD metrics through `fast-telemetry`
- OTLP spans through `fast-telemetry`

Key environment variables:

- `TELEMETRY_ENABLED=true`
- `TELEMETRY_DOGSTATSD_ENDPOINT=<host:port>`
- `TELEMETRY_OPENTELEMETRY_ENDPOINT=<url>`
- `TELEMETRY_DATADOG_API_KEY=<token>` for OTLP auth/header usage

There is no public `/metrics` endpoint anymore. The publish path is Datadog-first, not Prometheus/Grafana-first.

### Logs

Structured activity logs include fields such as:

- `event_name`
- `status`
- `tags`
- `ddtags`
- `latency_us`
- `error_type`
- `payload`

Representative event names:

- `analytics.organization_list.load`
- `analytics.dashboard.load`
- `analytics.storefront.load`
- `analytics.catalog.load`
- `analytics.cart.load`
- `analytics.event.ingest`
- `analytics.cart.create`
- `analytics.cart.add_item`
- `analytics.cart.checkout`

### Metrics

High-value metric families:

- `http_requests_total`
- `http_request_duration_seconds`
- `queries_by_type_total`
- `query_duration_by_type_seconds`
- `operation_success_total`
- `operation_errors_total`
- `cache_hits_total`
- `cache_misses_total`
- `activity_events_total`
- `activity_errors_total`
- `telemetry_exports_total`

### Spans

Current span coverage is strongest at:

- inbound server HTTP requests
- outbound `traffic-client -> analytics-server` requests
- major PostgreSQL-backed read and write operations
- Redis cache get/set/invalidation around hot paths

## Important Config

Useful server-side vars:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ENABLED` | `true` | Enable Redis |
| `POSTGRES_ENABLED` | `false` | Enable PostgreSQL |
| `INTERNAL_WORKLOAD_ENABLED` | `false` | Re-enable the legacy in-process server workload |
| `REDIS_URL` | `redis://localhost:6370` | Redis connection string |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB_NAME` | `analytics` | PostgreSQL database |
| `DB_POOL_SIZE` | `50` | PostgreSQL pool size |
| `REDIS_POOL_SIZE` | `100` | Redis pool size |
| `ORGANIZATIONS` | `500` | Tenant count |
| `USERS_PER_ORG` | `1000` | Users per tenant |

Useful client-side vars:

| Variable | Default | Description |
|----------|---------|-------------|
| `CLIENT_PROFILE` | `balanced` | Built-in traffic profile |
| `CLIENT_REPLICAS` | `1` | Compose worker count |
| `QUERIES_PER_SECOND` | `150` | Per-client read rate |
| `EVENTS_PER_SECOND` | `25` | Per-client write rate |
| `QUERY_WORKERS` | `8` | Per-client read worker count |
| `EVENT_WORKERS` | `4` | Per-client write worker count |
| `TARGET_BASE_URL` | `http://localhost:3000` | Server base URL |

For the full flag/env surface, use:

```bash
cargo run --manifest-path examples/analytics-demo/Cargo.toml --bin analytics-server -- --help
cargo run --manifest-path examples/analytics-demo/Cargo.toml --bin traffic-client -- --help
cargo run --manifest-path examples/analytics-demo/Cargo.toml --bin runtime-validator -- --help
```

## Azure Deployment

Use [AZURE_DEPLOYMENT.md](./AZURE_DEPLOYMENT.md) for the Azure Container Apps deployment path.

That guide assumes:

- `analytics-server`
- one or more `traffic-client` pools
- Azure Database for PostgreSQL
- Redis on Azure
- Datadog

## License

Apache-2.0
