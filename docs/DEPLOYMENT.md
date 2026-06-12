# Deployment

This guide describes a source build deployment of `eden-service`. It avoids
host-specific and container-specific assumptions. Use it as a baseline for local
servers, VMs, and managed infrastructure.

## 1. Choose A Runtime Shape

Pick the smallest feature set that matches the deployment.

Redis is the only production-ready gateway protocol in this release. The other
runtime shapes are useful for development and evaluation, but should not be used
as production deployments until their status is promoted.

| Deployment | Build command | Status |
|---|---|---|
| Redis gateway only | `cargo build -p eden-service --release --no-default-features --features redis --locked` | Production-ready |
| Embedded Redis build | `cargo build -p eden-service --release --no-default-features --features "redis embedded-db telemetry-export" --locked` | Production-ready Redis path with embedded storage |
| Full service | `cargo build -p eden-service --release --features server-runtime --locked` | Development/evaluation for non-Redis protocols |
| Postgres gateway only | `cargo build -p eden-service --release --no-default-features --features postgres --locked` | Development/evaluation |
| Mongo gateway only | `cargo build -p eden-service --release --no-default-features --features mongo --locked` | Development/evaluation |
| LLM/agent gateway only | `cargo build -p eden-service --release --no-default-features --features llm --locked` | Development/evaluation |

Use `server-runtime` when you want the broad evaluation surface: all endpoints,
LLM support, OpenAPI, metadata polling, embedded dashboard support, and
telemetry export.

Use focused features for gateway hot-path deployments and CI checks.

## 2. Install System Requirements

Install the build requirements from the root [README](../README.md):

- Rust from [`rust-toolchain.toml`](../rust-toolchain.toml)
- Cargo
- Protocol Buffers compiler
- OpenSSL, CMake, and `pkg-config`

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y protobuf-compiler cmake libssl-dev pkg-config
```

On macOS:

```bash
brew install protobuf cmake openssl pkg-config
```

## 3. Provision Storage

External database deployments use:

- PostgreSQL for authoritative control-plane state.
- ClickHouse for analytics, metrics, traces, logs, and metadata poll history.
- Redis-compatible endpoints when Redis gateway or Redis endpoint features are
  enabled.

Create the PostgreSQL database and user before starting the service. The service
initializes and updates its own schemas on startup, so the configured user needs
permission to create extensions, tables, indexes, and alter existing tables in
the selected database.

Create the ClickHouse database before startup, or configure the service to use
the default database. The configured user needs permission to create analytics
tables and insert/query telemetry rows.

The service uses an in-process cache for control-plane reads. Redis endpoint
traffic is configured through endpoint/interlay records after the service is
running.

Embedded database deployments use:

- Turso/SQLite-compatible local control-plane storage.
- DuckDB analytics storage.

Set `EDEN_TURSO_PATH` for the control-plane database file. Configure DuckDB
telemetry fields in `eden.toml` or environment variables if you do not want the
defaults from [`eden.example.toml`](../eden.example.toml).

## 4. Create Configuration

Copy the example configuration:

```bash
cp eden.example.toml eden.toml
```

Generate required secrets:

```bash
openssl rand -base64 32
uuidgen
openssl rand -hex 32
```

Set at least these fields or their environment variable equivalents:

```toml
[services.eden]
host = "0.0.0.0"
port = 8000
jwt_secret = "base64-secret-from-openssl"
node_uuid = "uuid-from-uuidgen"
new_org_token = "hex-token-from-openssl"

[databases.redis]
host = "127.0.0.1"
port = 6379
username = ""
password = ""
db_number = 0

[databases.postgres]
host = "127.0.0.1"
port = 5432
username = "eden"
password = "replace-me"
database = "eden"

[databases.clickhouse]
url = "http://127.0.0.1:8123"
username = "eden"
password = "replace-me"
database = "eden"
```

Environment variables override `eden.toml`. The most common equivalents are:

```bash
export EDEN_JWT_SECRET="base64-secret-from-openssl"
export EDEN_NODE_UUID="uuid-from-uuidgen"
export EDEN_NEW_ORG_TOKEN="hex-token-from-openssl"
export EDEN_HOST="0.0.0.0"
export EDEN_PORT="8000"
export REDIS_HOST="127.0.0.1"
export REDIS_PORT="6379"
export POSTGRES_HOST="127.0.0.1"
export POSTGRES_PORT="5432"
export POSTGRES_USER="eden"
export POSTGRES_PASSWORD="replace-me"
export POSTGRES_DB_NAME="eden"
export CLICKHOUSE_URL="http://127.0.0.1:8123"
export CLICKHOUSE_USER="eden"
export CLICKHOUSE_PASSWORD="replace-me"
export CLICKHOUSE_DATABASE="eden"
```

For embedded database deployments:

```bash
export EDEN_TURSO_PATH="/var/lib/eden/control-plane.db"
export EDEN_DB_ENCRYPTION_KEY="optional-encryption-key"
```

## 5. Build

Full service:

```bash
cargo build -p eden-service --release --features server-runtime --locked
```

Focused Redis service:

```bash
cargo build -p eden-service --release --no-default-features --features redis --locked
```

Embedded Redis service:

```bash
cargo build -p eden-service --release --no-default-features --features "redis embedded-db telemetry-export" --locked
```

The binary is written to:

```bash
target/release/eden-service
```

## 6. Start The Service

Run directly:

```bash
./target/release/eden-service --config eden.toml
```

Or install the binary and config under a process supervisor:

```bash
sudo install -m 0755 target/release/eden-service /usr/local/bin/eden-service
sudo install -d -m 0750 /etc/eden
sudo install -m 0640 eden.toml /etc/eden/eden.toml
```

Minimal `systemd` unit:

```ini
[Unit]
Description=Eden service
After=network-online.target
Wants=network-online.target

[Service]
User=eden
Group=eden
Environment=RUST_LOG=info
ExecStart=/usr/local/bin/eden-service --config /etc/eden/eden.toml
Restart=on-failure
RestartSec=5
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now eden-service
sudo systemctl status eden-service
```

## 7. Verify Startup

Check the public help route:

```bash
curl -fsS http://127.0.0.1:8000/api/v1/help
```

If built with `openapi` or `server-runtime`, check generated API docs:

```bash
curl -fsS http://127.0.0.1:8000/api-docs/openapi.json | head
```

Swagger UI is available at:

```text
http://127.0.0.1:8000/swagger-ui/
```

If `embedded-dashboard` is enabled and dashboard assets were present at compile
time, the same service can also serve the dashboard SPA from `/`.

## 8. Create The First Organization

Create an organization with an initial super-admin user. The bearer token is the
`new_org_token` configured above.

```bash
curl -fsS \
  -X POST http://127.0.0.1:8000/api/v1/new \
  -H "Authorization: Bearer ${EDEN_NEW_ORG_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "example-org",
    "description": "Example organization",
    "super_admins": [
      {
        "username": "admin",
        "password": "change-me",
        "description": "Initial administrator",
        "email": "admin@example.com",
        "display_name": "Admin",
        "perms": null
      }
    ]
  }'
```

Log in with Basic auth and `X-Org-Id`:

```bash
curl -fsS \
  -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H "X-Org-Id: example-org" \
  -u "admin:change-me"
```

Use the returned JWT as:

```text
Authorization: Bearer <token>
```

## 9. Enable Telemetry

With `telemetry-export`, the service can write metrics, traces, and logs to the
configured analytics store.

Recommended baseline:

```toml
[telemetry]
clickhouse_enabled = true
otlp_export_enabled = false
dogstatsd_enabled = false
log_level = "info"
```

To export spans to an OTLP collector:

```toml
[telemetry]
otlp_export_enabled = true
otlp_traces_endpoint = "http://127.0.0.1:4318"
```

To export metrics to DogStatsD:

```toml
[telemetry]
dogstatsd_enabled = true
dogstatsd_endpoint = "127.0.0.1:8125"
```

Dashboard and analytics clients should query Eden APIs, not storage backends
directly:

```text
GET /api/v1/analytics/series
GET /api/v1/analytics/telemetry/metrics
GET /api/v1/analytics/telemetry/traces
GET /api/v1/analytics/telemetry/logs
```

## 10. Operate Safely

- Keep `eden.toml` and environment files out of source control.
- Rotate `EDEN_JWT_SECRET` and organization bootstrap tokens if exposed.
- Bind `services.eden.host` to `127.0.0.1` behind a reverse proxy, or to
  `0.0.0.0` only when network access controls are already in place.
- Use TLS at the edge for Basic auth and bearer tokens.
- Back up PostgreSQL and ClickHouse for external database deployments.
- Back up the Turso and DuckDB files for embedded deployments.
- Run focused builds in CI for gateway changes and a full `server-runtime`
  check before release.

## Related Docs

- [Feature flags](FEATURES.md)
- [OpenAPI](OPENAPI.md)
- [API documentation](api_docs.md)
- [Configuration loader](../eden_config/README.md)
- [Metrics](METRICS.md)
- [Security model](SECURITY_MODEL.md)
