# Redis Migrator

A CLI tool to set up Eden Redis migrations, bulk-load data, run read/write workloads, and observe progress — all driven by a single `.env` file.

## Quick Start

### 1. Build

```bash
cd examples/redis-migrator
cargo build --release
```

### 2. Configure

Copy the example env file and fill in your connection details:

```bash
cp .env.example .env
```

```env
# Eden API
EDEN_API_URL=http://134.33.222.197:8000
EDEN_ORG_ID=adam-demo
EDEN_NEW_ORG_SECRET=neworgsecret
EDEN_ADMIN_USER=admin
EDEN_ADMIN_PASSWORD=adam-demo-pass

# Source Redis (origin) - e.g. Azure Redis Cache
REDIS_SOURCE_URL=rediss://:<your-redis-password>@your-redis-cache.redis.cache.windows.net:6380

# Destination Redis (target) - e.g. Azure Managed Redis
REDIS_DEST_URL=rediss://:<your-redis-password>@your-managed-redis.eastus.redis.azure.net:10000

# Migration
MIGRATION_MODE=big-bang
INTERLAY_PORT=5731
```

Use `redis://` for plain connections, `rediss://` for TLS. Passwords go in the URL as `rediss://:<your-redis-password>@host:port`.

### 3. Run the Full Workflow

```bash
# Step 1: Set up Eden (org, endpoints, interlay, migration)
cargo run --release -- setup

# Step 2: Populate source Redis with 1TB of mixed data types,
#          then auto-start a 99% read / 1% write client indefinitely
cargo run --release -- populate --mb 1048576 --mixed --then-client 1 -d 0
```

That's it. Steps can also be run individually — see below.

---

## Commands

### `setup` — Initialize Eden

Creates the organization, logs in, registers source/destination endpoints, creates an interlay, creates a migration, and wires them together. Handles 409 conflicts gracefully (skips resources that already exist).

```bash
cargo run --release -- setup
```

All config comes from `.env`. You can override any value via CLI flags:

```bash
cargo run --release -- setup \
  --api-url http://134.33.222.197:8000 \
  --source-url rediss://:<your-redis-password>@source-host:6380 \
  --dest-url rediss://:<your-redis-password>@dest-host:10000 \
  --mode canary \
  --canary-read-pct 0.05
```

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--api-url` | `EDEN_API_URL` | *(required)* | Eden API base URL |
| `--org-id` | `EDEN_ORG_ID` | `adam-demo` | Organization ID |
| `--org-token` | `EDEN_NEW_ORG_SECRET` | `neworgsecret` | Secret for org creation |
| `--admin-user` | `EDEN_ADMIN_USER` | `admin` | Admin username |
| `--admin-pass` | `EDEN_ADMIN_PASSWORD` | `adam-demo-pass` | Admin password |
| `--source-url` | `REDIS_SOURCE_URL` | *(required)* | Source Redis URL |
| `--dest-url` | `REDIS_DEST_URL` | *(required)* | Destination Redis URL |
| `--interlay-port` | `INTERLAY_PORT` | `5731` | Interlay listening port |
| `--mode` | `MIGRATION_MODE` | `big-bang` | `big-bang`, `canary`, or `blue-green` |
| `--canary-read-pct` | `CANARY_READ_PCT` | `0.05` | Read ratio for canary mode (0.0–1.0) |

`EDEN_NEW_ORG_TOKEN` and `EDEN_ADMIN_PASS` are still accepted as fallbacks for existing local `.env` files.

### `populate` — Bulk Load Data

Generates RESP protocol and streams it to `redis-cli --pipe` for maximum throughput. Supports all Redis data types including a `--mixed` mode that randomly assigns types per key.

```bash
# 100MB of STRING data (defaults)
cargo run --release -- populate --mb 100

# 1TB of mixed types with 4KB keys
cargo run --release -- populate --mb 1048576 --mixed --size 4096

# Populate then immediately run a 99/1 read/write client until Ctrl+C
cargo run --release -- populate --mb 1048576 --mixed --then-client 1 -d 0
```

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--url`, `-u` | `REDIS_SOURCE_URL` | `redis://localhost:6379` | Redis URL |
| `--mb`, `-m` | `MEGABYTES` | `1000` | Total data in MB |
| `--size`, `-s` | `KEY_SIZE` | `1024` | Value size per key in bytes |
| `--string` | | *(default)* | STRING type |
| `--json` | | | JSON type (requires RedisJSON) |
| `--hash` | | | HASH type |
| `--list` | | | LIST type |
| `--set` | | | SET type |
| `--zset` | | | SORTED SET type |
| `--mixed` | | | Random mix of STRING, HASH, LIST, SET, ZSET |
| `--prefix`, `-p` | `KEY_PREFIX` | `pop` | Key prefix (keys are `prefix:0`, `prefix:1`, ...) |
| `--batch-size`, `-b` | `BATCH_SIZE` | `10000` | Keys to buffer before flushing to pipe |
| `--ttl`, `-t` | `TTL` | `0` | TTL in seconds (0 = no expiry) |
| `--elements-per-key`, `-e` | `ELEMENTS_PER_KEY` | `10` | Elements per key (hash/list/set/zset) |
| `--clear` | | `false` | Delete existing keys with this prefix first |
| `--then-client`, `-w` | | | Auto-start client after populating (write % 0–100) |
| `-d` | | `60` | Client duration in seconds (0 = until Ctrl+C) |
| `--client-concurrency` | | `50` | Client worker count |

### `client` — Run Read/Write Workload

Runs random read/write operations against existing keys at a configurable ratio. Type-aware — detects each key's type (STRING, HASH, LIST, SET, ZSET) and uses the appropriate commands.

```bash
# 99% reads / 1% writes for 60 seconds
cargo run --release -- client -w 1 -n 1024000

# Run indefinitely until Ctrl+C
cargo run --release -- client -w 1 -n 1024000 -d 0

# Heavy write workload
cargo run --release -- client -w 50 -n 1024000 -d 300
```

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--url`, `-u` | `REDIS_SOURCE_URL` | `redis://localhost:6379` | Redis URL |
| `--prefix`, `-p` | `KEY_PREFIX` | `pop` | Key prefix to operate on |
| `--num-keys`, `-n` | `NUM_KEYS` | `1000` | Number of keys in the key space |
| `--write-pct`, `-w` | `WRITE_PCT` | `20` | Write percentage (0–100) |
| `--value-size`, `-s` | `VALUE_SIZE` | `1024` | Value size for writes in bytes |
| `--concurrency`, `-c` | `CONCURRENCY` | `50` | Concurrent workers |
| `--duration`, `-d` | `DURATION` | `60` | Duration in seconds (0 = until Ctrl+C) |
| `--report-interval` | `REPORT_INTERVAL` | `5` | Seconds between stats reports |

### `observe` — Run the Integrated Observer TUI

Runs the Redis observer TUI directly from the `redis-migrator` tool.

Example:

```bash
cargo run --release -- observe \
  redis://localhost:6378 \
  redis://localhost:6377 \
  http://localhost:8000 \
  redis://host.docker.internal:6378 \
  redis://host.docker.internal:6377
```

The same config can also be passed entirely as flags:

```bash
cargo run --release -- observe \
  --source-url redis://localhost:6378 \
  --dest-url redis://localhost:6377 \
  --api-url http://localhost:8000 \
  --eden-source-url redis://host.docker.internal:6378 \
  --eden-dest-url redis://host.docker.internal:6377 \
  --org-id adam-demo-local-redis \
  --org-token neworgsecret \
  --admin-user admin \
  --admin-pass adam-demo-pass \
  --mode canary \
  --canary-read-pct 0.05 \
  --interlay-port 5731 \
  --redis-url redis://localhost:5731 \
  --enable-populator
```

For your local setup, the first two URLs are what the TUI connects to from your Mac, while the optional Eden source/dest URLs are what Eden uses from inside Docker.

Inside the TUI:

- Press `o` to open the populator launcher view
- Use `Up` / `Down` to choose a field
- Type to edit values, use `Space` for toggles, and press `Enter` to launch `populate` in a new Terminal window
- Watch bulk-load progress directly in the launcher panel
- Press `w` to switch to the client workload panel when a client run is active

Set `REDIS_MIGRATOR_ENABLE_POPULATOR=false` in `.env` to hide the integrated populator launcher from the TUI.

### `observe-client` — Launch Observer + Client Together

Opens the integrated observer TUI in a new macOS Terminal window using the same Eden org and Redis endpoints, then starts the `client` workload in the current terminal.

This is especially useful for local Docker-based Eden runs where:

- the client should hit the interlay on `localhost`
- the observer TUI should connect to local Redis on `localhost`
- Eden itself should still use `host.docker.internal`

Example:

```bash
cargo run --release -- observe-client \
  --url redis://localhost:5731 \
  --num-keys 25600 \
  --write-pct 1 \
  --duration 0 \
  --source-url redis://host.docker.internal:6378 \
  --dest-url redis://host.docker.internal:6377
```

For your current local test setup:

```bash
cargo run --release -- observe-client \
  --url redis://localhost:5731 \
  --num-keys 25600 \
  --write-pct 1 \
  --duration 0 \
  --source-url redis://host.docker.internal:6378 \
  --dest-url redis://host.docker.internal:6377 \
  --org-id adam-demo-local-redis
```

If your `.env` already contains `REDIS_URL`, `REDIS_SOURCE_URL`, `REDIS_DEST_URL`, `EDEN_API_URL`, and `EDEN_ORG_ID`, you can usually shorten that to:

```bash
cargo run --release -- observe-client --num-keys 25600 --write-pct 1 --duration 0
```

Additional flags:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--source-url` | `REDIS_SOURCE_URL` | *(required)* | Redis URL Eden should use for the source |
| `--dest-url` | `REDIS_DEST_URL` | *(required)* | Redis URL Eden should use for the destination |
| `--api-url` | `EDEN_API_URL` | `http://localhost:8000` | Eden API base URL |
| `--org-id` | `EDEN_ORG_ID` | `adam-demo` | Eden organization ID |
| `--observer-source` | `OBSERVER_SOURCE` | derived | TUI source host:port override |
| `--observer-dest` | `OBSERVER_DEST` | derived | TUI dest host:port override |
| `--observer-start-delay-ms` | `OBSERVER_START_DELAY_MS` | `1500` | Wait time before starting the client |
| `--observer-ready-timeout-ms` | `OBSERVER_READY_TIMEOUT_MS` | `60000` | Max time to wait for the observer to connect before aborting |

Cloud note:

- `observe-client` now passes full Redis URLs through to the integrated observer, so `rediss://` URLs with passwords work for monitored cloud Redis instances too.
- Inside the observer TUI, press `w` to toggle the workload view and watch the client progress live.

---

## TLS & Authentication

Both Azure Redis Cache and Azure Managed Redis use TLS with password auth. Use the `rediss://` scheme:

```env
# Azure Redis Cache (TLS on port 6380)
REDIS_SOURCE_URL=rediss://:<your-redis-password>@eden-demo-azure-redis-cache.redis.cache.windows.net:6380

# Azure Managed Redis (TLS on port 10000)
REDIS_DEST_URL=rediss://:<your-redis-password>@eden-demo-azure-managed-redis.eastus.redis.azure.net:10000
```

The tool automatically passes `--tls` and `-a <password>` to `redis-cli --pipe` when it detects `rediss://` and a password in the URL.

To verify connectivity manually:

```bash
# Azure Redis Cache
redis-cli \
  -h eden-demo-azure-redis-cache.redis.cache.windows.net \
  -p 6380 \
  --tls \
  -a '<your-redis-password>' \
  ping

# Azure Managed Redis
redis-cli \
  -h eden-demo-azure-managed-redis.eastus.redis.azure.net \
  -p 10000 \
  --tls \
  --cacert /etc/ssl/cert.pem \
  -a '<your-redis-password>' \
  ping
```

---

## End-to-End Example

```bash
# 1. Configure
cp .env.example .env
# Edit .env with your Redis URLs and Eden API endpoint

# 2. Set up Eden migration infrastructure
cargo run --release -- setup

# 3. Populate 1TB of mixed-type data into the source Redis,
#    then run a 99/1 read/write workload indefinitely
cargo run --release -- populate \
  --mb 1048576 \
  --mixed \
  --then-client 1 \
  -d 0
```

## Requirements

- Rust 1.70+ (edition 2021)
- `redis-cli` in PATH (with TLS support for `rediss://` URLs)
- Redis 6.0+ (or Redis Stack for JSON support)
- Network access to Redis instances and Eden API
