# Postgres Observer

A terminal dashboard for monitoring PostgreSQL databases with Eden migration support.

## Features

- Real-time monitoring of multiple PostgreSQL instances
- Row count tracking with delta indicators
- Transactions per second (TPS) visualization
- Coverage analysis between source and destination databases
- Eden API integration for migration management
- Interactive charts using Braille characters

## Prerequisites

- Rust (2024 edition)
- Access to PostgreSQL instances
- Eden API server (optional, for migration features)

## Installation

```bash
cd examples/postgres-observer
cargo build --release
```

## Usage

```bash
cargo run -- <source_url> <dest_url> [api_endpoint] [eden_source_url] [eden_dest_url]
```

Migration setup uses Eden admin auth from the environment:

```bash
export EDEN_NEW_ORG_SECRET=neworgsecret
export EDEN_ADMIN_USER=admin
export EDEN_ADMIN_PASSWORD=adam-demo-pass
```

`EDEN_NEW_ORG_TOKEN` and `EDEN_ADMIN_PASS` are still accepted as fallbacks for existing local setups.

### Arguments

| Argument | Description |
|----------|-------------|
| `source_url` | Source PostgreSQL URL (e.g., `postgresql://user:pass@host:5432/db`) |
| `dest_url` | Destination PostgreSQL URL |
| `api_endpoint` | Eden API endpoint (default: http://localhost:8000) |
| `eden_source_url` | Eden's source PostgreSQL URL (when different from TUI connection) |
| `eden_dest_url` | Eden's dest PostgreSQL URL (when different from TUI connection) |

### Examples

```bash
# Basic usage with two local PostgreSQL instances
cargo run -- postgresql://postgres:postgres@localhost:5432/source postgresql://postgres:postgres@localhost:5433/dest

# With Eden API endpoint
cargo run -- postgresql://postgres:postgres@localhost:5432/source postgresql://postgres:postgres@localhost:5433/dest http://localhost:8000

# TUI connects locally, Eden uses different IPs
cargo run -- postgresql://postgres:postgres@localhost:5432/src postgresql://postgres:postgres@localhost:5433/dst http://localhost:8000 postgresql://postgres:postgres@172.24.2.218:5432/src postgresql://postgres:postgres@172.24.2.218:5433/dst
```

## Keyboard Controls

| Key | Action |
|-----|--------|
| `q` / `Ctrl+C` / `Esc` | Quit |
| `f` | Force coverage check now |
| `v` | Toggle TPS chart |
| `d` | Toggle debug log panel |
| `Tab` | Toggle migration mode (BigBang / Canary / BlueGreen) |
| `s` | Start migration setup (connect to Eden API) |
| `m` | Trigger migration |
| `r` | Refresh migration status |
| `c` | Complete running migration |
| `b` | Rollback completed/failed migration |
| `p` | Pause/resume migration |
| `+` / `=` | Increase canary traffic by 5% (canary mode only) |
| `-` | Decrease canary traffic by 5% (canary mode only) |
| `t` | Toggle environment (blue-green mode only) |

## Dashboard Layout

The dashboard consists of:

- **Left Panel**: Migration setup status showing API call progress
- **Stats Table**: Row counts, deltas, unique tables, TPS, connections, and coverage percentage
- **Charts**: Real-time graphs for row counts and transactions per second
- **Status Bar**: Available keyboard shortcuts and coverage countdown

## Migration Workflow

1. Press `Tab` to select migration mode (BigBang, Canary, or BlueGreen)
2. Press `s` to initiate setup - this creates the organization, endpoints, interlay, and migration in Eden
3. Wait for all API calls to complete (shown in the left panel)
4. Press `m` to trigger the migration when status shows "Ready"
5. For **Canary mode**: Use `+`/`-` to adjust traffic percentage
6. For **BlueGreen mode**: Press `t` to toggle between blue and green environments
7. Press `r` to refresh migration status at any time

## Coverage Analysis

Coverage analysis runs automatically every 15 seconds (or press `f` to force). It shows:

- **Unique tables**: Tables that exist only in this instance
- **Coverage**: Percentage of total unique tables present in each instance

A 100% coverage on the destination indicates all source tables are present in the destination.

## Monitoring Metrics

| Metric | Source | Description |
|--------|--------|-------------|
| Rows | `pg_stat_user_tables.n_live_tup` | Total live row count across all user tables |
| TPS | `pg_stat_database` | Transactions per second (commit + rollback delta) |
| Connections | `pg_stat_activity` | Active connections to the database |
| Coverage | Table comparison | Percentage of tables present in each instance |
