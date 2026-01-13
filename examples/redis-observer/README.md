# Redis Observer

A terminal dashboard for monitoring Redis databases with Eden migration support.

## Features

- Real-time monitoring of multiple Redis instances
- Key count tracking with delta indicators
- Operations per second (ops/sec) visualization
- Coverage analysis between source and destination databases
- Eden API integration for migration management
- Interactive charts using Braille characters

## Prerequisites

- Rust (2024 edition)
- Access to Redis instances
- Eden API server (optional, for migration features)

## Installation

```bash
cd examples/redis-observer
cargo build --release
```

## Usage

```bash
cargo run -- <source> <dest> [api_endpoint] [eden_source] [eden_dest]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `source` | Source Redis as `host:port` or just `port` (default host: 172.24.2.218) |
| `dest` | Destination Redis as `host:port` or just `port` (default host: 172.24.2.218) |
| `api_endpoint` | Eden API endpoint (default: http://localhost:8000) |
| `eden_source` | Eden's source Redis as `host:port` (when different from TUI connection) |
| `eden_dest` | Eden's dest Redis as `host:port` (when different from TUI connection) |

### Examples

```bash
# Both Redis instances use default host
cargo run -- 6379 6380

# Different hosts for each Redis instance
cargo run -- 192.168.1.10:6379 192.168.1.20:6380

# TUI connects locally, Eden uses different IPs
cargo run -- localhost:6379 localhost:6380 http://localhost:8000 172.24.2.211:6379 172.24.2.218:6379
```

## Keyboard Controls

| Key | Action |
|-----|--------|
| `q` / `Ctrl+C` / `Esc` | Quit |
| `c` | Force coverage check now |
| `v` | Toggle ops/sec chart |
| `d` | Toggle debug log panel |
| `s` | Start migration setup (connect to Eden API) |
| `m` | Trigger migration |
| `r` | Refresh migration status |

## Dashboard Layout

The dashboard consists of:

- **Left Panel**: Migration setup status showing API call progress
- **Stats Table**: Key counts, deltas, unique keys, ops/sec, connections, and coverage percentage
- **Charts**: Real-time graphs for key counts and operations per second
- **Status Bar**: Available keyboard shortcuts and coverage countdown

## Migration Workflow

1. Press `s` to initiate setup - this creates the organization, endpoints, interlay, and migration in Eden
2. Wait for all API calls to complete (shown in the left panel)
3. Press `m` to trigger the migration when status shows "Ready"
4. Press `r` to refresh migration status at any time

## Coverage Analysis

Coverage analysis runs automatically every 15 seconds (or press `c` to force). It shows:

- **Unique keys**: Keys that exist only in this instance
- **Coverage**: Percentage of total unique keys present in each instance

A 100% coverage on the destination indicates all source keys have been migrated.
