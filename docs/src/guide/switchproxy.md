# SwitchProxy

SwitchProxy is a TCP proxy that enables live traffic switching between two backend servers without dropping connections. It's designed for zero-downtime database migrations where you need to redirect client traffic from one server to another.

## Overview

SwitchProxy sits between your clients and backend servers, forwarding TCP traffic to a selected upstream server. When you switch backends, existing connections are seamlessly reconnected to the new server.

```
                    ┌─────────────────┐
                    │   Server 1      │
┌─────────┐         │  (e.g., old DB) │
│ Clients │ ──────► │─────────────────│
└─────────┘         │   SwitchProxy   │
                    │─────────────────│
                    │   Server 2      │
                    │  (e.g., new DB) │
                    └─────────────────┘
```

## Usage

### Starting SwitchProxy

```bash
switchproxy <api_addr> <listen_addr> <server1_addr> <server2_addr>
```

**Arguments:**

| Argument       | Description                         | Example          |
| -------------- | ----------------------------------- | ---------------- |
| `api_addr`     | Control API listen address          | `127.0.0.1:8009` |
| `listen_addr`  | Client connection listen address    | `127.0.0.1:6379` |
| `server1_addr` | Default backend server (server 1)   | `10.0.0.1:6379`  |
| `server2_addr` | Alternate backend server (server 2) | `10.0.0.2:6379`  |

**Example:**

```bash
# Start switchproxy for Redis migration
# - Control API on port 8009
# - Clients connect to port 6379
# - Server 1: old Redis on port 6378
# - Server 2: new Redis on port 6377

switchproxy 127.0.0.1:8009 127.0.0.1:6379 127.0.0.1:6378 127.0.0.1:6377
```

### Control API

SwitchProxy exposes a simple HTTP API for controlling traffic routing.

#### Get Current Route

```bash
curl http://localhost:8009/route
```

**Response:** Returns `1` or `2` indicating which server is currently selected.

#### Switch Route

```bash
# Switch to server 1
curl -X POST http://localhost:8009/route/1

# Switch to server 2
curl -X POST http://localhost:8009/route/2
```

**Response:** Returns the previous server selection.

### Interactive Control

SwitchProxy also accepts commands via stdin for interactive control:

- Type `1` and press Enter to switch to server 1
- Type `2` and press Enter to switch to server 2
- Press `Ctrl+C` to shutdown

## Migration Workflow Example

Here's a complete example of using SwitchProxy for a Redis migration:

### 1. Start SwitchProxy

```bash
# Old Redis: localhost:6378
# New Redis: localhost:6377
# Clients will connect to: localhost:6379

switchproxy 127.0.0.1:8009 127.0.0.1:6379 localhost:6378 localhost:6377
```

### 2. Verify Initial State

```bash
# Check current route (should be 1)
curl http://localhost:8009/route
# Output: 1

# Connect a client through the proxy
redis-cli -p 6379 SET mykey "hello"
redis-cli -p 6379 GET mykey
# Output: "hello"
```

### 3. Migrate Data

Sync data from old Redis to new Redis using your preferred method (e.g., DUMP/RESTORE, replication, or Eden migrations).

### 4. Switch Traffic

```bash
# Switch all traffic to the new server
curl -X POST http://localhost:8009/route/2
# Output: 1 (previous selection)

# Verify the switch
curl http://localhost:8009/route
# Output: 2
```

### 5. Verify Migration

```bash
# Clients are now transparently connected to the new server
redis-cli -p 6379 GET mykey
# Output: "hello" (from new server)
```

## How It Works

1. **Connection Handling**: When a client connects, SwitchProxy establishes a connection to the currently selected backend server and relays traffic bidirectionally.

2. **Live Switching**: When the server selection changes, SwitchProxy:

   - Notifies all active connection handlers
   - Each handler closes its current upstream connection
   - Immediately reconnects to the new backend server
   - Continues relaying traffic without dropping the client connection

3. **Lock-Free Selection**: Server selection uses atomic operations for thread-safe, lock-free updates that don't block connection handling.

4. **Low Latency**: TCP_NODELAY is enabled on all connections to disable Nagle's algorithm, minimizing latency for interactive protocols.

## Configuration

### Environment Variables

| Variable         | Description                                          | Default           |
| ---------------- | ---------------------------------------------------- | ----------------- |
| `EDEN_LOG_LEVEL` | Log levels to emit, semicolon-separated (info;warn)  | All compiled logs |
| `RUST_LOG`       | Tracing subscriber filter (trace, debug, info, etc.) | `info`            |

### Logging

SwitchProxy uses Eden's logging system which supports two-tier filtering:

```bash
# Show only info and warn logs
EDEN_LOG_LEVEL=info;warn switchproxy 127.0.0.1:8009 127.0.0.1:6379 localhost:6378 localhost:6377

# Show only error logs
EDEN_LOG_LEVEL=error switchproxy 127.0.0.1:8009 127.0.0.1:6379 localhost:6378 localhost:6377

# Disable all logs
EDEN_LOG_LEVEL=none switchproxy 127.0.0.1:8009 127.0.0.1:6379 localhost:6378 localhost:6377

# Enable debug-level tracing output
RUST_LOG=debug switchproxy 127.0.0.1:8009 127.0.0.1:6379 localhost:6378 localhost:6377
```

**Note**: If `EDEN_LOG_LEVEL` is not set, all compiled logs are emitted. When set, only the specified levels are shown.

## Integration with Eden Migrations

SwitchProxy can be used as part of an Eden migration workflow:

1. Create endpoints for both source and target databases in Eden
2. Start SwitchProxy pointing to both databases
3. Create and execute an Eden migration to sync data
4. Use the SwitchProxy control API to switch traffic
5. Verify data integrity on the new database
6. Decommission the old database

See [Redis Migration Demo](../examples/demo.md) for a complete example using Eden migrations with traffic switching.

## Limitations

- Currently supports exactly 2 backend servers
- TCP-only (no TLS termination at the proxy level)
- Stateless protocol support only (connection switch may interrupt in-flight transactions)

## Related

- [Migrations Guide](./migrations.md) - Database migration concepts
- [Interlays](../api/interlays.md) - Eden's built-in traffic routing layer
- [Redis Migration Demo](../examples/demo.md) - Complete migration example
