# Redis Populator

A command-line tool to populate a Redis database with configurable amounts of data using various Redis data types.

## Features

- Populate Redis with a specified amount of data (in megabytes)
- Configurable individual key/value sizes
- Support for multiple Redis data types:
  - **STRING** - Simple key-value pairs (default)
  - **JSON** - JSON documents (requires RedisJSON module)
  - **HASH** - Hash maps with multiple fields
  - **LIST** - Ordered lists of elements
  - **SET** - Unordered collections of unique elements
  - **SORTED SET** - Scored sets for rankings/leaderboards
- Concurrent connections for high throughput
- Pipelined batch operations
- Progress bar with ETA
- Optional TTL for all keys
- Clear existing keys before populating

## Installation

```bash
cd examples/redis-populator
cargo build --release
```

## Usage

### Basic Usage

```bash
# Populate with 100MB of STRING data (default)
cargo run --release -- --mb 100

# Populate with 1GB of data, 4KB per key
cargo run --release -- --mb 1000 --size 4096
```

### Command Line Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--host` | `-H` | Redis host to connect to | `localhost` |
| `--port` | `-P` | Redis port to connect to | `6379` |
| `--mb` | `-m` | Total megabytes of data to generate | `1000` |
| `--size` | `-s` | Individual key/value size in bytes | `1024` |
| `--string` | | Use STRING data type | (default) |
| `--json` | | Use JSON data type | |
| `--hash` | | Use HASH data type | |
| `--list` | | Use LIST data type | |
| `--set` | | Use SET data type | |
| `--zset` | | Use SORTED SET data type | |
| `--prefix` | `-p` | Key prefix for all generated keys | `pop` |
| `--concurrency` | `-c` | Number of concurrent connections | `50` |
| `--batch-size` | `-b` | Batch size for pipelining | `100` |
| `--ttl` | `-t` | TTL in seconds (0 = no expiry) | `0` |
| `--elements-per-key` | `-e` | Elements per key (hash/list/set/zset) | `10` |
| `--clear` | | Clear existing keys with same prefix | false |

### Data Type Examples

```bash
# STRING (default) - Simple key-value pairs
cargo run --release -- --mb 500 --string

# JSON - Structured JSON documents (requires RedisJSON)
cargo run --release -- --mb 500 --json

# HASH - Hash maps with 10 fields per key
cargo run --release -- --mb 500 --hash --elements-per-key 10

# LIST - Lists with 20 elements each
cargo run --release -- --mb 500 --list --elements-per-key 20

# SET - Sets with 15 unique members
cargo run --release -- --mb 500 --set --elements-per-key 15

# SORTED SET - Sorted sets with random scores
cargo run --release -- --mb 500 --zset --elements-per-key 25
```

### Advanced Examples

```bash
# Populate 10GB with 8KB keys, high concurrency
cargo run --release -- \
  --mb 10000 \
  --size 8192 \
  --concurrency 100 \
  --batch-size 200

# Populate with TTL (expire after 1 hour)
cargo run --release -- --mb 1000 --ttl 3600

# Clear existing data and repopulate
cargo run --release -- --mb 500 --clear --prefix myapp

# Connect to remote Redis
cargo run --release -- \
  --host redis.example.com \
  --port 6379 \
  --mb 1000

# Connect to different port
cargo run --release -- \
  --port 6380 \
  --mb 1000
```

### Environment Variables

All options can also be set via environment variables:

```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export MEGABYTES=1000
export KEY_SIZE=2048
export KEY_PREFIX=mydata
export CONCURRENCY=75
export BATCH_SIZE=150
export TTL=7200
export ELEMENTS_PER_KEY=15

cargo run --release
```

## Output Example

```
Redis Populator
================
Redis:           localhost:6379
Data Type:       STRING
Total Data:      1000 MB
Key Size:        1024 bytes
Keys to Create:  1024000
Concurrency:     50
Batch Size:      100
Key Prefix:      pop

Connected to Redis successfully
 [00:00:32] [########################################] 1000.00 MiB/1000.00 MiB (100%) [31.25 MiB/s] ETA: 0s

Population Complete
===================
Keys Created:    1024000
Data Written:    1000.00 MB
Time Elapsed:    32.15s
Throughput:      31.10 MB/s (31847 keys/s)
Total DB Keys:   1024000
```

## Performance Tips

1. **Increase concurrency** for faster throughput: `--concurrency 100`
2. **Use larger batch sizes** for fewer round trips: `--batch-size 500`
3. **Use STRING type** for maximum speed (simplest operations)
4. **Build with release mode** for best performance: `cargo run --release`
5. **Use local Redis** or ensure low latency connection

## Data Type Considerations

| Type | Use Case | Notes |
|------|----------|-------|
| STRING | Simple caching, sessions | Fastest, most common |
| JSON | Structured documents | Requires RedisJSON module |
| HASH | User profiles, objects | Good for partial updates |
| LIST | Queues, recent items | Ordered, allows duplicates |
| SET | Tags, unique visitors | No duplicates, unordered |
| SORTED SET | Leaderboards, time series | Scored ranking |

## Requirements

- Rust 1.70+ (edition 2021)
- Redis 6.0+ (or Redis Stack for JSON support)
- Network access to Redis instance
