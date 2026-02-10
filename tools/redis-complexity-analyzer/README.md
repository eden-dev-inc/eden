# Redis Complexity Analyzer

A real-time TUI tool to analyze Redis database complexity and data type distribution.

## Features

- **Real-time TUI Dashboard**: Live-updating terminal interface showing all metrics
- **Historical Tracking**: Tracks current, average, and maximum values over time
- **Data Scale Analysis**: Memory usage monitoring
- **Key Count Analysis**: Total key tracking
- **Type Distribution**: Samples keys to determine data type mix including core types (strings, hashes, lists, sets, sorted sets, streams) and Redis modules (JSON, TimeSeries, Bloom/Cuckoo filters, Graph, Search, Gears)
- **Redis 7 & 8 Compatible**: Supports both legacy module type strings and Redis 8+ native type identifiers
- **Throughput Analysis**: Monitors ops/sec

## Installation

```bash
cd tools/redis-complexity-analyzer
cargo build --release
```

## Usage

```bash
# Default: Real-time TUI dashboard (refreshes every 5 seconds)
redis-complexity-analyzer -H redis.example.com -P 6379

# Custom refresh interval (every 30 seconds)
redis-complexity-analyzer -H redis.example.com -i 30

# With authentication
redis-complexity-analyzer -H redis.example.com -P 6379 -a mypassword

# One-shot console output (disables TUI)
redis-complexity-analyzer -H redis.example.com --once

# JSON output for automation (disables TUI)
redis-complexity-analyzer -H redis.example.com -o json

# Custom sampling rate (10% of keys)
redis-complexity-analyzer -H redis.example.com -s 0.10
```

## CLI Options

```
Options:
  -H, --host <HOST>           Redis host [default: localhost]
  -P, --port <PORT>           Redis port [default: 6379]
  -a, --password <PASSWORD>   Redis password
  -d, --db <DB>               Database number [default: 0]
  -s, --sample-rate <RATE>    Sample % (0.01-1.0) [default: 0.05]
      --min-samples <N>       Minimum samples [default: 1000]
      --max-samples <N>       Maximum samples [default: 100000]
  -o, --output-format <FMT>   Output: console, json (disables TUI)
  -i, --interval <SECS>       TUI refresh interval [default: 5]
      --once                  Run once and exit (disables TUI)
```

## TUI Dashboard

By default, the tool launches an interactive terminal dashboard that updates in real-time:

```
┌──────────────────────────────────────────────────────────────────────┐
│ Redis Complexity Analyzer  CONNECTED   redis.example.com:6379 (v7.2.4)│
└──────────────────────────────────────────────────────────────────────┘
┌─ Database Metrics (42 samples) ─────────────────────────────────────┐
│ Metric      Current      Average      Maximum                        │
│ Memory      98.50 GB     95.20 GB     100.00 GB                      │
│ Keys        19.80M       19.50M       20.00M                         │
│ Ops/sec     48.00K       45.00K       50.00K                         │
│ Clients     42           -            -                              │
└──────────────────────────────────────────────────────────────────────┘
┌─ Type Distribution (1.00M sampled, 5.0% coverage) ───────────────────┐
│ String        50.0%  ██████████                                      │
│ Hash          30.0%  ██████                                          │
│ Sorted Set    20.0%  ████                                            │
└──────────────────────────────────────────────────────────────────────┘
 Press 'q' to quit | Refresh: 5s | Last update: 2.3s ago | Updates: 15
```

**TUI Controls:**
- `q` or `Esc`: Quit the application

**Metrics Tracking:**
- **Current**: Real-time value from the latest sample
- **Average**: Running average across all samples in this session
- **Maximum**: Peak value observed during this session

## Console Output

Use `--once` or `-o console` for one-shot analysis:

```
Redis Complexity Analyzer
=========================

Target: redis.example.com:6379 (v7.2.4)

Database Metrics
----------------
  Memory:     100.00 GB
  Keys:       20.00M
  Ops/sec:    50.00K
  Clients:    42

Type Distribution (1.00M sampled, 5.0% coverage)
------------------------------------------------
  String        50.0% ██████████
  Hash          30.0% ██████
  Sorted Set    20.0% ████

Completed in 2,341ms
```

## Supported Data Types

### Core Redis Types

| Type | Description |
|------|-------------|
| String | Basic key-value storage |
| Hash | Field-value maps |
| List | Ordered collections |
| Set | Unordered unique collections |
| Sorted Set | Scored, ordered collections |
| Stream | Append-only log structures |

### Redis Module Types

| Type | Module |
|------|--------|
| JSON | RedisJSON |
| TimeSeries | RedisTimeSeries |
| Bloom Filter | RedisBloom |
| Cuckoo Filter | RedisBloom |
| Count-Min Sketch | RedisBloom |
| Top-K | RedisBloom |
| T-Digest | RedisBloom |
| Graph | RedisGraph |
| Search Index | RediSearch |
| Gears Function | RedisGears |

## JSON Output

Use `-o json` for machine-readable output:

```json
{
  "timestamp": "2026-02-05T21:30:00Z",
  "host": "redis.example.com",
  "port": 6379,
  "metrics": {
    "used_memory_bytes": 107374182400,
    "total_keys": 20000000,
    "ops_per_sec": 50000,
    "redis_version": "7.2.4",
    "connected_clients": 50
  },
  "type_distribution": {
    "counts": { "string": 500000, "hash": 300000, "zset": 200000 },
    "total_sampled": 1000000
  },
  "sample_coverage": 5.0,
  "duration_ms": 2341
}
```
