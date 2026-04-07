# Eden Redis Migration Analyzer

Analyze Redis database complexity, estimate Azure migration pricing and timeline.

Connects to a live Redis instance, analyzes its data types, configuration complexity, and module usage, then lets you interactively select your target Azure Cache for Redis SKU and region to get a migration price and timeline estimate.

## Features

- **Real-time TUI Dashboard**: Live-updating terminal interface with three views
  - **[1] Analysis** — Memory, keys, ops/sec, type distribution with historical tracking
  - **[2] Complexity** — Configuration scoring (cluster mode, ACLs, modules, persistence, Lua scripts, pub/sub)
  - **[3] Pricing** — Interactive Azure region & SKU selector with pricing and time estimates
- **Azure Retail Prices API Integration**: Live pricing from the Azure Retail Prices API — no auth required
- **Complexity-Based Pricing**: 10% base price with 1.0x–2.5x complexity multiplier
- **Migration Time Comparison**: Exodus vs manual migration timeline side-by-side
- **Redis 7 & 8 Compatible**: Supports core types and modules (JSON, TimeSeries, Bloom, Search, Gears, Graph)

## Installation

### Homebrew (macOS / Linux)

```bash
brew tap eden-platform/eden
brew install eden-redis-migration-analyzer
```

### From source

```bash
cd tools/redis-complexity-analyzer
cargo build --release
# Binary is at target/release/eden-redis-migration-analyzer
```

## Usage

```bash
# Default: Real-time TUI dashboard (refreshes every 5 seconds)
eden-redis-migration-analyzer -H redis.example.com -P 6379

# With password authentication
eden-redis-migration-analyzer -H redis.example.com -P 6379 -a mypassword

# With ACL auth (Redis 6+ username + password)
eden-redis-migration-analyzer -H redis.example.com -u myuser -a mypassword

# TLS connection (Azure, Elasticache, etc.)
eden-redis-migration-analyzer -H myredis.redis.cache.windows.net -P 6380 --tls -a mykey

# TLS with self-signed certificate
eden-redis-migration-analyzer -H redis.internal -P 6379 --tls --tls-insecure -a mypassword

# One-shot console output with pricing (disables TUI)
eden-redis-migration-analyzer -H redis.example.com --once --azure-region eastus

# JSON output for automation (disables TUI)
eden-redis-migration-analyzer -H redis.example.com -o json --azure-region westeurope

# Custom sampling rate (10% of keys)
eden-redis-migration-analyzer -H redis.example.com -s 0.10
```

## CLI Options

```
Options:
  -H, --host <HOST>              Redis host [default: localhost]
  -P, --port <PORT>              Redis port [default: 6379]
  -a, --password <PASSWORD>      Redis password
  -u, --username <USERNAME>      Redis username (ACL auth, Redis 6+)
      --tls                      Enable TLS/SSL connection
      --tls-insecure             Allow invalid TLS certificates (self-signed)
  -n, --db <DB>                  Database number [default: 0]
  -s, --sample-rate <RATE>       Sample % (0.01-1.0) [default: 0.05]
      --min-samples <N>          Minimum samples [default: 1000]
      --max-samples <N>          Maximum samples [default: 100000]
  -o, --output-format <FMT>      Output: console, json (disables TUI)
  -i, --interval <SECS>          TUI refresh interval [default: 5]
      --once                     Run once and exit (disables TUI)
      --azure-region <REGION>    Azure region for pricing [default: eastus]
```

All connection options can also be set via environment variables: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_USERNAME`, `REDIS_TLS`, `REDIS_TLS_INSECURE`, `REDIS_DB`.

## TUI Dashboard

The tool launches an interactive terminal dashboard with three views:

### [1] Analysis View

```
┌──────────────────────────────────────────────────────────────────────┐
│ Eden Redis Migration Analyzer  CONNECTED  redis.example.com:6379 (v7.2.4) │
└──────────────────────────────────────────────────────────────────────┘
┌─ Database Metrics ───────────────────────────────────────────────────┐
│ Metric      Current      Average      Maximum                        │
│ Memory      98.50 GB     95.20 GB     100.00 GB                      │
│ Keys        19.80M       19.50M       20.00M                         │
│ Ops/sec     48.00K       45.00K       50.00K                         │
└──────────────────────────────────────────────────────────────────────┘
┌─ Type Distribution ──────────────────────────────────────────────────┐
│ String        50.0%  ██████████                                      │
│ Hash          30.0%  ██████                                          │
│ Sorted Set    20.0%  ████                                            │
└──────────────────────────────────────────────────────────────────────┘
```

### [3] Pricing View

Select Azure region (left panel), browse SKUs (right panel), see pricing and timeline:

```
┌─ Azure Region ─────────┐┌─ East US — 42 SKUs ─────────────────────┐
│   ▶ East US (eastus)    ││ SKU          Meter       $/Hour  Annual │
│     East US 2           ││ ▶ C0 Basic   C0 Basic    0.0220  $193  │
│     West US             ││   C1 Basic   C1 Basic    0.0550  $482  │
│ ●   West Europe         ││   P1 Premium P1 Premium  0.5710  ...   │
└────────────────────────┘└────────────────────────────────────────────┘
┌─ Exodus Pricing Estimate ───┐┌─ Exodus vs Manual Migration ─────────┐
│ Tier: Moderate (15%)         ││ Data Size: 98.50 GB                  │
│ SKU: C0 Basic                ││ Exodus: <1hr setup + 24hr = 25 hours │
│ Annual Azure Spend: $192.72  ││ Manual: 2w plan + 2w impl + 1.5w test│
│ Exodus Price: $1,000/yr      ││ Time Saved: 97% faster with Exodus   │
└──────────────────────────────┘└──────────────────────────────────────┘
```

### TUI Controls

| Key | Action |
|-----|--------|
| `q` / `Esc` | Quit |
| `Tab` / `1` / `2` / `3` | Switch view |
| `Up` / `Down` | Scroll lists |
| `Left` / `Right` | Switch focus (Pricing view: region ↔ SKU) |
| `Enter` | Confirm selection (region or SKU) |

## Complexity Scoring

Each finding from the analysis contributes to the total complexity score:

| Severity | Points | Description |
|----------|--------|-------------|
| Info | 1 | Informational — minimal migration effort |
| Warning | 3 | Requires configuration changes or testing |
| Critical | 5 | Requires architecture changes or feature alternatives |

### What Gets Analyzed

| Category | Finding | Severity | Points |
|----------|---------|----------|--------|
| **Clustering** | Standalone mode | Info | 1 |
| **Clustering** | OSS Cluster mode enabled | Critical | 5 |
| **Clustering** | Read replicas detected | Warning | 3 |
| **ACLs** | Default ACLs only | Info | 1 |
| **ACLs** | Custom ACL rules (must recreate with Entra ID) | Critical | 5 |
| **Modules** | Compatible module (JSON, Search, Bloom, TimeSeries) | Info | 1 |
| **Modules** | Flash-tier-restricted module | Warning | 3 |
| **Modules** | Unsupported module (RedisGears, Graph) | Critical | 5 |
| **Features** | Keyspace notifications enabled (not supported in AMR) | Critical | 5 |
| **Features** | Lua scripts (EVAL/EVALSHA usage) | Warning | 3 |
| **Features** | Active Pub/Sub channels | Warning | 3 |
| **Persistence** | No persistence (ephemeral) | Info | 1 |
| **Persistence** | RDB snapshots enabled | Warning | 3 |
| **Persistence** | AOF persistence enabled | Warning | 3 |
| **Configuration** | Standard eviction policy | Info | 1 |
| **Configuration** | Non-standard eviction policy | Warning | 3 |
| **Configuration** | RediSearch with incompatible eviction policy | Critical | 5 |
| **Connection** | Non-TLS connection (port 6379) | Warning | 3 |
| **Connection** | Endpoint change required | Info | 1 |
| **Commands** | Multi-key cross-slot commands detected | Warning | 3 |
| **Scale** | Small dataset (< 1 GB) | Info | 1 |
| **Scale** | Dataset 1–25 GB | Warning | 3 |
| **Scale** | Dataset 25–100 GB | Warning | 3 |
| **Scale** | Large dataset (100+ GB) | Critical | 5 |
| **Scale** | Low throughput (< 1K ops/sec) | Info | 1 |
| **Scale** | Throughput 1K–10K ops/sec | Warning | 3 |
| **Scale** | Throughput 10K–50K ops/sec | Warning | 3 |
| **Scale** | High throughput (50K+ ops/sec) | Critical | 5 |

### Tier Thresholds

| Tier | Score Range | Multiplier |
|------|------------|------------|
| Simple | 0–10 | 1.0x |
| Moderate | 11–25 | 1.5x |
| Difficult | 26–50 | 2.0x |
| Complex | 51+ | 2.5x |

### Example Scenarios

**Simple (score 0–10)** — A standalone Redis with default config:
- Standalone (1) + Default ACLs (1) + No modules (1) + No persistence (1) + Standard eviction (1) + Endpoint change (1) + Small data (1) + Low ops (1) = **score 8**

**Moderate (score 11–25)** — A production cache with some customization:
- Standalone (1) + Custom ACLs (+5) + RDB (+3) + AOF (+3) + Lua scripts (+3) + Non-TLS (+3) + 1-25 GB data (+3) + 1K-10K ops (+3) = **score 24**

**Difficult (score 26–50)** — A complex setup with cluster mode and modules:
- OSS Cluster (+5) + Custom ACLs (+5) + Keyspace notifications (+5) + Flash-restricted module (+3) + RDB (+3) + AOF (+3) + Lua scripts (+3) + Pub/Sub (+3) + Non-TLS (+3) + Cross-slot commands (+3) + 25-100 GB (+3) + 10K-50K ops (+3) = **score 47**

**Complex (score 51+)** — Requires many findings stacking up:
- OSS Cluster (5) + Custom ACLs (5) + Keyspace notifications (5) + 2 unsupported modules (5+5) + RediSearch eviction conflict (5) + Flash module (3) + RDB (3) + AOF (3) + Lua (3) + Pub/Sub (3) + Non-TLS (3) + Read replicas (3) + Cross-slot (3) + 100+ GB data (5) + 50K+ ops (5) = **score 69**
- To reach Complex, the database must have nearly every possible complication. Most real-world databases fall into Simple or Moderate.

## Pricing

Exodus annual license is **10% of annual Azure spend** (rounded to nearest $100, minimum $2,500/yr, capped at 20% of spend).

Includes: fully automated migration + ongoing monitoring + analysis + AI integrations + support.

A complexity multiplier is applied based on the analysis:

| Tier | Score | Multiplier | Example: $10K/yr Azure spend |
|------|-------|------------|------------------------------|
| Simple | 0–10 | 1.0x | $2,500/yr (min) |
| Moderate | 11–25 | 1.5x | $3,800/yr |
| Difficult | 26–50 | 2.0x | $5,000/yr |
| Complex | 51+ | 2.5x | $5,000/yr (capped at 20%) |

The displayed price is an estimate — final pricing depends on a full complexity assessment.

### Full Pricing Examples

**Example 1: Small startup cache**
- ACR SKU: C1 Standard ($0.055/hr → $482/yr)
- Database: 500 MB, 200 ops/sec, standalone, default config
- Complexity: Simple (score 8 — all Info findings + non-TLS warning)
- Exodus base: $1,000/yr (10% = $48, rounded to minimum)
- Multiplier: 1.0x → **Exodus price: $1,000/yr**
- Manual cost: $8,000 one-time (40 hrs × $200/hr)
- **You save $7,000 — plus zero downtime risk**

**Example 2: Mid-size production cache**
- ACR SKU: P2 Premium ($0.555/hr → $4,862/yr)
- Database: 10 GB, 5K ops/sec, custom ACLs, RDB snapshots, Lua scripts
- Complexity: Moderate (score 24 — ACLs +5, RDB +3, Lua +3, TLS +3, size +3, ops +3, infos)
- Exodus base: $1,000/yr (10% = $486, rounded to minimum)
- Multiplier: 1.5x → **Exodus price: $1,500/yr**
- Manual cost: $12,000 one-time (60 hrs × $200/hr)
- **You save $10,500 — with built-in rollback & validation**

**Example 3: Large enterprise deployment**
- ACR SKU: E100 Enterprise ($3.769/hr → $33,016/yr)
- Database: 80 GB, 25K ops/sec, OSS cluster, keyspace notifications, custom ACLs, RDB+AOF, Lua, pub/sub
- Complexity: Difficult (score 47)
- Exodus base: $3,300/yr (10% of $33,016, rounded to $100)
- Multiplier: 2.0x → **Exodus price: $6,600/yr**
- Manual cost: $16,000 one-time (80 hrs × $200/hr)
- **You save $9,400 — with milliseconds of downtime**

**Example 4: High-throughput complex system**
- ACR SKU: E400 Enterprise ($15.076/hr → $132,066/yr)
- Database: 300 GB, 80K ops/sec, OSS cluster, RedisGears, custom ACLs, keyspace notifications, RDB+AOF, Lua, pub/sub
- Complexity: Complex (score 69)
- Exodus base: $13,200/yr (10% of $132,066, rounded to $100)
- Multiplier: 2.5x → **Exodus price: $33,000/yr**
- Manual cost: $24,000 one-time (120 hrs × $200/hr)
- Note: Manual is cheaper one-time, but Exodus includes ongoing support, validated migration, rollback, and milliseconds of downtime — manual does not.

## Why Eden vs Doing It Yourself

| | Eden | Do It Yourself |
|---|------|----------------|
| **Automation** | Fully automated | Custom scripts |
| **Timeline** | Minutes to hours | 4–12 weeks |
| **Cost** | 10% of Azure spend/yr | $8,000–$24,000 one-time |
| **Downtime** | Milliseconds | Hours to days |
| **Data Integrity** | Checksum verified — every key validated | Manual spot-checks |
| **Rollback** | Instant, built-in | Restore from last backup |
| **Client Changes** | Transparent DNS cutover | Rewrite connection strings across services |
| **Compliance** | Full audit trail (SOC2/PCI ready) | Manual screenshots |
| **Battle-Tested** | Hundreds of migrations | Your team's first attempt |
| **Support** | Included | None |

### Manual Cost Breakdown

Assumes 10 hours/week of dedicated engineering time at $200/hr:

| Complexity | Weeks | Hours | Manual Cost |
|------------|-------|-------|-------------|
| Simple | 4 | 40 | $8,000 |
| Moderate | 6 | 60 | $12,000 |
| Difficult | 8 | 80 | $16,000 |
| Complex | 12 | 120 | $24,000 |

Manual migrations also carry hidden costs: extended downtime, data loss risk during cutover, no automated rollback, rewriting connection strings across every service, no compliance audit trail, and no ongoing support.

## ACR → AMR Infrastructure Savings

Azure Managed Redis (AMR) is often significantly cheaper than legacy Azure Cache for Redis (ACR) for the same capacity. The Recommend tab (4) right-sizes your AMR instance based on your actual workload.

### Comparable Pricing (East US)

| Workload | Current ACR | $/yr | Recommended AMR | $/yr | Savings |
|----------|-------------|------|-----------------|------|---------|
| 6 GB general | P1 Premium | $2,427 | B5 Balanced | $1,367 | **44%** |
| 13 GB general | P2 Premium | $9,724 | B10 Balanced | $2,759 | **72%** |
| 26 GB general | P3 Premium | $19,438 | B20 Balanced | $5,510 | **72%** |
| 50 GB general | E50 Enterprise | $16,513 | B50 Balanced | $11,011 | **33%** |
| 100 GB memory | E100 Enterprise | $33,016 | M100 Memory | $15,102 | **54%** |
| 250 GB flash | F700 Flash | $35,136 | A500 Flash | $27,331 | **22%** |

Note: Compute-optimized AMR (X-series) may cost more than Enterprise ACR for the same capacity — but you get dedicated vCPUs and better tail latency. The Recommend tab picks the best profile for your workload pattern.

### Total Savings: Exodus + AMR Migration

Combining Exodus migration pricing with AMR infrastructure savings:

**P2 Premium (13 GB) → B10 Balanced**
- Current ACR: $9,724/yr
- New AMR: $2,759/yr (save $6,965/yr on infra)
- Exodus price: $1,000/yr (10% of $2,759 = minimum)
- **Year 1 net savings: $5,965** ($9,724 - $2,759 - $1,000)
- **Year 2+ savings: $6,965/yr**
- Manual DIY cost: $12,000 (60 hrs) + downtime risk

**E100 Enterprise (100 GB) → M100 Memory Optimized**
- Current ACR: $33,016/yr
- New AMR: $15,102/yr (save $17,914/yr on infra)
- Exodus base: $1,500/yr (10% of $15,102 → $1,510, rounded)
- Complexity: Moderate (1.5x) → Exodus price: $2,300/yr
- **Year 1 net savings: $15,614** ($33,016 - $15,102 - $2,300)
- **Year 2+ savings: $17,914/yr**
- Manual DIY cost: $16,000 (80 hrs) + weeks of downtime risk

The migration pays for itself in infrastructure savings alone, before counting the avoided cost of building a manual migration.

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
