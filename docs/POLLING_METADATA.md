# Architecture at a glance

Two layers:

1. Endpoint-agnostic job scheduler and capability system, lives in `endpoint-types`
2. Per-endpoint collectors (PostgreSQL, Redis, MongoDB and others) with their own queries and capability discovery

Seven crates matter:

- `endpoint-types/src/metadata/`: core traits (`SyncCollector`, `SyncMetadata`, `MetadataCollection`, `CapabilityChecker`), job executor, config, publisher
- `postgres/src/metadata/`: 14 PG collectors, capability discovery, version-conditional SQL
- `ep-redis/src/metadata/`: 11 Redis collectors (standalone loader functions), module/cluster discovery
- `mongo/src/metadata/`: 20 MongoDB collectors, topology discovery, profiling opt-in
- `cassandra/src/metadata/`: 10 Cassandra collectors, capability discovery, virtual-table branching, system-table gating
- `clickhouse/src/metadata/`: 14 ClickHouse collectors, capability discovery, feature-based job gating
- `oracle/src/metadata/`: 15 Oracle collectors, capability discovery, edition/version/license gating

# How a poll cycle works

The metadata scheduler runs three independent tick loops, one per frequency tier. On each tick:

1. For each registered endpoint, acquire a connection from the pool.
2. Call `discover_capabilities()` once per connection, returning a `Box<dyn CapabilityChecker>` reused for all jobs this cycle.
3. Build the job list via `SyncMetadata::jobs(frequency)`. Each `MetadataJob` carries a name, executor closure and optional capability requirements.
4. Pass the job list to `run_metadata_jobs_with_capabilities()`, which iterates sequentially (priority order), checks requirements, executes and collects `JobReport`s.
5. The result is a `MetadataBatch<M>` (the updated metadata struct, reports and timing), serialized to JSON and published to Redis via `MetadataOutputs`.

Endpoints are processed concurrently within a tick, capped at `max_concurrent_endpoints` (default 4). Each gets an `endpoint_timeout` (default 120s) wall-clock cap. A per-endpoint mutex serializes cache writes across tiers so that concurrent High/Medium/Low workers don't overwrite each other's updates; the lock is acquired outside the timeout so waiting behind another tier doesn't eat the wall-clock budget.

# Frequency tiers

Three tiers, each with independent tick intervals:

| Tier   | Default interval | What it covers                                             |
|--------|------------------|------------------------------------------------------------|
| High   | 60s              | Connections, locks, activity, WAL, replication, performance |
| Medium | 30min            | Bgwriter, tables, indexes, vacuum, databases, persistence  |
| Low    | 24h              | Extensions, settings, configuration, security              |

This distribution uses fixed metadata polling intervals. Low-frequency collectors are never accelerated.

# Backoff on failure

If a poll cycle fails for an endpoint, the scheduler backs off exponentially: base 30s, factor 2x, max 900s. The counter resets on a successful cycle.

# The SyncCollector trait

Every collector struct implements `SyncCollector<A>` where `A` is the connection type (`PostgresAsync`, `RedisAsync`, `MongoAsync`, `CassandraAsync`, `OracleAsync`, `ClickhouseAsync`):

The single method is `sync_metadata(&self, context, telemetry, capabilities) -> BoxFuture<ResultEP<Self>>`. The `&self` receiver acts as a template: the collector reads its own configuration and produces a fresh instance that replaces the old state in the metadata struct.

Each endpoint wires this via a macro (`impl_sync_collector_pg!`, `_redis!`, `_mongo!`, `_cass!`, `_oracle!`, `_ch!`) delegating to an inherent `sync_metadata` method or standalone loader function (Redis).

# MetadataCollection trait

Every collector also implements `MetadataCollection` for its query definitions and schedule. The associated type `Request` (typically `HashMap<String, QueryInput>`) is returned by `request()`, which declares all query variants the collector might need. Methods `description()`, `category()`, `interval()` and `profiling_requirement()` round it out. `sync_metadata` picks which queries to execute based on capabilities.

# Capability system

## Core types

`CapabilityId(&'static str)` is a newtype (e.g. `"pg.version.17"`). `CapabilityChecker` is a trait with a single method `has(&self, id: &CapabilityId) -> bool`. Two built-in implementations:
- `UnknownCapabilities`: `has()` always returns false. Used when discovery fails; only unrestricted jobs run.
- `PermissiveCapabilities`: `has()` always returns true. Used by `run_metadata_jobs()` and in tests.

## Discovery

Each endpoint implements `SyncMetadata::discover_capabilities()`, running lightweight probes once per connection:

- **PostgreSQL**: `SHOW server_version` (major version), `pg_is_in_recovery()` (role), `pg_extension` (installed extensions)
- **Redis**: `INFO cluster` (cluster flag), `INFO server` (version), `INFO modules` (loaded modules, parsed from raw lines because the HashMap collapses duplicate `module:` keys)
- **MongoDB**: `hello` (topology: mongos vs replica set), `listShards` (sharded cluster)
- **Cassandra**: `system.local` (version, cluster name, partitioner), `system_schema.tables WHERE keyspace_name = 'system'` (system table inventory), `system_schema.keyspaces WHERE keyspace_name = 'system_views'` (virtual tables probe). All three queries run under a 5s timeout; partial failures leave the corresponding capabilities false rather than failing discovery entirely.
- **Oracle**: `v$instance` for version and edition (tries `version_full` first, falls back to `version` for pre-18c), `v$database` for CDB status, `v$parameter` for Diagnostics Pack license (`control_management_pack_access`), probe query against `dba_tables` for DBA view access. Each probe runs under a 5s timeout and degrades independently.
- **ClickHouse**: `SELECT version()` for version string, then count-based probes against `system.replicas`, `system.zookeeper`, `system.clusters` and `system.dictionaries`. Each probe runs under a 5s timeout; failures default to false.

If discovery fails, `UnknownCapabilities` is used and only unrestricted jobs run.

## Capability constants

Each endpoint defines its capabilities as `pub const` values:

**PostgreSQL** (`postgres/src/metadata/capabilities.rs`):
- `PG_VERSION_14`, `PG_VERSION_16`, `PG_VERSION_17`, `PG_VERSION_18`: version gates
- `PG_ROLE_PRIMARY`: primary vs replica

The `has()` implementation uses prefix matching on `"pg.version."`: strips the prefix, parses the number, checks `version_major >= N`. So `PG_VERSION_16` is true on PG 16, 17, 18 and above.

**Redis** (`ep-redis/src/metadata/capabilities.rs`):
- `REDIS_CLUSTER`: cluster mode
- Dynamic `"redis.module.{name}"`: matches against `loaded_modules` vec

**MongoDB** (`mongo/src/metadata/capabilities.rs`):
- `MONGO_REPLICA_SET`: topology is replica set
- `MONGO_SHARDED`: topology is sharded
- `MONGO_SHARDED_OR_MONGOS`: either sharded or connected to mongos

**Cassandra** (`cassandra/src/metadata/capabilities.rs`):
- `CASSANDRA_VERSION_3`, `CASSANDRA_VERSION_4`, `CASSANDRA_VERSION_5`: version gates; `has()` uses prefix matching on `"cassandra.version."`, parses the suffix as `u32`, checks `version_major >= N` (same pattern as PG)
- `CASSANDRA_HAS_COMPACTION_HISTORY` (`"cassandra.table.compaction_history"`): `system.compaction_history` present
- `CASSANDRA_HAS_SSTABLE_ACTIVITY` (`"cassandra.table.sstable_activity"`): `system.sstable_activity` present
- `CASSANDRA_HAS_SNAPSHOTS_TABLE` (`"cassandra.table.snapshots"`): `system.snapshots` present
- `CASSANDRA_HAS_SIZE_ESTIMATES` (`"cassandra.table.size_estimates"`): `system.size_estimates` present
- `CASSANDRA_HAS_VIRTUAL_TABLES` (`"cassandra.virtual_tables"`): `system_views` keyspace exists (Cassandra 4.0+)

**Oracle** (`oracle/src/metadata/capabilities.rs`):
- `ORACLE_VERSION_12`, `_18`, `_19`, `_21`, `_23`: version gates (>= semantics, same prefix-match pattern)
- `ORACLE_EDITION_ENTERPRISE`: Enterprise Edition (case-insensitive match on `edition` from `v$instance`)
- `ORACLE_CDB`: Container Database / multitenant architecture (12c+)
- `ORACLE_DIAG_PACK`: Diagnostics Pack licensed (`control_management_pack_access` contains `"DIAGNOSTIC"`)
- `ORACLE_HAS_DBA_VIEWS`: `DBA_` prefixed views accessible (probed by querying `dba_tables`)

**ClickHouse** (`clickhouse/src/metadata/capabilities.rs`):
- `CLICKHOUSE_VERSION_21`, `_22`, `_23`, `_24`: version gates (>= semantics)
- `CLICKHOUSE_HAS_REPLICATION`: `system.replicas` has entries (replicated tables exist)
- `CLICKHOUSE_HAS_ZOOKEEPER`: `system.zookeeper` is queryable (Keeper/ZooKeeper configured)
- `CLICKHOUSE_HAS_CLUSTERS`: `system.clusters` has entries
- `CLICKHOUSE_HAS_DICTIONARIES`: `system.dictionaries` has entries

## Two levels of gating

### Job-level: skip before execution

Jobs declare requirements via `.with_requirement()` (e.g. WAL requires `PG_VERSION_14` + `PG_ROLE_PRIMARY`). `run_metadata_jobs_with_capabilities()` checks each requirement before executing. If any `capabilities.has(req)` returns false, the job is skipped with `SkipReason::CapabilityMissing`.

Current job-level requirements by endpoint:

| Endpoint   | Job                | Requirement                        | Reason                                              |
|------------|--------------------|------------------------------------|-----------------------------------------------------|
| Cassandra  | `compaction_info`  | `CASSANDRA_HAS_COMPACTION_HISTORY` | Hard dependency on `system.compaction_history`       |
| Cassandra  | `repair_info`      | `CASSANDRA_HAS_COMPACTION_HISTORY` | Hard dependency on `system.compaction_history`       |
| Cassandra  | `table_info`       | `CASSANDRA_HAS_SIZE_ESTIMATES`     | Hard dependency on `system.size_estimates`           |
| Oracle     | `index_info`       | `ORACLE_HAS_DBA_VIEWS`            | All queries use `DBA_` views exclusively             |
| Oracle     | `segment_info`     | `ORACLE_HAS_DBA_VIEWS`            | All queries use `DBA_` views exclusively             |
| Oracle     | `storage_info`     | `ORACLE_HAS_DBA_VIEWS`            | All queries use `DBA_` views exclusively             |
| Oracle     | `table_info`       | `ORACLE_HAS_DBA_VIEWS`            | All queries use `DBA_` views exclusively             |
| Oracle     | `tablespace_info`  | `ORACLE_HAS_DBA_VIEWS`            | All queries use `DBA_` views exclusively             |
| ClickHouse | `cluster_info`     | `CLICKHOUSE_HAS_CLUSTERS`         | Queries `system.clusters`                            |
| ClickHouse | `replication_info` | `CLICKHOUSE_HAS_REPLICATION`      | Queries `system.replicas`                            |
| ClickHouse | `zookeeper_info`   | `CLICKHOUSE_HAS_ZOOKEEPER`        | Queries `system.zookeeper`                           |
| ClickHouse | `dictionary_info`  | `CLICKHOUSE_HAS_DICTIONARIES`     | Queries `system.dictionaries`                        |

### Query-level: branch inside the collector

Collectors that need different SQL per capability call `capabilities.has()` directly inside `sync_metadata` to pick the right query or skip sub-queries.

**PostgreSQL**, version-conditional SQL:

| Collector       | Capability gate | What changes                                                                                  |
|-----------------|-----------------|-----------------------------------------------------------------------------------------------|
| `bgwriter.rs`   | `PG_VERSION_17` | Checkpoint stats moved from `pg_stat_bgwriter` to `pg_stat_checkpointer` with renamed columns |
| `wal.rs`        | `PG_VERSION_16` | `wal_write`, `wal_sync`, `wal_write_time`, `wal_sync_time` removed in PG 16                  |
| `extensions.rs` | `PG_VERSION_18` | `trusted` column removed from `pg_available_extensions` in PG 18                              |

**Cassandra**, virtual-table queries gated on `CASSANDRA_HAS_VIRTUAL_TABLES`:

| Collector        | Capability gate                | What changes                                                                      |
|------------------|--------------------------------|-----------------------------------------------------------------------------------|
| `threadpools.rs` | `CASSANDRA_HAS_VIRTUAL_TABLES` | Supplements/replaces per-node thread pool counts with `system_views.thread_pools` |
| `cluster.rs`     | `CASSANDRA_HAS_VIRTUAL_TABLES` | Adds connected client count from `system_views.clients`                           |
| `compaction.rs`  | `CASSANDRA_HAS_VIRTUAL_TABLES` | Adds active compaction task list from `system_views.sstable_tasks`                |

**Oracle**, DBA view access gating on `ORACLE_HAS_DBA_VIEWS`:

| Collector     | What it skips without DBA views                                                 |
|---------------|---------------------------------------------------------------------------------|
| `performance` | File I/O stats and tablespace I/O queries (`dba_data_files`, `dba_tablespaces`) |
| `sessions`    | Security metrics and session history (`dba_audit_trail`)                        |
| `database`    | Database size and tablespace counts (`dba_data_files`, `dba_free_space`)        |

**ClickHouse**, feature gating inside the cluster collector:

| Collector | Capability gate              | What it skips                                       |
|-----------|------------------------------|-----------------------------------------------------|
| `cluster` | `CLICKHOUSE_HAS_REPLICATION` | Replication queue query (`system.replication_queue`) |
| `cluster` | `CLICKHOUSE_HAS_ZOOKEEPER`   | ZooKeeper status queries                            |

Redis and MongoDB collectors receive capabilities but don't branch on them; their queries are stable across supported versions.

## Adding capabilities to a new endpoint

Purely additive, no core changes:

1. Create `capabilities.rs` in the endpoint crate with a struct implementing `CapabilityChecker`
2. Override `discover_capabilities()` in the endpoint's `SyncMetadata` impl
3. Define `pub const` capability IDs with a dotted namespace (e.g. `"oracle.version.19"`, `"clickhouse.has_zookeeper"`)
4. Use `.with_requirement()` on jobs for hard dependencies; use `capabilities.has()` inside collectors for conditional sub-queries

# Job execution model

## MetadataJob

`MetadataJob<A, M>` carries a name, frequency, executor closure, error mode (`Recoverable` or `Fatal`), optional per-job timeout and capability requirements. Jobs execute sequentially within an endpoint, sorted by priority (High first). A Fatal failure stops the batch; Recoverable failures are logged and execution continues.

Per-job timeout defaults to the global `job_timeout` (60s) but can be overridden with `.with_timeout()`.

## MetadataBatch

The output of a poll cycle for one endpoint at one frequency. `MetadataBatch<M>` bundles the frequency, start/end timestamps, per-job `JobReport`s (name, duration, status, optional error), a `had_fatal` flag and the updated metadata struct `M`. Published to Redis as JSON via `MetadataOutputs::publish()`, keyed by `{prefix}{endpoint_uuid}` with the frequency tier as the hash field.

## Error handling

Two error modes:

- `JobErrorMode::Recoverable` (default): error is logged, report records the failure, execution continues to the next job. Most collectors use this.
- `JobErrorMode::Fatal`: error halts the batch. Used sparingly.

`CollectorErrorPolicy` adds a third pattern, Redis-specific: `RecordAndContinue` records errors into `parsing_errors` on the metadata struct instead of failing the job. Partial Redis INFO parsing can succeed even when individual sections fail.

# Per-endpoint specifics

## PostgreSQL

14 collectors (7 High, 5 Medium, 2 Low). Each is a struct with an inherent `sync_metadata` method that picks queries from its `MetadataCollection::request()` map, runs them via `run_single_row()` / `run_query_with_timeout()` and parses results. Many collect core metrics cheaply, then conditionally fetch detailed metrics when problems surface (e.g. bgwriter pulls checkpoint settings only when `health_score < 70`). Builder functions `build_pg_single_job` / `build_pg_vec_job` handle the boilerplate; thin `pg_single!` / `pg_vec!` macros in `jobs()` do only field wiring.

## Redis

11 collectors (standalone `load_*` functions in `parser/`), plus 2 custom inline jobs. Wired to `SyncCollector` via `impl_sync_collector_redis!`. `parsing_errors` on `RedisMetadata` accumulates section-level error messages across the poll cycle, reset at the start of each High-frequency tick. `build_redis_job_with_errors` records success/failure messages into this field, so partial INFO parsing can succeed even when individual sections fail.

## MongoDB

20 collectors, with profiling-aware gating. Many Medium/Low collectors require `profiling_requirement() = Level1` or `Level2`. The `MongoProfilingMode` config controls behavior:

- `Disabled` (default): collectors requiring profiling are filtered out at job-build time (never registered).
- `Level1` / `Level2`: a `mongo.ensure_profiling` setup job is injected at the start of Medium and Low tiers. At runtime, `profiling_allows_execution()` checks the actual profiling level before each collector runs.
- `Dynamic`: collectors are registered (same as Level1) but no setup job is injected; profiling is toggled by the escalation bridge.

All MongoDB metadata fields are `Option<T>` (unlike PG/Redis which use bare types) because collectors can be absent depending on profiling and topology.

## Cassandra

10 collectors (3 High, 6 Medium, 1 Low):

| Tier   | Collectors                                                                                                       |
|--------|------------------------------------------------------------------------------------------------------------------|
| High   | `cluster_info`, `node_info`, `threadpool_info`                                                                   |
| Medium | `compaction_info`, `repair_info`, `tombstone_info`, `keyspace_info`, `table_info`, `snapshot_info`               |
| Low    | `schema_info`                                                                                                    |

Data source is CQL-only with no JMX dependency. Three Medium jobs are gated by capability requirements: `compaction_info` and `repair_info` require `CASSANDRA_HAS_COMPACTION_HISTORY` (hard dependency on `system.compaction_history`), `table_info` requires `CASSANDRA_HAS_SIZE_ESTIMATES` (hard dependency on `system.size_estimates`). Three collectors branch internally on `CASSANDRA_HAS_VIRTUAL_TABLES` to optionally query `system_views` (Cassandra 4.0+): `threadpool_info` adds task counts, `cluster_info` adds client counts, `compaction_info` adds active task details.

**Soft-fail pattern.** Non-critical queries are issued via `run_optional_named_query()` in `stc/utils.rs`, which returns `Option<Value>` rather than propagating errors. Absent system tables produce `None`; the field is left at its default.

**File structure.** `stc/utils.rs` holds shared CQL helpers. Larger collectors are split into subdirectories: `stc/schema/` (mod, types, tests) and `stc/tables/` (mod, types, collector). Smaller collectors remain as single files.

### Future work

- Scylla's driver `get_cluster_data()` can provide node liveness (UP/DOWN) without JMX and is the simplest way to get accurate `node_info.status` on ScyllaDB.
- Per-table read/write ops and latency histograms require JMX or a Jolokia HTTP bridge (no CQL equivalent).
- Wire additional virtual tables: `system_views.caches` (row/key cache hit rates), `system_views.settings` (live configuration values).

## Oracle

15 collectors (7 High, 7 Medium, 1 Low):

| Tier   | Collectors                                                                                                          |
|--------|---------------------------------------------------------------------------------------------------------------------|
| High   | `activity_info`, `connection_info`, `lock_info`, `performance_stats`, `session_info`, `transaction_info`, `wait_events` |
| Medium | `database_stats`, `index_info`, `redolog_info`, `segment_info`, `storage_info`, `table_info`, `tablespace_info`     |
| Low    | `parameter_info`                                                                                                    |

Five Medium jobs require `ORACLE_HAS_DBA_VIEWS` because their SQL exclusively uses `DBA_` prefixed views (`dba_indexes`, `dba_segments`, `dba_tablespaces` and `dba_data_files`) with no `ALL_` fallback. Three High collectors (performance, sessions, database) branch internally on `ORACLE_HAS_DBA_VIEWS` to conditionally skip DBA-dependent sub-queries while still collecting the rest of their metrics.

**Conditional detail collection.** Oracle collectors use a health-threshold pattern: core metrics are always collected, detailed metrics are fetched only when problems surface (e.g. storage details only when tablespaces are in warning/critical state). Helpers `assign_optional_if()` and `should_collect()` in `stc/utils.rs` support this.

**File structure.** Each collector is a subdirectory under `stc/` with `mod.rs`, `models.rs` (or `models/`), `collector.rs` (or `collector/`), `collection.rs` and `methods/`. Shared helpers live in `stc/utils.rs` (query execution, row extraction) and `stc/common.rs` (health evaluation).

## ClickHouse

14 collectors (7 High, 5 Medium, 2 Low):

| Tier   | Collectors                                                                                                            |
|--------|-----------------------------------------------------------------------------------------------------------------------|
| High   | `activity_info`, `connection_info`, `query_info`, `cluster_info`, `replication_info`, `storage_info`, `zookeeper_info` |
| Medium | `merge_info`, `mutation_info`, `part_info`, `database_stats`, `table_info`                                            |
| Low    | `dictionary_info`, `settings_info`                                                                                    |

Four jobs are gated by feature capabilities: `cluster_info` (`HAS_CLUSTERS`), `replication_info` (`HAS_REPLICATION`), `zookeeper_info` (`HAS_ZOOKEEPER`), `dictionary_info` (`HAS_DICTIONARIES`). The cluster collector additionally branches internally on `HAS_REPLICATION` and `HAS_ZOOKEEPER` to skip replication queue and ZooKeeper status sub-queries when those features are absent.

**File structure.** Each collector is a subdirectory under `stc/` split into `core_sync.rs`, `detailed_sync.rs` and `parsers.rs`. Shared helpers in `stc/utils.rs` include `MetadataQueryBatch` (batch query execution with timeout), `OptionalQueryBatch` (for conditional detailed queries) and `RowExt` (typed row extraction).

# Relationship with request-scoped reads

The metadata poller collects schema data using the endpoint's service account
credentials (`read_conn` or `system_conn`), producing the global corpus for
admin views, analytics, synthesis, and annotation enrichment.

Per-user scoped schema enrichment is not included in this distribution. Access
control for user-facing requests should continue to be enforced by endpoint
credentials, IAM grants, and gateway policy.

# Publishing

`MetadataOutputs` writes poll batches to the Eden Redis cache as a hash:

- Key: `metadata:{endpoint_cache_uuid}`
- Field: `high`, `medium` or `low`
- Value: JSON-serialized `MetadataBatch<M>`

On `TypeError` (key exists but isn't a hash), the key is deleted and re-set.

# Serialization

Metadata structs implement both `serde::Serialize`/`Deserialize` and `borsh::BorshSerialize`/`BorshDeserialize`. JSON is used for Redis publishing and API responses. Borsh is used for inter-process communication (the `EpMetadata` trait object uses `linkme::distributed_slice` for type-erased deserialization, keyed by `EpKind`).

# Config reference

All metadata config is constructed from `eden_config::MetadataCollectionConfig`. `MetadataConfig` in `endpoint-types` does not depend on `eden_config` directly.

| Setting                    | Default       | What it controls                                     |
|----------------------------|---------------|------------------------------------------------------|
| `intervals.high`           | 60s           | High-frequency tick interval                         |
| `intervals.medium`         | 1800s (30min) | Medium-frequency tick interval                       |
| `intervals.low`            | 86400s (24h)  | Low-frequency tick interval                          |
| `job_timeout`              | 60s           | Per-job timeout (overridable per-job)                |
| `endpoint_timeout`         | 120s          | Wall-clock cap for all jobs on one endpoint per tick |
| `max_concurrent_endpoints` | 4             | Concurrent endpoint processing per tick              |
| `backoff.base`             | 30s           | Initial backoff on failure                           |
| `backoff.factor`           | 2             | Exponential backoff multiplier                       |
| `backoff.max`              | 900s (15min)  | Maximum backoff duration                             |
| `collector_query_timeout`  | 5s            | Default query timeout for individual SQL/commands    |
| `redis_prefix`             | `metadata:`   | Key prefix for Redis publishing                      |

## MongoDB profiling (`[analytics.mongo]`)

| Setting             | Default    | What it controls                                  |
|---------------------|------------|---------------------------------------------------|
| `profiling`         | `Disabled` | `Disabled`, `Level1`, `Level2` or `Dynamic`       |
| `profiling_slow_ms` | 100        | Slow operation threshold when profiling is enabled |
