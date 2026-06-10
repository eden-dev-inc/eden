//! ClickHouse row types for per-protocol poll metrics tables.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::Serialize;

pub mod tables {
    pub const REDIS_POLL_METRICS: &str = "analytics.redis_poll_metrics";
    pub const POSTGRES_POLL_METRICS: &str = "analytics.postgres_poll_metrics";
    pub const MONGO_POLL_METRICS: &str = "analytics.mongo_poll_metrics";
    pub const ORACLE_POLL_METRICS: &str = "analytics.oracle_poll_metrics";
    pub const CASSANDRA_POLL_METRICS: &str = "analytics.cassandra_poll_metrics";
    pub const CLICKHOUSE_POLL_METRICS: &str = "analytics.clickhouse_poll_metrics";
}

/// Row for `analytics.redis_poll_metrics`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct RedisPollMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub frequency: String,
    pub collection_ms: u32,
    pub had_fatal: u8,

    // Memory (high)
    pub used_memory: Option<u64>,
    pub used_memory_rss: Option<u64>,
    pub used_memory_peak: Option<u64>,
    pub used_memory_overhead: Option<u64>,
    pub used_memory_startup: Option<u64>,
    pub used_memory_dataset: Option<u64>,
    pub total_system_memory: Option<u64>,
    pub maxmemory: Option<u64>,
    pub maxmemory_policy: Option<String>,
    pub mem_fragmentation_ratio: Option<f64>,
    pub mem_fragmentation_bytes: Option<i64>,
    pub allocator_frag_ratio: Option<f64>,
    pub allocator_frag_bytes: Option<i64>,
    pub allocator_rss_ratio: Option<f64>,
    pub allocator_allocated: Option<u64>,
    pub allocator_active: Option<u64>,
    pub allocator_resident: Option<u64>,
    pub used_memory_lua: Option<u64>,
    pub used_memory_scripts: Option<u64>,
    pub used_memory_vm_total: Option<u64>,
    pub mem_clients_normal: Option<u64>,
    pub mem_clients_slaves: Option<u64>,
    pub mem_aof_buffer: Option<u64>,
    pub mem_replication_backlog: Option<u64>,
    pub active_defrag_running: Option<u8>,
    pub lazyfree_pending_objects: Option<u64>,

    // CPU (high)
    pub used_cpu_sys: Option<f64>,
    pub used_cpu_user: Option<f64>,
    pub used_cpu_sys_children: Option<f64>,
    pub used_cpu_user_children: Option<f64>,
    pub used_cpu_sys_main_thread: Option<f64>,
    pub used_cpu_user_main_thread: Option<f64>,

    // Clients (high)
    pub connected_clients: Option<u32>,
    pub blocked_clients: Option<u32>,
    pub maxclients: Option<u32>,
    pub cluster_connections: Option<u32>,
    pub tracking_clients: Option<u32>,
    pub pubsub_clients: Option<u32>,
    pub watching_clients: Option<u32>,
    pub clients_in_timeout_table: Option<u32>,
    pub client_recent_max_input_buffer: Option<u64>,
    pub client_recent_max_output_buffer: Option<u64>,
    pub total_watched_keys: Option<u64>,
    pub total_blocking_keys: Option<u64>,

    // Replication (high)
    pub replication_role: Option<String>,
    pub connected_slaves: Option<u32>,
    pub master_repl_offset: Option<u64>,
    pub repl_backlog_active: Option<u8>,
    pub repl_backlog_size: Option<u64>,
    pub repl_backlog_histlen: Option<u64>,
    pub master_link_status: Option<String>,
    pub master_sync_in_progress: Option<u8>,
    pub slave_repl_offset: Option<u64>,
    pub master_link_down_since_seconds: Option<u64>,

    // Database (high, derived)
    pub total_keys: Option<u64>,
    pub total_expires: Option<u64>,
    pub database_count: Option<u32>,

    // JSON blobs (high)
    pub client_details_json: String,
    pub slave_replicas_json: String,
    pub database_stats_json: String,

    // Cluster (medium)
    pub cluster_enabled: Option<u8>,
    pub cluster_state: Option<String>,
    pub cluster_known_nodes: Option<u32>,
    pub cluster_size: Option<u32>,
    pub cluster_info_json: String,

    // Persistence (medium)
    pub rdb_last_save_time: Option<u64>,
    pub rdb_changes_since_last_save: Option<u64>,
    pub aof_enabled: Option<u8>,
    pub aof_rewrite_in_progress: Option<u8>,
    pub persistence_info_json: String,

    // Modules (medium)
    pub modules_info_json: String,

    // Server (low)
    pub redis_version: Option<String>,
    pub redis_mode: Option<String>,
    pub os: Option<String>,
    pub uptime_in_seconds: Option<u64>,
    pub hz: Option<u32>,

    // Config (low)
    pub config_json: String,

    // Security (low)
    pub security_info_json: String,
}

impl RedisPollMetricsRow {
    pub fn common(
        snapshot_time: DateTime<Utc>,
        organization_uuid: String,
        endpoint_uuid: String,
        frequency: String,
        collection_ms: u32,
        had_fatal: bool,
    ) -> Self {
        Self {
            snapshot_time,
            organization_uuid,
            endpoint_uuid,
            frequency,
            collection_ms,
            had_fatal: u8::from(had_fatal),
            used_memory: None,
            used_memory_rss: None,
            used_memory_peak: None,
            used_memory_overhead: None,
            used_memory_startup: None,
            used_memory_dataset: None,
            total_system_memory: None,
            maxmemory: None,
            maxmemory_policy: None,
            mem_fragmentation_ratio: None,
            mem_fragmentation_bytes: None,
            allocator_frag_ratio: None,
            allocator_frag_bytes: None,
            allocator_rss_ratio: None,
            allocator_allocated: None,
            allocator_active: None,
            allocator_resident: None,
            used_memory_lua: None,
            used_memory_scripts: None,
            used_memory_vm_total: None,
            mem_clients_normal: None,
            mem_clients_slaves: None,
            mem_aof_buffer: None,
            mem_replication_backlog: None,
            active_defrag_running: None,
            lazyfree_pending_objects: None,
            used_cpu_sys: None,
            used_cpu_user: None,
            used_cpu_sys_children: None,
            used_cpu_user_children: None,
            used_cpu_sys_main_thread: None,
            used_cpu_user_main_thread: None,
            connected_clients: None,
            blocked_clients: None,
            maxclients: None,
            cluster_connections: None,
            tracking_clients: None,
            pubsub_clients: None,
            watching_clients: None,
            clients_in_timeout_table: None,
            client_recent_max_input_buffer: None,
            client_recent_max_output_buffer: None,
            total_watched_keys: None,
            total_blocking_keys: None,
            replication_role: None,
            connected_slaves: None,
            master_repl_offset: None,
            repl_backlog_active: None,
            repl_backlog_size: None,
            repl_backlog_histlen: None,
            master_link_status: None,
            master_sync_in_progress: None,
            slave_repl_offset: None,
            master_link_down_since_seconds: None,
            total_keys: None,
            total_expires: None,
            database_count: None,
            client_details_json: "[]".to_string(),
            slave_replicas_json: "[]".to_string(),
            database_stats_json: "[]".to_string(),
            cluster_enabled: None,
            cluster_state: None,
            cluster_known_nodes: None,
            cluster_size: None,
            cluster_info_json: "{}".to_string(),
            rdb_last_save_time: None,
            rdb_changes_since_last_save: None,
            aof_enabled: None,
            aof_rewrite_in_progress: None,
            persistence_info_json: "{}".to_string(),
            modules_info_json: "{}".to_string(),
            redis_version: None,
            redis_mode: None,
            os: None,
            uptime_in_seconds: None,
            hz: None,
            config_json: "{}".to_string(),
            security_info_json: "{}".to_string(),
        }
    }
}

/// Row for `analytics.postgres_poll_metrics`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct PostgresPollMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub frequency: String,
    pub collection_ms: u32,
    pub had_fatal: u8,

    // Activity (high)
    pub active_connections: Option<u64>,
    pub idle_connections: Option<u64>,
    pub idle_in_transaction: Option<u64>,
    pub total_connections: Option<u64>,
    pub max_connections: Option<u64>,
    pub connection_utilization_pct: Option<f64>,
    pub waiting_queries_count: Option<u64>,
    pub blocking_queries_count: Option<u64>,

    // Locks (high)
    pub total_locks: Option<u64>,
    pub granted_locks: Option<u64>,
    pub waiting_locks: Option<u64>,
    pub deadlock_count: Option<u64>,
    pub max_lock_wait_time: Option<f64>,

    // Performance (high)
    pub buffer_cache_hit_ratio: Option<f64>,
    pub index_hit_ratio: Option<f64>,
    pub total_operations: Option<u64>,
    pub total_transactions: Option<u64>,
    pub total_blocks_read: Option<u64>,
    pub total_blocks_hit: Option<u64>,
    pub total_temp_files: Option<u64>,
    pub total_temp_bytes: Option<u64>,

    // Replication (high)
    pub is_primary: Option<u8>,
    pub is_in_recovery: Option<u8>,
    pub active_replicas: Option<u64>,
    pub max_replica_lag_seconds: Option<f64>,
    pub synchronous_replicas: Option<u64>,

    // Transactions (high)
    pub xact_committed: Option<u64>,
    pub xact_rolled_back: Option<u64>,
    pub commit_ratio: Option<f64>,
    pub deadlocks_total: Option<u64>,

    // WAL (high)
    pub wal_bytes: Option<u64>,
    pub wal_records: Option<u64>,
    pub wal_fpi: Option<u64>,

    // JSON blobs (high)
    pub activity_info_json: String,
    pub lock_info_json: String,

    // BGWriter (medium)
    pub buffers_checkpoint: Option<u64>,
    pub buffers_clean: Option<u64>,
    pub buffers_backend: Option<u64>,

    // JSON (medium)
    pub database_stats_json: String,
    pub table_info_json: String,
    pub index_info_json: String,
    pub vacuum_info_json: String,

    // JSON (low)
    pub extensions_json: String,
    pub settings_json: String,
}

impl PostgresPollMetricsRow {
    pub fn common(
        snapshot_time: DateTime<Utc>,
        organization_uuid: String,
        endpoint_uuid: String,
        frequency: String,
        collection_ms: u32,
        had_fatal: bool,
    ) -> Self {
        Self {
            snapshot_time,
            organization_uuid,
            endpoint_uuid,
            frequency,
            collection_ms,
            had_fatal: u8::from(had_fatal),
            active_connections: None,
            idle_connections: None,
            idle_in_transaction: None,
            total_connections: None,
            max_connections: None,
            connection_utilization_pct: None,
            waiting_queries_count: None,
            blocking_queries_count: None,
            total_locks: None,
            granted_locks: None,
            waiting_locks: None,
            deadlock_count: None,
            max_lock_wait_time: None,
            buffer_cache_hit_ratio: None,
            index_hit_ratio: None,
            total_operations: None,
            total_transactions: None,
            total_blocks_read: None,
            total_blocks_hit: None,
            total_temp_files: None,
            total_temp_bytes: None,
            is_primary: None,
            is_in_recovery: None,
            active_replicas: None,
            max_replica_lag_seconds: None,
            synchronous_replicas: None,
            xact_committed: None,
            xact_rolled_back: None,
            commit_ratio: None,
            deadlocks_total: None,
            wal_bytes: None,
            wal_records: None,
            wal_fpi: None,
            activity_info_json: "{}".to_string(),
            lock_info_json: "{}".to_string(),
            buffers_checkpoint: None,
            buffers_clean: None,
            buffers_backend: None,
            database_stats_json: "[]".to_string(),
            table_info_json: "{}".to_string(),
            index_info_json: "{}".to_string(),
            vacuum_info_json: "{}".to_string(),
            extensions_json: "[]".to_string(),
            settings_json: "{}".to_string(),
        }
    }
}

/// Row for `analytics.mongo_poll_metrics`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct MongoPollMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub frequency: String,
    pub collection_ms: u32,
    pub had_fatal: u8,

    // Connections (high)
    pub current_connections: Option<u64>,
    pub available_connections: Option<u64>,
    pub total_created: Option<u64>,

    // Locks (high)
    pub current_queue_total: Option<u64>,
    pub current_queue_readers: Option<u64>,
    pub current_queue_writers: Option<u64>,
    pub deadlocks_detected: Option<u64>,
    pub lock_contention_ratio: Option<f64>,

    // Network (high)
    pub bytes_in: Option<u64>,
    pub bytes_out: Option<u64>,
    pub num_requests: Option<f64>,

    // Performance (high)
    pub overall_performance_score: Option<f64>,

    // WiredTiger (high)
    pub cache_bytes_currently_in_cache: Option<u64>,
    pub cache_maximum_bytes_configured: Option<u64>,
    pub cache_evictions: Option<u64>,
    pub pages_read_into_cache: Option<u64>,
    pub pages_written_from_cache: Option<u64>,
    pub cache_hit_ratio: Option<f64>,

    // Replication (high)
    pub is_primary: Option<u8>,
    pub replication_lag_ms: Option<f64>,
    pub member_count: Option<u32>,

    // Transactions (high)
    pub total_started: Option<u64>,
    pub total_committed: Option<u64>,
    pub total_aborted: Option<u64>,

    // JSON blobs (high)
    pub server_info_json: String,
    pub replication_info_json: String,
    pub wiredtiger_info_json: String,

    // Oplog (medium)
    pub oplog_size_mb: Option<f64>,
    pub oplog_used_mb: Option<f64>,

    // JSON (medium)
    pub aggregation_stats_json: String,
    pub collection_info_json: String,
    pub database_stats_json: String,
    pub index_info_json: String,
    pub profiler_info_json: String,
    pub sharding_info_json: String,

    // JSON (low)
    pub balancer_info_json: String,
    pub memory_info_json: String,
    pub security_info_json: String,
    pub user_info_json: String,
}

impl MongoPollMetricsRow {
    pub fn common(
        snapshot_time: DateTime<Utc>,
        organization_uuid: String,
        endpoint_uuid: String,
        frequency: String,
        collection_ms: u32,
        had_fatal: bool,
    ) -> Self {
        Self {
            snapshot_time,
            organization_uuid,
            endpoint_uuid,
            frequency,
            collection_ms,
            had_fatal: u8::from(had_fatal),
            current_connections: None,
            available_connections: None,
            total_created: None,
            current_queue_total: None,
            current_queue_readers: None,
            current_queue_writers: None,
            deadlocks_detected: None,
            lock_contention_ratio: None,
            bytes_in: None,
            bytes_out: None,
            num_requests: None,
            overall_performance_score: None,
            cache_bytes_currently_in_cache: None,
            cache_maximum_bytes_configured: None,
            cache_evictions: None,
            pages_read_into_cache: None,
            pages_written_from_cache: None,
            cache_hit_ratio: None,
            is_primary: None,
            replication_lag_ms: None,
            member_count: None,
            total_started: None,
            total_committed: None,
            total_aborted: None,
            server_info_json: "{}".to_string(),
            replication_info_json: "{}".to_string(),
            wiredtiger_info_json: "{}".to_string(),
            oplog_size_mb: None,
            oplog_used_mb: None,
            aggregation_stats_json: "{}".to_string(),
            collection_info_json: "[]".to_string(),
            database_stats_json: "[]".to_string(),
            index_info_json: "[]".to_string(),
            profiler_info_json: "{}".to_string(),
            sharding_info_json: "{}".to_string(),
            balancer_info_json: "{}".to_string(),
            memory_info_json: "{}".to_string(),
            security_info_json: "{}".to_string(),
            user_info_json: "[]".to_string(),
        }
    }
}

/// Row for `analytics.oracle_poll_metrics`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct OraclePollMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub frequency: String,
    pub collection_ms: u32,
    pub had_fatal: u8,

    // Activity / Connection / Session (high)
    pub active_sessions: Option<u64>,
    pub total_sessions: Option<u64>,
    pub max_sessions: Option<u64>,
    pub session_utilization_pct: Option<f64>,
    pub waiting_sessions_count: Option<u64>,
    pub blocking_sessions_count: Option<u64>,
    pub current_processes: Option<u64>,
    pub max_processes: Option<u64>,
    pub process_utilization_pct: Option<f64>,
    pub sga_size: Option<u64>,
    pub current_pga_used: Option<u64>,

    // Locks (high)
    pub total_active_locks: Option<u64>,
    pub blocking_locks: Option<u64>,
    pub blocked_sessions: Option<u64>,
    pub total_deadlocks: Option<u64>,
    pub max_lock_wait_time: Option<f64>,

    // Performance (high)
    pub health_score: Option<f64>,

    // Transactions (high)
    pub active_transactions: Option<u64>,
    pub user_commits: Option<u64>,
    pub user_rollbacks: Option<u64>,
    pub rollback_ratio: Option<f64>,
    pub transaction_health_score: Option<f64>,

    // Wait events (high)
    pub cpu_time_percent: Option<f64>,
    pub wait_time_percent: Option<f64>,
    pub wait_health_score: Option<f64>,

    // JSON blobs (high)
    pub activity_info_json: String,
    pub connection_info_json: String,
    pub lock_info_json: String,
    pub performance_stats_json: String,
    pub session_info_json: String,
    pub transaction_info_json: String,
    pub wait_events_json: String,

    // Database stats scalars (medium, first element)
    pub buffer_cache_hit_ratio: Option<f64>,
    pub transactions_per_sec: Option<f64>,
    pub physical_reads_per_sec: Option<f64>,
    pub database_size: Option<u64>,
    pub used_space: Option<u64>,
    pub uptime_seconds: Option<f64>,

    // JSON blobs (medium)
    pub database_stats_json: String,
    pub index_info_json: String,
    pub redolog_info_json: String,
    pub segment_info_json: String,
    pub storage_info_json: String,
    pub table_info_json: String,
    pub tablespace_info_json: String,

    // JSON blobs (low)
    pub parameter_info_json: String,
}

impl OraclePollMetricsRow {
    pub fn common(
        snapshot_time: DateTime<Utc>,
        organization_uuid: String,
        endpoint_uuid: String,
        frequency: String,
        collection_ms: u32,
        had_fatal: bool,
    ) -> Self {
        Self {
            snapshot_time,
            organization_uuid,
            endpoint_uuid,
            frequency,
            collection_ms,
            had_fatal: u8::from(had_fatal),
            active_sessions: None,
            total_sessions: None,
            max_sessions: None,
            session_utilization_pct: None,
            waiting_sessions_count: None,
            blocking_sessions_count: None,
            current_processes: None,
            max_processes: None,
            process_utilization_pct: None,
            sga_size: None,
            current_pga_used: None,
            total_active_locks: None,
            blocking_locks: None,
            blocked_sessions: None,
            total_deadlocks: None,
            max_lock_wait_time: None,
            health_score: None,
            active_transactions: None,
            user_commits: None,
            user_rollbacks: None,
            rollback_ratio: None,
            transaction_health_score: None,
            cpu_time_percent: None,
            wait_time_percent: None,
            wait_health_score: None,
            activity_info_json: "{}".to_string(),
            connection_info_json: "{}".to_string(),
            lock_info_json: "{}".to_string(),
            performance_stats_json: "{}".to_string(),
            session_info_json: "{}".to_string(),
            transaction_info_json: "{}".to_string(),
            wait_events_json: "{}".to_string(),
            buffer_cache_hit_ratio: None,
            transactions_per_sec: None,
            physical_reads_per_sec: None,
            database_size: None,
            used_space: None,
            uptime_seconds: None,
            database_stats_json: "[]".to_string(),
            index_info_json: "[]".to_string(),
            redolog_info_json: "{}".to_string(),
            segment_info_json: "[]".to_string(),
            storage_info_json: "{}".to_string(),
            table_info_json: "[]".to_string(),
            tablespace_info_json: "[]".to_string(),
            parameter_info_json: "{}".to_string(),
        }
    }
}

/// Row for `analytics.cassandra_poll_metrics`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct CassandraPollMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub frequency: String,
    pub collection_ms: u32,
    pub had_fatal: u8,

    // Cluster (high)
    pub total_nodes: Option<u64>,
    pub up_nodes: Option<u64>,
    pub down_nodes: Option<u64>,
    pub cluster_health_pct: Option<f64>,
    pub schema_agreement: Option<u8>,
    pub total_client_connections: Option<u64>,
    pub pending_compactions: Option<u64>,
    pub active_repairs: Option<u64>,

    // Node resource metrics (high, first node)
    pub heap_memory_used_mb: Option<f64>,
    pub heap_memory_max_mb: Option<f64>,
    pub heap_memory_utilization_pct: Option<f64>,
    pub cpu_utilization_pct: Option<f64>,
    pub disk_used_gb: Option<f64>,
    pub disk_utilization_pct: Option<f64>,

    // Node performance metrics (high, first node)
    pub read_requests_per_sec: Option<f64>,
    pub write_requests_per_sec: Option<f64>,
    pub avg_read_latency_ms: Option<f64>,
    pub avg_write_latency_ms: Option<f64>,
    pub cache_hit_ratio_pct: Option<f64>,

    // Thread pools (high)
    pub threadpool_active_threads: Option<u64>,
    pub threadpool_pending_tasks: Option<u64>,
    pub threadpool_dropped_tasks: Option<u64>,
    pub threadpool_health_score: Option<f64>,

    // JSON blobs (high)
    pub cluster_info_json: String,
    pub node_info_json: String,
    pub threadpool_info_json: String,

    // Compaction (medium)
    pub compaction_pending: Option<u64>,
    pub compaction_active: Option<u64>,
    pub compaction_rate_mb_per_sec: Option<f64>,

    // Repair (medium)
    pub repair_success_rate_pct: Option<f64>,
    pub keyspaces_needing_repair: Option<u64>,

    // Tombstone (medium)
    pub tombstone_health_score: Option<f64>,
    pub high_tombstone_ratio_tables: Option<u64>,

    // JSON blobs (medium)
    pub compaction_info_json: String,
    pub repair_info_json: String,
    pub tombstone_info_json: String,
    pub keyspace_info_json: String,
    pub table_info_json: String,
    pub snapshot_info_json: String,

    // JSON blobs (low)
    pub schema_info_json: String,
}

impl CassandraPollMetricsRow {
    pub fn common(
        snapshot_time: DateTime<Utc>,
        organization_uuid: String,
        endpoint_uuid: String,
        frequency: String,
        collection_ms: u32,
        had_fatal: bool,
    ) -> Self {
        Self {
            snapshot_time,
            organization_uuid,
            endpoint_uuid,
            frequency,
            collection_ms,
            had_fatal: u8::from(had_fatal),
            total_nodes: None,
            up_nodes: None,
            down_nodes: None,
            cluster_health_pct: None,
            schema_agreement: None,
            total_client_connections: None,
            pending_compactions: None,
            active_repairs: None,
            heap_memory_used_mb: None,
            heap_memory_max_mb: None,
            heap_memory_utilization_pct: None,
            cpu_utilization_pct: None,
            disk_used_gb: None,
            disk_utilization_pct: None,
            read_requests_per_sec: None,
            write_requests_per_sec: None,
            avg_read_latency_ms: None,
            avg_write_latency_ms: None,
            cache_hit_ratio_pct: None,
            threadpool_active_threads: None,
            threadpool_pending_tasks: None,
            threadpool_dropped_tasks: None,
            threadpool_health_score: None,
            cluster_info_json: "{}".to_string(),
            node_info_json: "[]".to_string(),
            threadpool_info_json: "{}".to_string(),
            compaction_pending: None,
            compaction_active: None,
            compaction_rate_mb_per_sec: None,
            repair_success_rate_pct: None,
            keyspaces_needing_repair: None,
            tombstone_health_score: None,
            high_tombstone_ratio_tables: None,
            compaction_info_json: "{}".to_string(),
            repair_info_json: "{}".to_string(),
            tombstone_info_json: "{}".to_string(),
            keyspace_info_json: "[]".to_string(),
            table_info_json: "[]".to_string(),
            snapshot_info_json: "{}".to_string(),
            schema_info_json: "{}".to_string(),
        }
    }
}

/// Row for `analytics.clickhouse_poll_metrics`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct ClickhousePollMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub frequency: String,
    pub collection_ms: u32,
    pub had_fatal: u8,

    // Activity (high)
    pub running_queries: Option<u64>,
    pub queued_queries: Option<u64>,
    pub longest_query_duration: Option<f64>,
    pub queries_per_second: Option<f64>,
    pub query_memory_usage: Option<u64>,

    // Connections (high)
    pub total_connections: Option<u64>,
    pub max_connections: Option<u64>,
    pub connection_utilization_pct: Option<f64>,
    pub active_users_count: Option<u64>,

    // Queries (high)
    pub slow_queries: Option<u64>,
    pub high_memory_queries: Option<u64>,
    pub avg_query_execution_time: Option<f64>,
    pub total_bytes_read: Option<u64>,
    pub total_rows_processed: Option<u64>,

    // Cluster (high)
    pub cluster_health_pct: Option<f64>,
    pub total_shards: Option<u64>,
    pub total_replicas: Option<u64>,

    // Replication (high)
    pub avg_replication_lag: Option<f64>,
    pub max_replication_lag: Option<f64>,
    pub lagging_tables: Option<u64>,
    pub readonly_tables: Option<u64>,
    pub total_queue_size: Option<u64>,

    // Storage (high)
    pub total_disk_usage: Option<u64>,
    pub total_rows_stored: Option<u64>,
    pub avg_compression_ratio: Option<f64>,
    pub fragmented_tables: Option<u64>,
    pub reclaimable_space: Option<u64>,

    // ZooKeeper (high)
    pub zk_active_connections: Option<u64>,
    pub zk_max_replication_lag_seconds: Option<f64>,
    pub zk_detached_replicas: Option<u64>,
    pub zk_readonly_replicas: Option<u64>,

    // JSON blobs (high)
    pub activity_info_json: String,
    pub connection_info_json: String,
    pub query_info_json: String,
    pub cluster_info_json: String,
    pub replication_info_json: String,
    pub storage_info_json: String,
    pub zookeeper_info_json: String,

    // Merges (medium)
    pub running_merges: Option<u64>,
    pub queued_merges: Option<u64>,
    pub avg_merge_throughput: Option<f64>,

    // Mutations (medium)
    pub active_mutations: Option<u64>,
    pub failed_mutations: Option<u64>,
    pub stuck_mutations: Option<u64>,

    // JSON blobs (medium)
    pub merge_info_json: String,
    pub mutation_info_json: String,
    pub part_info_json: String,
    pub database_stats_json: String,
    pub table_info_json: String,

    // JSON blobs (low)
    pub dictionary_info_json: String,
    pub settings_info_json: String,
}

impl ClickhousePollMetricsRow {
    pub fn common(
        snapshot_time: DateTime<Utc>,
        organization_uuid: String,
        endpoint_uuid: String,
        frequency: String,
        collection_ms: u32,
        had_fatal: bool,
    ) -> Self {
        Self {
            snapshot_time,
            organization_uuid,
            endpoint_uuid,
            frequency,
            collection_ms,
            had_fatal: u8::from(had_fatal),
            running_queries: None,
            queued_queries: None,
            longest_query_duration: None,
            queries_per_second: None,
            query_memory_usage: None,
            total_connections: None,
            max_connections: None,
            connection_utilization_pct: None,
            active_users_count: None,
            slow_queries: None,
            high_memory_queries: None,
            avg_query_execution_time: None,
            total_bytes_read: None,
            total_rows_processed: None,
            cluster_health_pct: None,
            total_shards: None,
            total_replicas: None,
            avg_replication_lag: None,
            max_replication_lag: None,
            lagging_tables: None,
            readonly_tables: None,
            total_queue_size: None,
            total_disk_usage: None,
            total_rows_stored: None,
            avg_compression_ratio: None,
            fragmented_tables: None,
            reclaimable_space: None,
            zk_active_connections: None,
            zk_max_replication_lag_seconds: None,
            zk_detached_replicas: None,
            zk_readonly_replicas: None,
            activity_info_json: "{}".to_string(),
            connection_info_json: "{}".to_string(),
            query_info_json: "{}".to_string(),
            cluster_info_json: "{}".to_string(),
            replication_info_json: "{}".to_string(),
            storage_info_json: "{}".to_string(),
            zookeeper_info_json: "{}".to_string(),
            running_merges: None,
            queued_merges: None,
            avg_merge_throughput: None,
            active_mutations: None,
            failed_mutations: None,
            stuck_mutations: None,
            merge_info_json: "{}".to_string(),
            mutation_info_json: "{}".to_string(),
            part_info_json: "[]".to_string(),
            database_stats_json: "[]".to_string(),
            table_info_json: "[]".to_string(),
            dictionary_info_json: "[]".to_string(),
            settings_info_json: "{}".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_common_defaults() {
        let row = RedisPollMetricsRow::common(Utc::now(), "t1".into(), "e1".into(), "high".into(), 42, false);
        assert_eq!(row.organization_uuid, "t1");
        assert_eq!(row.frequency, "high");
        assert_eq!(row.had_fatal, 0);
        assert!(row.used_memory.is_none());
        assert_eq!(row.client_details_json, "[]");
    }

    #[test]
    fn postgres_common_defaults() {
        let row = PostgresPollMetricsRow::common(Utc::now(), "t1".into(), "e1".into(), "medium".into(), 100, true);
        assert_eq!(row.had_fatal, 1);
        assert!(row.active_connections.is_none());
        assert_eq!(row.database_stats_json, "[]");
    }

    #[test]
    fn mongo_common_defaults() {
        let row = MongoPollMetricsRow::common(Utc::now(), "t1".into(), "e1".into(), "low".into(), 200, false);
        assert_eq!(row.frequency, "low");
        assert!(row.current_connections.is_none());
        assert_eq!(row.user_info_json, "[]");
    }

    #[test]
    fn oracle_common_defaults() {
        let row = OraclePollMetricsRow::common(Utc::now(), "t1".into(), "e1".into(), "high".into(), 50, false);
        assert_eq!(row.organization_uuid, "t1");
        assert_eq!(row.had_fatal, 0);
        assert!(row.active_sessions.is_none());
        assert_eq!(row.activity_info_json, "{}");
        assert_eq!(row.database_stats_json, "[]");
    }

    #[test]
    fn cassandra_common_defaults() {
        let row = CassandraPollMetricsRow::common(Utc::now(), "t1".into(), "e1".into(), "medium".into(), 80, true);
        assert_eq!(row.had_fatal, 1);
        assert!(row.total_nodes.is_none());
        assert_eq!(row.cluster_info_json, "{}");
        assert_eq!(row.node_info_json, "[]");
    }

    #[test]
    fn clickhouse_common_defaults() {
        let row = ClickhousePollMetricsRow::common(Utc::now(), "t1".into(), "e1".into(), "low".into(), 150, false);
        assert_eq!(row.frequency, "low");
        assert!(row.running_queries.is_none());
        assert_eq!(row.activity_info_json, "{}");
        assert_eq!(row.part_info_json, "[]");
    }
}
