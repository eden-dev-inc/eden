-- Redis poll metrics from metadata collection
-- One row per endpoint per frequency tier per collection cycle

CREATE TABLE IF NOT EXISTS analytics.redis_poll_metrics
(
    -- Common
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    frequency LowCardinality(String),
    collection_ms UInt32,
    had_fatal UInt8 DEFAULT 0,

    -- Memory (high)
    used_memory Nullable(UInt64),
    used_memory_rss Nullable(UInt64),
    used_memory_peak Nullable(UInt64),
    used_memory_overhead Nullable(UInt64),
    used_memory_startup Nullable(UInt64),
    used_memory_dataset Nullable(UInt64),
    total_system_memory Nullable(UInt64),
    maxmemory Nullable(UInt64),
    maxmemory_policy Nullable(String),
    mem_fragmentation_ratio Nullable(Float64),
    mem_fragmentation_bytes Nullable(Int64),
    allocator_frag_ratio Nullable(Float64),
    allocator_frag_bytes Nullable(Int64),
    allocator_rss_ratio Nullable(Float64),
    allocator_allocated Nullable(UInt64),
    allocator_active Nullable(UInt64),
    allocator_resident Nullable(UInt64),
    used_memory_lua Nullable(UInt64),
    used_memory_scripts Nullable(UInt64),
    used_memory_vm_total Nullable(UInt64),
    mem_clients_normal Nullable(UInt64),
    mem_clients_slaves Nullable(UInt64),
    mem_aof_buffer Nullable(UInt64),
    mem_replication_backlog Nullable(UInt64),
    active_defrag_running Nullable(UInt8),
    lazyfree_pending_objects Nullable(UInt64),

    -- CPU (high)
    used_cpu_sys Nullable(Float64),
    used_cpu_user Nullable(Float64),
    used_cpu_sys_children Nullable(Float64),
    used_cpu_user_children Nullable(Float64),
    used_cpu_sys_main_thread Nullable(Float64),
    used_cpu_user_main_thread Nullable(Float64),

    -- Clients (high)
    connected_clients Nullable(UInt32),
    blocked_clients Nullable(UInt32),
    maxclients Nullable(UInt32),
    cluster_connections Nullable(UInt32),
    tracking_clients Nullable(UInt32),
    pubsub_clients Nullable(UInt32),
    watching_clients Nullable(UInt32),
    clients_in_timeout_table Nullable(UInt32),
    client_recent_max_input_buffer Nullable(UInt64),
    client_recent_max_output_buffer Nullable(UInt64),
    total_watched_keys Nullable(UInt64),
    total_blocking_keys Nullable(UInt64),

    -- Replication (high)
    replication_role Nullable(String),
    connected_slaves Nullable(UInt32),
    master_repl_offset Nullable(UInt64),
    repl_backlog_active Nullable(UInt8),
    repl_backlog_size Nullable(UInt64),
    repl_backlog_histlen Nullable(UInt64),
    master_link_status Nullable(String),
    master_sync_in_progress Nullable(UInt8),
    slave_repl_offset Nullable(UInt64),
    master_link_down_since_seconds Nullable(UInt64),

    -- Database (high, derived)
    total_keys Nullable(UInt64),
    total_expires Nullable(UInt64),
    database_count Nullable(UInt32),

    -- JSON blobs (high)
    client_details_json String DEFAULT '[]' CODEC(ZSTD(3)),
    slave_replicas_json String DEFAULT '[]' CODEC(ZSTD(3)),
    database_stats_json String DEFAULT '[]' CODEC(ZSTD(3)),

    -- Cluster (medium)
    cluster_enabled Nullable(UInt8),
    cluster_state Nullable(String),
    cluster_known_nodes Nullable(UInt32),
    cluster_size Nullable(UInt32),
    cluster_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Persistence (medium)
    rdb_last_save_time Nullable(UInt64),
    rdb_changes_since_last_save Nullable(UInt64),
    aof_enabled Nullable(UInt8),
    aof_rewrite_in_progress Nullable(UInt8),
    persistence_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Modules (medium)
    modules_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Server (low)
    redis_version Nullable(String),
    redis_mode Nullable(String),
    os Nullable(String),
    uptime_in_seconds Nullable(UInt64),
    hz Nullable(UInt32),

    -- Config (low)
    config_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Security (low)
    security_info_json String DEFAULT '{}' CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, frequency, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
