use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

/// Redis memory usage and allocation information
///
/// This struct contains comprehensive memory metrics from the Redis server,
/// including allocation details, fragmentation ratios, and script memory usage.
/// Data is collected from the "Memory" section of Redis INFO command.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub struct RedisMemoryInfo {
    /// Total number of bytes allocated by Redis using its allocator
    pub used_memory: u64,
    /// Human readable representation of used_memory
    pub used_memory_human: String,
    /// Number of bytes that Redis allocated as seen by the OS (resident set size)
    pub used_memory_rss: u64,
    /// Human readable representation of used_memory_rss
    pub used_memory_rss_human: String,
    /// Peak memory consumed by Redis (in bytes)
    pub used_memory_peak: u64,
    /// Human readable representation of used_memory_peak
    pub used_memory_peak_human: String,
    /// The percentage of used_memory out of used_memory_peak
    pub used_memory_peak_perc: String,
    /// Sum in bytes of all overheads for managing internal data structures
    pub used_memory_overhead: u64,
    /// Initial amount of memory consumed by Redis at startup in bytes
    pub used_memory_startup: u64,
    /// Size in bytes of the dataset (used_memory_overhead subtracted from used_memory)
    pub used_memory_dataset: u64,
    /// Percentage of used_memory_dataset out of net memory usage
    pub used_memory_dataset_perc: String,
    /// Total amount of memory that the Redis host has
    pub total_system_memory: u64,
    /// Human readable representation of total_system_memory
    pub total_system_memory_human: String,

    // Script and VM memory metrics (Redis 7.0+ evolution)
    /// Number of bytes used by the Lua engine (deprecated in Redis 7.0)
    pub used_memory_lua: u64,
    /// Number of bytes used by script VM engines for EVAL framework (Redis 7.0+)
    pub used_memory_vm_eval: u64,
    /// Human readable representation of used_memory_lua (deprecated in Redis 7.0)
    pub used_memory_lua_human: String,
    /// Number of bytes overhead by EVAL scripts (Redis 7.0+)
    pub used_memory_scripts_eval: u64,
    /// Number of EVAL scripts cached by the server (Redis 7.0+)
    pub number_of_cached_scripts: u32,
    /// Number of functions (Redis 7.0+)
    pub number_of_functions: u32,
    /// Number of libraries (Redis 7.0+)
    pub number_of_libraries: u32,
    /// Number of bytes used by script VM engines for Functions framework (Redis 7.0+)
    pub used_memory_vm_functions: u64,
    /// Total VM memory: used_memory_vm_eval + used_memory_vm_functions (Redis 7.0+)
    pub used_memory_vm_total: u64,
    /// Human readable representation of used_memory_vm_total
    pub used_memory_vm_total_human: String,
    /// Number of bytes overhead by Function scripts (Redis 7.0+)
    pub used_memory_functions: u64,
    /// Total script memory: used_memory_scripts_eval + used_memory_functions (Redis 7.0+)
    pub used_memory_scripts: u64,
    /// Human readable representation of used_memory_scripts
    pub used_memory_scripts_human: String,

    // Configuration and policy
    /// Value of the maxmemory configuration directive
    pub maxmemory: u64,
    /// Human readable representation of maxmemory
    pub maxmemory_human: String,
    /// Value of the maxmemory-policy configuration directive
    pub maxmemory_policy: String,

    // Fragmentation and allocator metrics
    /// Ratio between used_memory_rss and used_memory
    pub mem_fragmentation_ratio: f64,
    /// Delta between used_memory_rss and used_memory
    pub mem_fragmentation_bytes: i64,
    /// True external fragmentation ratio between allocator_active and allocator_allocated
    pub allocator_frag_ratio: f64,
    /// Delta between allocator_active and allocator_allocated
    pub allocator_frag_bytes: i64,
    /// Ratio between allocator_resident and allocator_active
    pub allocator_rss_ratio: f64,
    /// Delta between allocator_resident and allocator_active
    pub allocator_rss_bytes: i64,
    /// Ratio between used_memory_rss and allocator_resident
    pub rss_overhead_ratio: f64,
    /// Delta between used_memory_rss and allocator_resident
    pub rss_overhead_bytes: i64,
    /// Total bytes allocated from the allocator, including internal fragmentation
    pub allocator_allocated: u64,
    /// Total bytes in allocator active pages, includes external fragmentation
    pub allocator_active: u64,
    /// Total bytes resident (RSS) in the allocator
    pub allocator_resident: u64,
    /// Total bytes of 'muzzy' memory (RSS) in the allocator
    pub allocator_muzzy: u64,

    // Client and buffer memory
    /// Used memory that's not counted for key eviction (replica and AOF buffers)
    pub mem_not_counted_for_evict: u64,
    /// Memory used by replica clients (Redis 7.0+ may show 0 due to shared buffers)
    pub mem_clients_slaves: u64,
    /// Memory used by normal clients
    pub mem_clients_normal: u64,
    /// Memory used by links to peers on the cluster bus (cluster mode)
    pub mem_cluster_links: u64,
    /// Transient memory used for AOF and AOF rewrite buffers
    pub mem_aof_buffer: u64,
    /// Memory used by replication backlog
    pub mem_replication_backlog: u64,
    /// Total memory consumed for replication buffers (Redis 7.0+)
    pub mem_total_replication_buffers: u64,

    // System and defragmentation
    /// Memory allocator, chosen at compile time
    pub mem_allocator: String,
    /// Temporary memory overhead of database dictionaries being rehashed (Redis 7.4+)
    pub mem_overhead_db_hashtable_rehashing: u64,
    /// Whether defragmentation is active and CPU percentage it intends to use
    pub active_defrag_running: bool,
    /// Number of objects waiting to be freed (UNLINK, FLUSHDB/FLUSHALL ASYNC)
    pub lazyfree_pending_objects: u64,
    /// Number of objects that have been lazy freed
    pub lazyfreed_objects: u64,
}

impl MetadataCollection for RedisMemoryInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("memory".to_string())]))
    }
    fn description(&self) -> &'static str {
        "Return the memory information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "memory"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl Default for RedisMemoryInfo {
    fn default() -> Self {
        Self {
            used_memory: 0,
            used_memory_human: String::new(),
            used_memory_rss: 0,
            used_memory_rss_human: String::new(),
            used_memory_peak: 0,
            used_memory_peak_human: String::new(),
            used_memory_peak_perc: String::new(),
            used_memory_overhead: 0,
            used_memory_startup: 0,
            used_memory_dataset: 0,
            used_memory_dataset_perc: String::new(),
            total_system_memory: 0,
            total_system_memory_human: String::new(),
            used_memory_lua: 0,
            used_memory_vm_eval: 0,
            used_memory_lua_human: String::new(),
            used_memory_scripts_eval: 0,
            number_of_cached_scripts: 0,
            number_of_functions: 0,
            number_of_libraries: 0,
            used_memory_vm_functions: 0,
            used_memory_vm_total: 0,
            used_memory_vm_total_human: String::new(),
            used_memory_functions: 0,
            used_memory_scripts: 0,
            used_memory_scripts_human: String::new(),
            maxmemory: 0,
            maxmemory_human: String::new(),
            maxmemory_policy: String::new(),
            mem_fragmentation_ratio: 0.0,
            mem_fragmentation_bytes: 0,
            allocator_frag_ratio: 0.0,
            allocator_frag_bytes: 0,
            allocator_rss_ratio: 0.0,
            allocator_rss_bytes: 0,
            rss_overhead_ratio: 0.0,
            rss_overhead_bytes: 0,
            allocator_allocated: 0,
            allocator_active: 0,
            allocator_resident: 0,
            allocator_muzzy: 0,
            mem_not_counted_for_evict: 0,
            mem_clients_slaves: 0,
            mem_clients_normal: 0,
            mem_cluster_links: 0,
            mem_aof_buffer: 0,
            mem_replication_backlog: 0,
            mem_total_replication_buffers: 0,
            mem_allocator: String::new(),
            mem_overhead_db_hashtable_rehashing: 0,
            active_defrag_running: false,
            lazyfree_pending_objects: 0,
            lazyfreed_objects: 0,
        }
    }
}

impl RedisMemoryInfo {
    /// Calculates memory utilization as a percentage of total system memory
    ///
    /// # Returns
    /// * Percentage of system memory used by Redis (0.0 to 100.0)
    pub fn system_memory_utilization_percentage(&self) -> f64 {
        if self.total_system_memory == 0 {
            0.0
        } else {
            (self.used_memory as f64 / self.total_system_memory as f64) * 100.0
        }
    }

    /// Calculates memory utilization as a percentage of configured maxmemory
    ///
    /// # Returns
    /// * Percentage of maxmemory used (0.0 to 100.0), or 0.0 if maxmemory is 0
    pub fn maxmemory_utilization_percentage(&self) -> f64 {
        if self.maxmemory == 0 {
            0.0
        } else {
            (self.used_memory as f64 / self.maxmemory as f64) * 100.0
        }
    }

    /// Checks if memory usage is approaching the configured limit
    ///
    /// # Arguments
    /// * `threshold_percentage` - Warning threshold as percentage (0.0 to 100.0)
    ///
    /// # Returns
    /// * True if memory usage exceeds the threshold percentage of maxmemory
    pub fn is_approaching_memory_limit(&self, threshold_percentage: f64) -> bool {
        self.maxmemory_utilization_percentage() > threshold_percentage
    }

    /// Calculates the efficiency of memory usage (dataset vs overhead)
    ///
    /// # Returns
    /// * Percentage of used memory that is actual data (0.0 to 100.0)
    pub fn memory_efficiency_percentage(&self) -> f64 {
        if self.used_memory == 0 {
            0.0
        } else {
            (self.used_memory_dataset as f64 / self.used_memory as f64) * 100.0
        }
    }

    /// Checks if memory fragmentation is concerning
    ///
    /// # Arguments
    /// * `fragmentation_threshold` - Fragmentation ratio threshold (e.g., 1.5)
    /// * `min_fragmentation_bytes` - Minimum fragmentation bytes to consider (e.g., 10MB)
    ///
    /// # Returns
    /// * True if fragmentation exceeds both thresholds
    pub fn has_concerning_fragmentation(&self, fragmentation_threshold: f64, min_fragmentation_bytes: i64) -> bool {
        self.mem_fragmentation_ratio > fragmentation_threshold && self.mem_fragmentation_bytes > min_fragmentation_bytes
    }

    /// Gets the total memory used by all client connections
    ///
    /// # Returns
    /// * Total client memory usage in bytes
    pub fn total_client_memory(&self) -> u64 {
        self.mem_clients_normal + self.mem_clients_slaves
    }

    /// Gets the total memory used by scripts and VM engines
    ///
    /// # Returns
    /// * Total script memory usage in bytes (includes both counted and non-counted memory)
    pub fn total_script_memory(&self) -> u64 {
        // For Redis 7.0+, use the new metrics; fall back to legacy for older versions
        if self.used_memory_vm_total > 0 || self.used_memory_scripts > 0 {
            self.used_memory_vm_total + self.used_memory_scripts
        } else {
            self.used_memory_lua
        }
    }

    /// Gets the total memory used by replication
    ///
    /// # Returns
    /// * Total replication memory usage in bytes
    pub fn total_replication_memory(&self) -> u64 {
        // For Redis 7.0+, use mem_total_replication_buffers if available
        if self.mem_total_replication_buffers > 0 {
            self.mem_total_replication_buffers
        } else {
            self.mem_replication_backlog + self.mem_clients_slaves
        }
    }

    /// Checks if active defragmentation is running
    ///
    /// # Returns
    /// * True if active defragmentation is currently running
    pub fn is_defragmentation_active(&self) -> bool {
        self.active_defrag_running
    }

    /// Gets the amount of memory that can potentially be reclaimed
    ///
    /// # Returns
    /// * Bytes that could be reclaimed through defragmentation or cleanup
    pub fn reclaimable_memory(&self) -> u64 {
        let fragmentation_waste = if self.mem_fragmentation_bytes > 0 {
            self.mem_fragmentation_bytes as u64
        } else {
            0
        };

        let muzzy_memory = self.allocator_muzzy;
        let _pending_lazy_free = self.lazyfree_pending_objects; // This is a count, not bytes

        fragmentation_waste + muzzy_memory
    }

    /// Checks if there are scripts consuming significant memory
    ///
    /// # Arguments
    /// * `threshold_bytes` - Memory threshold in bytes
    ///
    /// # Returns
    /// * True if script memory usage exceeds the threshold
    pub fn has_high_script_memory_usage(&self, threshold_bytes: u64) -> bool {
        self.total_script_memory() > threshold_bytes
    }

    /// Gets a summary of memory breakdown by category
    ///
    /// # Returns
    /// * Tuple of (dataset, overhead, clients, scripts, replication, other) in bytes
    pub fn memory_breakdown(&self) -> (u64, u64, u64, u64, u64, u64) {
        let dataset = self.used_memory_dataset;
        let overhead = self.used_memory_overhead;
        let clients = self.total_client_memory();
        let scripts = self.total_script_memory();
        let replication = self.total_replication_memory();
        let other = self.used_memory.saturating_sub(dataset + overhead + clients + scripts + replication);

        (dataset, overhead, clients, scripts, replication, other)
    }
}
