use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

/// Redis persistence and data durability information
///
/// This struct contains comprehensive persistence metrics from the Redis server,
/// including RDB snapshots, AOF logging, and data loading operations.
/// Data is collected from the "Persistence" section of Redis INFO command.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisPersistenceInfo {
    // General loading and COW metrics
    /// Flag indicating if the load of a dump file is on-going
    pub loading: bool,
    /// Currently loading replication data-set asynchronously while serving old data (Redis 7.0+)
    pub async_loading: bool,
    /// The peak size in bytes of copy-on-write memory while a child fork is running
    pub current_cow_peak: u64,
    /// The size in bytes of copy-on-write memory while a child fork is running
    pub current_cow_size: u64,
    /// The age, in seconds, of the current_cow_size value
    pub current_cow_size_age: u64,
    /// The percentage of progress of the current fork process
    pub current_fork_perc: f64,
    /// Number of keys processed by the current save operation
    pub current_save_keys_processed: u64,
    /// Number of keys at the beginning of the current save operation
    pub current_save_keys_total: u64,

    // RDB snapshot metrics
    /// Number of changes since the last dump
    pub rdb_changes_since_last_save: u64,
    /// Flag indicating a RDB save is on-going
    pub rdb_bgsave_in_progress: bool,
    /// Epoch-based timestamp of last successful RDB save
    pub rdb_last_save_time: u64,
    /// Status of the last RDB save operation
    pub rdb_last_bgsave_status: String,
    /// Duration of the last RDB save operation in seconds
    pub rdb_last_bgsave_time_sec: i64,
    /// Duration of the on-going RDB save operation if any
    pub rdb_current_bgsave_time_sec: i64,
    /// The size in bytes of copy-on-write memory during the last RDB save operation
    pub rdb_last_cow_size: u64,
    /// Number of volatile keys deleted during the last RDB loading (Redis 7.0+)
    pub rdb_last_load_keys_expired: u64,
    /// Number of keys loaded during the last RDB loading (Redis 7.0+)
    pub rdb_last_load_keys_loaded: u64,
    /// Number of RDB snapshots performed since startup
    pub rdb_saves: u64,

    // AOF logging metrics
    /// Flag indicating AOF logging is activated
    pub aof_enabled: bool,
    /// Flag indicating a AOF rewrite operation is on-going
    pub aof_rewrite_in_progress: bool,
    /// Flag indicating an AOF rewrite operation will be scheduled once the on-going RDB save is complete
    pub aof_rewrite_scheduled: bool,
    /// Duration of the last AOF rewrite operation in seconds
    pub aof_last_rewrite_time_sec: i64,
    /// Duration of the on-going AOF rewrite operation if any
    pub aof_current_rewrite_time_sec: i64,
    /// Status of the last AOF rewrite operation
    pub aof_last_bgrewrite_status: String,
    /// Status of the last write operation to the AOF
    pub aof_last_write_status: String,
    /// The size in bytes of copy-on-write memory during the last AOF rewrite operation
    pub aof_last_cow_size: u64,
    /// Number of AOF rewrites performed since startup
    pub aof_rewrites: u64,

    // Module fork metrics
    /// Flag indicating a module fork is on-going
    pub module_fork_in_progress: bool,
    /// The size in bytes of copy-on-write memory during the last module fork operation
    pub module_fork_last_cow_size: u64,

    // AOF-specific fields (when AOF is enabled)
    /// AOF current file size
    pub aof_current_size: Option<u64>,
    /// AOF file size on latest startup or rewrite
    pub aof_base_size: Option<u64>,
    /// Flag indicating an AOF rewrite operation will be scheduled once the on-going RDB save is complete
    pub aof_pending_rewrite: Option<bool>,
    /// Size of the AOF buffer
    pub aof_buffer_length: Option<u64>,
    /// Size of the AOF rewrite buffer (removed in Redis 7.0)
    pub aof_rewrite_buffer_length: Option<u64>,
    /// Number of fsync pending jobs in background I/O queue
    pub aof_pending_bio_fsync: Option<u64>,
    /// Delayed fsync counter
    pub aof_delayed_fsync: Option<u64>,

    // Loading operation fields (when a load operation is on-going)
    /// Epoch-based timestamp of the start of the load operation
    pub loading_start_time: Option<u64>,
    /// Total file size being loaded
    pub loading_total_bytes: Option<u64>,
    /// The memory usage of the server that had generated the RDB file
    pub loading_rdb_used_mem: Option<u64>,
    /// Number of bytes already loaded
    pub loading_loaded_bytes: Option<u64>,
    /// Same value as loading_loaded_bytes expressed as a percentage
    pub loading_loaded_perc: Option<f64>,
    /// ETA in seconds for the load to be complete
    pub loading_eta_seconds: Option<u64>,
}

impl MetadataCollection for RedisPersistenceInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("persistence".to_string())]))
    }
    fn description(&self) -> &'static str {
        "Return the persistence information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "persistence"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl Default for RedisPersistenceInfo {
    fn default() -> Self {
        Self {
            loading: false,
            async_loading: false,
            current_cow_peak: 0,
            current_cow_size: 0,
            current_cow_size_age: 0,
            current_fork_perc: 0.0,
            current_save_keys_processed: 0,
            current_save_keys_total: 0,
            rdb_changes_since_last_save: 0,
            rdb_bgsave_in_progress: false,
            rdb_last_save_time: 0,
            rdb_last_bgsave_status: String::new(),
            rdb_last_bgsave_time_sec: -1,
            rdb_current_bgsave_time_sec: -1,
            rdb_last_cow_size: 0,
            rdb_last_load_keys_expired: 0,
            rdb_last_load_keys_loaded: 0,
            rdb_saves: 0,
            aof_enabled: false,
            aof_rewrite_in_progress: false,
            aof_rewrite_scheduled: false,
            aof_last_rewrite_time_sec: -1,
            aof_current_rewrite_time_sec: -1,
            aof_last_bgrewrite_status: String::new(),
            aof_last_write_status: String::new(),
            aof_last_cow_size: 0,
            aof_rewrites: 0,
            module_fork_in_progress: false,
            module_fork_last_cow_size: 0,
            aof_current_size: None,
            aof_base_size: None,
            aof_pending_rewrite: None,
            aof_buffer_length: None,
            aof_rewrite_buffer_length: None,
            aof_pending_bio_fsync: None,
            aof_delayed_fsync: None,
            loading_start_time: None,
            loading_total_bytes: None,
            loading_rdb_used_mem: None,
            loading_loaded_bytes: None,
            loading_loaded_perc: None,
            loading_eta_seconds: None,
        }
    }
}

impl RedisPersistenceInfo {
    /// Checks if any persistence operation is currently active
    ///
    /// # Returns
    /// * True if RDB save, AOF rewrite, or module fork is in progress
    pub fn is_persistence_active(&self) -> bool {
        self.rdb_bgsave_in_progress || self.aof_rewrite_in_progress || self.module_fork_in_progress
    }

    /// Checks if data loading operation is currently active
    ///
    /// # Returns
    /// * True if data is being loaded from disk
    pub fn is_loading_active(&self) -> bool {
        self.loading || self.async_loading
    }

    /// Gets the current progress of any active save operation
    ///
    /// # Returns
    /// * Progress percentage (0.0 to 100.0) or None if no operation is active
    pub fn current_save_progress(&self) -> Option<f64> {
        if self.current_save_keys_total > 0 {
            Some((self.current_save_keys_processed as f64 / self.current_save_keys_total as f64) * 100.0)
        } else {
            None
        }
    }

    /// Gets the current progress of any active loading operation
    ///
    /// # Returns
    /// * Progress percentage (0.0 to 100.0) or None if no loading is active
    pub fn current_loading_progress(&self) -> Option<f64> {
        self.loading_loaded_perc
    }

    /// Calculates time since last RDB save
    ///
    /// # Arguments
    /// * `current_time` - Current epoch timestamp
    ///
    /// # Returns
    /// * Seconds since last RDB save, or None if no save has occurred
    pub fn time_since_last_rdb_save(&self, current_time: u64) -> Option<u64> {
        if self.rdb_last_save_time > 0 {
            Some(current_time.saturating_sub(self.rdb_last_save_time))
        } else {
            None
        }
    }

    /// Checks if RDB backup is stale based on change count threshold
    ///
    /// # Arguments
    /// * `change_threshold` - Maximum number of changes before backup is considered stale
    ///
    /// # Returns
    /// * True if changes since last save exceed the threshold
    pub fn is_rdb_backup_stale(&self, change_threshold: u64) -> bool {
        self.rdb_changes_since_last_save > change_threshold
    }

    /// Checks if AOF file size has grown significantly since last rewrite
    ///
    /// # Arguments
    /// * `growth_ratio_threshold` - Ratio threshold (e.g., 2.0 for 100% growth)
    ///
    /// # Returns
    /// * True if AOF has grown beyond the threshold ratio, or None if AOF is disabled
    pub fn is_aof_growth_significant(&self, growth_ratio_threshold: f64) -> Option<bool> {
        if !self.aof_enabled {
            return None;
        }

        match (self.aof_current_size, self.aof_base_size) {
            (Some(current), Some(base)) if base > 0 => {
                let growth_ratio = current as f64 / base as f64;
                Some(growth_ratio > growth_ratio_threshold)
            }
            _ => None,
        }
    }

    /// Checks if any persistence operation has failed recently
    ///
    /// # Returns
    /// * True if last RDB save or AOF rewrite/write failed
    pub fn has_persistence_failures(&self) -> bool {
        self.rdb_last_bgsave_status.to_lowercase().contains("err")
            || self.aof_last_bgrewrite_status.to_lowercase().contains("err")
            || self.aof_last_write_status.to_lowercase().contains("err")
    }

    /// Gets the total copy-on-write memory usage across all operations
    ///
    /// # Returns
    /// * Current COW memory usage in bytes
    pub fn total_cow_memory(&self) -> u64 {
        self.current_cow_size
    }

    /// Gets the peak copy-on-write memory usage
    ///
    /// # Returns
    /// * Peak COW memory usage in bytes during current fork
    pub fn peak_cow_memory(&self) -> u64 {
        self.current_cow_peak
    }

    /// Estimates remaining time for current loading operation
    ///
    /// # Returns
    /// * Estimated seconds remaining, or None if no loading is active
    pub fn loading_eta(&self) -> Option<u64> {
        self.loading_eta_seconds
    }

    /// Calculates data loading speed
    ///
    /// # Arguments
    /// * `current_time` - Current epoch timestamp
    ///
    /// # Returns
    /// * Bytes per second loading speed, or None if no loading is active
    pub fn loading_speed_bytes_per_sec(&self, current_time: u64) -> Option<f64> {
        match (self.loading_start_time, self.loading_loaded_bytes) {
            (Some(start_time), Some(loaded_bytes)) => {
                let elapsed = current_time.saturating_sub(start_time);
                if elapsed > 0 {
                    Some(loaded_bytes as f64 / elapsed as f64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Gets persistence health summary
    ///
    /// # Returns
    /// * Tuple of (rdb_healthy, aof_healthy, operations_active)
    pub fn persistence_health_summary(&self) -> (bool, bool, bool) {
        let rdb_healthy = !self.rdb_last_bgsave_status.to_lowercase().contains("err");
        let aof_healthy =
            !self.aof_last_bgrewrite_status.to_lowercase().contains("err") && !self.aof_last_write_status.to_lowercase().contains("err");
        let operations_active = self.is_persistence_active();

        (rdb_healthy, aof_healthy, operations_active)
    }

    /// Checks if persistence configuration is recommended for production
    ///
    /// # Returns
    /// * True if either RDB or AOF is enabled
    pub fn has_persistence_enabled(&self) -> bool {
        self.aof_enabled || self.rdb_saves > 0 || self.rdb_last_save_time > 0
    }

    /// Gets the ratio of expired keys during last RDB load
    ///
    /// # Returns
    /// * Ratio of expired to total loaded keys (0.0 to 1.0), or None if no load data
    pub fn rdb_load_expiration_ratio(&self) -> Option<f64> {
        let total_keys = self.rdb_last_load_keys_loaded + self.rdb_last_load_keys_expired;
        if total_keys > 0 {
            Some(self.rdb_last_load_keys_expired as f64 / total_keys as f64)
        } else {
            None
        }
    }

    /// Checks if AOF fsync operations are lagging
    ///
    /// # Arguments
    /// * `threshold` - Maximum acceptable pending fsync operations
    ///
    /// # Returns
    /// * True if pending fsync operations exceed threshold, or None if AOF disabled
    pub fn is_aof_fsync_lagging(&self, threshold: u64) -> Option<bool> {
        if !self.aof_enabled {
            return None;
        }

        self.aof_pending_bio_fsync.map(|pending| pending > threshold)
    }
}
