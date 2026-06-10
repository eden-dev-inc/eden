use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

/// Redis CPU usage and performance information
///
/// This struct contains comprehensive CPU metrics from the Redis server,
/// including system and user CPU time for main thread, background threads, and child processes.
/// Data is collected from the "CPU" section of Redis INFO command.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisCpuInfo {
    /// System CPU consumed by the Redis server (all threads combined)
    /// Sum of system CPU consumed by main thread and background threads
    pub used_cpu_sys: f64,

    /// User CPU consumed by the Redis server (all threads combined)
    /// Sum of user CPU consumed by main thread and background threads
    pub used_cpu_user: f64,

    /// System CPU consumed by background processes (child processes)
    /// Includes RDB saves, AOF rewrites, and other forked operations
    pub used_cpu_sys_children: f64,

    /// User CPU consumed by background processes (child processes)
    /// Includes RDB saves, AOF rewrites, and other forked operations
    pub used_cpu_user_children: f64,

    /// System CPU consumed by the Redis server main thread only
    /// Excludes background threads like I/O threads, bio threads, etc.
    pub used_cpu_sys_main_thread: f64,

    /// User CPU consumed by the Redis server main thread only
    /// Excludes background threads like I/O threads, bio threads, etc.
    pub used_cpu_user_main_thread: f64,
}

impl MetadataCollection for RedisCpuInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("cpu".to_string())]))
    }

    fn description(&self) -> &'static str {
        "Return the CPU usage information for the Redis database"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "cpu"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl Default for RedisCpuInfo {
    fn default() -> Self {
        Self {
            used_cpu_sys: 0.0,
            used_cpu_user: 0.0,
            used_cpu_sys_children: 0.0,
            used_cpu_user_children: 0.0,
            used_cpu_sys_main_thread: 0.0,
            used_cpu_user_main_thread: 0.0,
        }
    }
}

impl RedisCpuInfo {
    /// Calculates total CPU time consumed by the Redis server
    ///
    /// # Returns
    /// * Total CPU seconds (system + user) for all server threads
    pub fn total_server_cpu_time(&self) -> f64 {
        self.used_cpu_sys + self.used_cpu_user
    }

    /// Calculates total CPU time consumed by child processes
    ///
    /// # Returns
    /// * Total CPU seconds (system + user) for all child processes
    pub fn total_children_cpu_time(&self) -> f64 {
        self.used_cpu_sys_children + self.used_cpu_user_children
    }

    /// Calculates total CPU time consumed by the main thread only
    ///
    /// # Returns
    /// * Total CPU seconds (system + user) for the main thread
    pub fn total_main_thread_cpu_time(&self) -> f64 {
        self.used_cpu_sys_main_thread + self.used_cpu_user_main_thread
    }

    /// Calculates background threads CPU time (excludes main thread)
    ///
    /// # Returns
    /// * Total CPU seconds for background threads (I/O threads, bio threads, etc.)
    pub fn background_threads_cpu_time(&self) -> f64 {
        self.total_server_cpu_time() - self.total_main_thread_cpu_time()
    }

    /// Calculates the ratio of system CPU to total CPU for the server
    ///
    /// # Returns
    /// * Ratio of system CPU time to total CPU time (0.0 to 1.0)
    pub fn server_system_cpu_ratio(&self) -> f64 {
        let total = self.total_server_cpu_time();
        if total == 0.0 { 0.0 } else { self.used_cpu_sys / total }
    }

    /// Calculates the ratio of user CPU to total CPU for the server
    ///
    /// # Returns
    /// * Ratio of user CPU time to total CPU time (0.0 to 1.0)
    pub fn server_user_cpu_ratio(&self) -> f64 {
        let total = self.total_server_cpu_time();
        if total == 0.0 { 0.0 } else { self.used_cpu_user / total }
    }

    /// Calculates the ratio of main thread CPU to total server CPU
    ///
    /// # Returns
    /// * Ratio of main thread CPU to total server CPU (0.0 to 1.0)
    pub fn main_thread_cpu_ratio(&self) -> f64 {
        let total = self.total_server_cpu_time();
        if total == 0.0 {
            0.0
        } else {
            self.total_main_thread_cpu_time() / total
        }
    }

    /// Calculates the ratio of background threads CPU to total server CPU
    ///
    /// # Returns
    /// * Ratio of background threads CPU to total server CPU (0.0 to 1.0)
    pub fn background_threads_cpu_ratio(&self) -> f64 {
        let total = self.total_server_cpu_time();
        if total == 0.0 {
            0.0
        } else {
            self.background_threads_cpu_time() / total
        }
    }

    /// Calculates the ratio of children CPU to total CPU (server + children)
    ///
    /// # Returns
    /// * Ratio of child processes CPU to total CPU usage (0.0 to 1.0)
    pub fn children_cpu_ratio(&self) -> f64 {
        let total = self.total_server_cpu_time() + self.total_children_cpu_time();
        if total == 0.0 {
            0.0
        } else {
            self.total_children_cpu_time() / total
        }
    }

    /// Calculates CPU utilization rate over a time period
    ///
    /// # Arguments
    /// * `previous_cpu_info` - Previous CPU info measurement
    /// * `time_elapsed_seconds` - Time elapsed between measurements in seconds
    ///
    /// # Returns
    /// * CPU utilization as percentage (0.0 to 100.0 per core)
    pub fn cpu_utilization_rate(&self, previous_cpu_info: &RedisCpuInfo, time_elapsed_seconds: f64) -> f64 {
        if time_elapsed_seconds <= 0.0 {
            return 0.0;
        }

        let current_total = self.total_server_cpu_time();
        let previous_total = previous_cpu_info.total_server_cpu_time();
        let cpu_time_diff = current_total - previous_total;

        (cpu_time_diff / time_elapsed_seconds) * 100.0
    }

    /// Calculates main thread CPU utilization rate over a time period
    ///
    /// # Arguments
    /// * `previous_cpu_info` - Previous CPU info measurement
    /// * `time_elapsed_seconds` - Time elapsed between measurements in seconds
    ///
    /// # Returns
    /// * Main thread CPU utilization as percentage (0.0 to 100.0 per core)
    pub fn main_thread_utilization_rate(&self, previous_cpu_info: &RedisCpuInfo, time_elapsed_seconds: f64) -> f64 {
        if time_elapsed_seconds <= 0.0 {
            return 0.0;
        }

        let current_main = self.total_main_thread_cpu_time();
        let previous_main = previous_cpu_info.total_main_thread_cpu_time();
        let cpu_time_diff = current_main - previous_main;

        (cpu_time_diff / time_elapsed_seconds) * 100.0
    }

    /// Calculates children processes CPU utilization rate over a time period
    ///
    /// # Arguments
    /// * `previous_cpu_info` - Previous CPU info measurement
    /// * `time_elapsed_seconds` - Time elapsed between measurements in seconds
    ///
    /// # Returns
    /// * Children processes CPU utilization as percentage (0.0 to 100.0 per core)
    pub fn children_utilization_rate(&self, previous_cpu_info: &RedisCpuInfo, time_elapsed_seconds: f64) -> f64 {
        if time_elapsed_seconds <= 0.0 {
            return 0.0;
        }

        let current_children = self.total_children_cpu_time();
        let previous_children = previous_cpu_info.total_children_cpu_time();
        let cpu_time_diff = current_children - previous_children;

        (cpu_time_diff / time_elapsed_seconds) * 100.0
    }

    /// Checks if CPU usage patterns indicate potential performance issues
    ///
    /// # Arguments
    /// * `high_system_cpu_threshold` - Threshold for concerning system CPU ratio (e.g., 0.3 for 30%)
    ///
    /// # Returns
    /// * True if CPU patterns suggest potential issues
    pub fn has_cpu_performance_concerns(&self, high_system_cpu_threshold: f64) -> bool {
        // High system CPU ratio might indicate I/O bottlenecks or system contention
        self.server_system_cpu_ratio() > high_system_cpu_threshold
    }

    /// Checks if background threads are consuming significant CPU
    ///
    /// # Arguments
    /// * `background_threshold` - Threshold for background thread CPU ratio (e.g., 0.2 for 20%)
    ///
    /// # Returns
    /// * True if background threads are using significant CPU
    pub fn has_high_background_cpu_usage(&self, background_threshold: f64) -> bool {
        self.background_threads_cpu_ratio() > background_threshold
    }

    /// Checks if child processes are consuming significant CPU
    ///
    /// # Arguments
    /// * `children_threshold` - Threshold for children CPU ratio (e.g., 0.1 for 10%)
    ///
    /// # Returns
    /// * True if child processes are using significant CPU
    pub fn has_high_children_cpu_usage(&self, children_threshold: f64) -> bool {
        self.children_cpu_ratio() > children_threshold
    }

    /// Gets a comprehensive CPU usage breakdown
    ///
    /// # Returns
    /// * Tuple of (main_thread_ratio, background_ratio, children_ratio, system_ratio)
    pub fn cpu_usage_breakdown(&self) -> (f64, f64, f64, f64) {
        (
            self.main_thread_cpu_ratio(),
            self.background_threads_cpu_ratio(),
            self.children_cpu_ratio(),
            self.server_system_cpu_ratio(),
        )
    }

    /// Estimates average CPU utilization since startup
    ///
    /// # Arguments
    /// * `uptime_seconds` - Server uptime in seconds
    ///
    /// # Returns
    /// * Average CPU utilization percentage since startup
    pub fn average_cpu_utilization_since_startup(&self, uptime_seconds: u64) -> f64 {
        if uptime_seconds == 0 {
            return 0.0;
        }

        let total_cpu_time = self.total_server_cpu_time() + self.total_children_cpu_time();
        (total_cpu_time / uptime_seconds as f64) * 100.0
    }

    /// Checks if the server is CPU-bound based on CPU patterns
    ///
    /// # Arguments
    /// * `user_cpu_threshold` - Threshold for user CPU dominance (e.g., 0.8 for 80%)
    ///
    /// # Returns
    /// * True if patterns suggest CPU-bound workload
    pub fn is_cpu_bound(&self, user_cpu_threshold: f64) -> bool {
        // CPU-bound workloads typically have high user CPU and low system CPU
        self.server_user_cpu_ratio() > user_cpu_threshold
    }

    /// Checks if the server is I/O-bound based on CPU patterns
    ///
    /// # Arguments
    /// * `system_cpu_threshold` - Threshold for system CPU dominance (e.g., 0.4 for 40%)
    ///
    /// # Returns
    /// * True if patterns suggest I/O-bound workload
    pub fn is_io_bound(&self, system_cpu_threshold: f64) -> bool {
        // I/O-bound workloads typically have higher system CPU relative to user CPU
        self.server_system_cpu_ratio() > system_cpu_threshold
    }

    /// Gets CPU health summary
    ///
    /// # Returns
    /// * Tuple of (is_healthy, has_performance_concerns, is_cpu_bound, is_io_bound)
    pub fn cpu_health_summary(&self) -> (bool, bool, bool, bool) {
        let has_concerns =
            self.has_cpu_performance_concerns(0.3) || self.has_high_background_cpu_usage(0.3) || self.has_high_children_cpu_usage(0.2);
        let is_healthy = !has_concerns;
        let is_cpu_bound = self.is_cpu_bound(0.8);
        let is_io_bound = self.is_io_bound(0.4);

        (is_healthy, has_concerns, is_cpu_bound, is_io_bound)
    }
}
