use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

/// Redis replication configuration and status information
///
/// This struct contains comprehensive replication metrics from the Redis server,
/// including master/replica status, replication backlog, and sync operations.
/// Data is collected from the "Replication" section of Redis INFO command.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisReplicationInfo {
    // Core replication identity
    /// Server role: "master" or "slave"
    pub role: RedisRole,
    /// Number of connected replicas
    pub connected_slaves: u32,
    /// The state of an ongoing failover, if any
    pub master_failover_state: Option<String>,

    // Replication IDs and offsets
    /// The replication ID of the Redis server
    pub master_replid: Option<String>,
    /// The secondary replication ID, used for PSYNC after a failover
    pub master_replid2: Option<String>,
    /// The server's current replication offset
    pub master_repl_offset: Option<u64>,
    /// The offset up to which replication IDs are accepted
    pub second_repl_offset: Option<i64>,

    // Replication backlog
    /// Flag indicating replication backlog is active
    pub repl_backlog_active: Option<bool>,
    /// Total size in bytes of the replication backlog buffer
    pub repl_backlog_size: Option<u64>,
    /// The master offset of the replication backlog buffer
    pub repl_backlog_first_byte_offset: Option<u64>,
    /// Size in bytes of the data in the replication backlog buffer
    pub repl_backlog_histlen: Option<u64>,

    // Replica-specific fields (when role is slave)
    /// Host or IP address of the master
    pub master_host: Option<String>,
    /// Master listening TCP port
    pub master_port: Option<u16>,
    /// Status of the link (up/down)
    pub master_link_status: Option<String>,
    /// Number of seconds since the last interaction with master
    pub master_last_io_seconds_ago: Option<u64>,
    /// Indicate the master is syncing to the replica
    pub master_sync_in_progress: Option<bool>,
    /// The read replication offset of the replica instance
    pub slave_read_repl_offset: Option<u64>,
    /// The replication offset of the replica instance
    pub slave_repl_offset: Option<u64>,
    /// The priority of the instance as a candidate for failover
    pub slave_priority: Option<u32>,
    /// Flag indicating if the replica is read-only
    pub slave_read_only: Option<bool>,
    /// Flag indicating if the replica is announced by Sentinel
    pub replica_announced: Option<bool>,

    // SYNC operation fields (when sync is ongoing)
    /// Total number of bytes that need to be transferred
    pub master_sync_total_bytes: Option<u64>,
    /// Number of bytes already transferred
    pub master_sync_read_bytes: Option<u64>,
    /// Number of bytes left before syncing is complete
    pub master_sync_left_bytes: Option<i64>,
    /// The percentage of sync completion
    pub master_sync_perc: Option<f64>,
    /// Number of seconds since last transfer I/O during a SYNC operation
    pub master_sync_last_io_seconds_ago: Option<u64>,

    // Link down information
    /// Number of seconds since the link is down (when link is down)
    pub master_link_down_since_seconds: Option<u64>,

    // Minimum replicas configuration
    /// Number of replicas currently considered good (min-replicas-to-write directive)
    pub min_slaves_good_slaves: Option<u32>,

    /// Information about connected replicas
    pub slave_replicas: Vec<RedisSlaveInfo>,
}

impl MetadataCollection for RedisReplicationInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("replication".to_string())])).to_owned()
    }
    fn description(&self) -> &'static str {
        "Return the replication information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "replication"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum RedisRole {
    Master,
    Slave,
}

/// Information about a connected replica
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisSlaveInfo {
    /// Replica IP address
    pub ip: String,
    /// Replica port
    pub port: u16,
    /// Replica state (online, sync, etc.)
    pub state: String,
    /// Replica replication offset
    pub offset: u64,
    /// Replica lag in seconds
    pub lag: u64,
}

impl RedisSlaveInfo {}

impl Default for RedisReplicationInfo {
    fn default() -> Self {
        Self {
            role: RedisRole::Master,
            connected_slaves: 0,
            master_failover_state: None,
            master_replid: None,
            master_replid2: None,
            master_repl_offset: None,
            second_repl_offset: None,
            repl_backlog_active: None,
            repl_backlog_size: None,
            repl_backlog_first_byte_offset: None,
            repl_backlog_histlen: None,
            master_host: None,
            master_port: None,
            master_link_status: None,
            master_last_io_seconds_ago: None,
            master_sync_in_progress: None,
            slave_read_repl_offset: None,
            slave_repl_offset: None,
            slave_priority: None,
            slave_read_only: None,
            replica_announced: None,
            master_sync_total_bytes: None,
            master_sync_read_bytes: None,
            master_sync_left_bytes: None,
            master_sync_perc: None,
            master_sync_last_io_seconds_ago: None,
            master_link_down_since_seconds: None,
            min_slaves_good_slaves: None,
            slave_replicas: Vec::new(),
        }
    }
}

impl RedisReplicationInfo {
    /// Checks if this instance is a master
    pub fn is_master(&self) -> bool {
        matches!(self.role, RedisRole::Master)
    }

    /// Checks if this instance is a replica/slave
    pub fn is_replica(&self) -> bool {
        matches!(self.role, RedisRole::Slave)
    }

    /// Checks if replication is healthy
    ///
    /// # Returns
    /// * True if replication appears to be functioning normally
    pub fn is_replication_healthy(&self) -> bool {
        match self.role {
            RedisRole::Master => {
                // For master, healthy if no failover in progress and backlog is active (if replicas exist)
                if self.connected_slaves == 0 {
                    true // No replicas, so replication is not applicable
                } else {
                    self.master_failover_state.is_none() && self.repl_backlog_active.unwrap_or(false)
                }
            }
            RedisRole::Slave => {
                // For replica, healthy if link is up and not syncing
                self.master_link_status.as_ref().is_some_and(|status| status == "up")
                    && !self.master_sync_in_progress.unwrap_or(false)
                    && self.master_link_down_since_seconds.is_none()
            }
        }
    }

    /// Gets the current sync progress for an ongoing SYNC operation
    ///
    /// # Returns
    /// * Progress percentage (0.0 to 100.0) or None if no sync in progress
    pub fn sync_progress(&self) -> Option<f64> {
        if self.master_sync_in_progress.unwrap_or(false) {
            self.master_sync_perc
        } else {
            None
        }
    }

    /// Calculates replication lag for this replica
    ///
    /// # Returns
    /// * Replication lag in bytes, or None if not applicable or no offset data
    pub fn replication_lag_bytes(&self) -> Option<u64> {
        if self.is_replica() {
            match (self.master_repl_offset, self.slave_repl_offset) {
                (Some(master_offset), Some(slave_offset)) => Some(master_offset.saturating_sub(slave_offset)),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Gets the time since last communication with master
    ///
    /// # Returns
    /// * Seconds since last I/O with master, or None if not a replica
    pub fn master_communication_lag(&self) -> Option<u64> {
        self.master_last_io_seconds_ago
    }

    /// Checks if the replication link is down
    ///
    /// # Returns
    /// * True if the link to master is down
    pub fn is_link_down(&self) -> bool {
        if self.is_replica() {
            self.master_link_status.as_ref().is_none_or(|status| status != "up") || self.master_link_down_since_seconds.is_some()
        } else {
            false
        }
    }

    /// Gets the duration since the link went down
    ///
    /// # Returns
    /// * Seconds since link went down, or None if link is up
    pub fn link_down_duration(&self) -> Option<u64> {
        self.master_link_down_since_seconds
    }

    /// Checks if there are enough healthy replicas (for min-replicas-to-write)
    ///
    /// # Returns
    /// * True if minimum replica requirements are met, or None if not configured
    pub fn has_sufficient_replicas(&self) -> Option<bool> {
        self.min_slaves_good_slaves.map(|good_slaves| good_slaves > 0)
    }

    /// Gets information about lagging replicas
    ///
    /// # Arguments
    /// * `lag_threshold_seconds` - Maximum acceptable lag in seconds
    ///
    /// # Returns
    /// * Vector of replica info for replicas exceeding the lag threshold
    pub fn get_lagging_replicas(&self, lag_threshold_seconds: u64) -> Vec<&RedisSlaveInfo> {
        self.slave_replicas.iter().filter(|replica| replica.lag > lag_threshold_seconds).collect()
    }

    /// Gets information about replicas in non-online state
    ///
    /// # Returns
    /// * Vector of replica info for replicas not in "online" state
    pub fn get_unhealthy_replicas(&self) -> Vec<&RedisSlaveInfo> {
        self.slave_replicas.iter().filter(|replica| replica.state != "online").collect()
    }

    /// Calculates backlog utilization percentage
    ///
    /// # Returns
    /// * Percentage of backlog buffer used (0.0 to 100.0), or None if no backlog
    pub fn backlog_utilization_percentage(&self) -> Option<f64> {
        match (self.repl_backlog_size, self.repl_backlog_histlen) {
            (Some(size), Some(histlen)) if size > 0 => Some((histlen as f64 / size as f64) * 100.0),
            _ => None,
        }
    }

    /// Checks if backlog is at risk of overflow
    ///
    /// # Arguments
    /// * `threshold_percentage` - Threshold percentage (0.0 to 100.0)
    ///
    /// # Returns
    /// * True if backlog utilization exceeds threshold
    pub fn is_backlog_at_risk(&self, threshold_percentage: f64) -> bool {
        self.backlog_utilization_percentage().is_some_and(|utilization| utilization > threshold_percentage)
    }

    /// Gets replication health summary
    ///
    /// # Returns
    /// * Tuple of (is_healthy, has_lag_issues, has_connectivity_issues, has_sync_issues)
    pub fn replication_health_summary(&self) -> (bool, bool, bool, bool) {
        let is_healthy = self.is_replication_healthy();
        let has_lag_issues = !self.get_lagging_replicas(10).is_empty(); // 10 second threshold
        let has_connectivity_issues = self.is_link_down() || self.master_communication_lag().is_some_and(|lag| lag > 30); // 30 second threshold
        let has_sync_issues = self.master_sync_in_progress.unwrap_or(false) && self.sync_progress().is_some_and(|progress| progress < 50.0); // Sync stalled below 50%

        (is_healthy, has_lag_issues, has_connectivity_issues, has_sync_issues)
    }

    /// Estimates remaining sync time for ongoing SYNC operation
    ///
    /// # Returns
    /// * Estimated seconds remaining, or None if no sync in progress or insufficient data
    pub fn estimated_sync_time_remaining(&self) -> Option<u64> {
        if !self.master_sync_in_progress.unwrap_or(false) {
            return None;
        }

        match (self.master_sync_read_bytes, self.master_sync_total_bytes, self.master_sync_last_io_seconds_ago) {
            (Some(read_bytes), Some(total_bytes), Some(last_io)) if total_bytes > read_bytes && last_io > 0 => {
                let remaining_bytes = total_bytes - read_bytes;
                let bytes_per_second = read_bytes as f64 / last_io as f64;
                if bytes_per_second > 0.0 {
                    Some((remaining_bytes as f64 / bytes_per_second) as u64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Checks if this replica is eligible for failover
    ///
    /// # Returns
    /// * True if replica is announced and has a priority set
    pub fn is_failover_eligible(&self) -> bool {
        self.is_replica() && self.replica_announced.unwrap_or(false) && self.slave_priority.unwrap_or(0) > 0
    }

    /// Gets the maximum lag among all connected replicas
    ///
    /// # Returns
    /// * Maximum lag in seconds, or 0 if no replicas
    pub fn max_replica_lag(&self) -> u64 {
        self.slave_replicas.iter().map(|replica| replica.lag).max().unwrap_or(0)
    }

    /// Gets the average lag among all connected replicas
    ///
    /// # Returns
    /// * Average lag in seconds, or 0.0 if no replicas
    pub fn average_replica_lag(&self) -> f64 {
        if self.slave_replicas.is_empty() {
            0.0
        } else {
            let total_lag: u64 = self.slave_replicas.iter().map(|replica| replica.lag).sum();
            total_lag as f64 / self.slave_replicas.len() as f64
        }
    }
}

impl RedisSlaveInfo {
    /// Parses a slave info string from Redis INFO output
    ///
    /// # Arguments
    /// * `s` - Slave info string in format "ip=127.0.0.1,port=6380,state=online,offset=123,lag=0"
    ///
    /// # Returns
    /// * Parsed RedisSlaveInfo or error if format is invalid
    pub fn parse_from_string(s: &str) -> Result<Self, &'static str> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() < 3 {
            return Err("Invalid slave info format");
        }

        let ip = parts[0].split('=').nth(1).unwrap_or("").to_string();
        let port = parts[1].split('=').nth(1).unwrap_or("0").parse().unwrap_or(0);
        let state = parts[2].split('=').nth(1).unwrap_or("").to_string();
        let offset = parts.get(3).and_then(|p| p.split('=').nth(1)).and_then(|s| s.parse().ok()).unwrap_or(0);
        let lag = parts.get(4).and_then(|p| p.split('=').nth(1)).and_then(|s| s.parse().ok()).unwrap_or(0);

        Ok(RedisSlaveInfo { ip, port, state, offset, lag })
    }

    /// Checks if this replica is healthy
    ///
    /// # Returns
    /// * True if replica state is "online"
    pub fn is_healthy(&self) -> bool {
        self.state == "online"
    }

    /// Checks if this replica has concerning lag
    ///
    /// # Arguments
    /// * `threshold_seconds` - Maximum acceptable lag in seconds
    ///
    /// # Returns
    /// * True if lag exceeds the threshold
    pub fn has_concerning_lag(&self, threshold_seconds: u64) -> bool {
        self.lag > threshold_seconds
    }
}
