use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use std::{collections::HashMap, str::FromStr};

/// Redis client connection information and statistics
///
/// This struct contains metrics about client connections to the Redis server,
/// including connection counts, buffer sizes, and blocking operations.
/// Data is typically collected from the "Clients" section of Redis INFO command.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisClientInfo {
    /// Total number of client connections currently connected to the Redis server
    /// Excludes connections from replicas
    pub connected_clients: u32,
    /// An approximation of the number of sockets used by the cluster's bus
    /// Only present in Redis cluster mode
    pub cluster_connections: u32,
    /// The value of the maxclients configuration directive
    /// Upper limit for the sum of connected_clients, connected_slaves and cluster_connections
    pub maxclients: u32,
    /// Largest input buffer among all current client connections (in bytes)
    /// Helps identify clients sending large commands that could impact memory usage
    pub client_recent_max_input_buffer: u64,
    /// Largest output buffer among all current client connections (in bytes)
    /// Indicates clients that may be slow to read responses, causing memory buildup
    pub client_recent_max_output_buffer: u64,
    /// Number of clients currently blocked on blocking operations
    /// Such as BLPOP, BRPOP, BRPOPLPUSH, BLMOVE, BZPOPMIN, BZPOPMAX
    pub blocked_clients: u32,
    /// Number of clients being tracked for client-side caching
    /// Available in Redis 6.0+ with client-side caching feature
    pub tracking_clients: u32,
    /// Number of clients in pubsub mode (SUBSCRIBE, PSUBSCRIBE, SSUBSCRIBE)
    /// Added in Redis 7.4
    pub pubsub_clients: u32,
    /// Number of clients in watching mode (WATCH)
    /// Added in Redis 7.4
    pub watching_clients: u32,
    /// Number of clients in the timeout table
    /// Clients that have a timeout set and are being monitored for timeout events
    pub clients_in_timeout_table: u32,
    /// Number of watched keys
    /// Added in Redis 7.4
    pub total_watched_keys: u64,
    /// Total number of keys currently being blocked on by clients
    /// Across all blocking operations like BLPOP, BRPOP, etc.
    /// Added in Redis 7.2
    pub total_blocking_keys: u64,
    /// Number of blocking keys that are tracking keys that don't exist yet
    /// Clients waiting for keys that haven't been created
    /// Added in Redis 7.2
    pub total_blocking_keys_on_nokey: u64,
    /// Detailed information about individual client connections
    /// Populated from CLIENT LIST command output
    pub client_details: Vec<RedisClientDetail>,
}

impl MetadataCollection for RedisClientInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("clients".to_string())]))
    }
    fn description(&self) -> &'static str {
        "Return the clients information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "clients"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

/// Detailed information about an individual Redis client connection
///
/// Contains all the fields available from the CLIENT LIST command output.
/// Each client connection has a unique ID and various attributes describing
/// its current state and resource usage.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisClientDetail {
    /// Unique client connection identifier
    pub id: u64,
    /// Client IP address and port (e.g., "127.0.0.1:12345")
    pub addr: String,
    /// File descriptor number used by this connection
    pub fd: Option<i32>,
    /// Client name set via CLIENT SETNAME command
    pub name: Option<String>,
    /// Age of the connection in seconds
    pub age: u64,
    /// Idle time in seconds since last command
    pub idle: u64,
    /// Client flags indicating connection state and type
    pub flags: Vec<RedisClientFlag>,
    /// Database ID currently selected by this client
    pub db: u32,
    /// Number of channel subscriptions
    pub sub: u32,
    /// Number of pattern subscriptions
    pub psub: u32,
    /// Number of commands in a MULTI/EXEC context (-1 if not in transaction)
    pub multi: i32,
    /// Query buffer length (0 means no query pending)
    pub qbuf: u64,
    /// Free space of query buffer (0 means buffer is full)
    pub qbuf_free: u64,
    /// Output buffer length
    pub obl: u64,
    /// Output list length (replies that have not been written yet)
    pub oll: u64,
    /// Output buffer memory usage
    pub omem: u64,
    /// File descriptor events (r=readable, w=writable)
    pub events: String,
    /// Last command executed by this client
    pub cmd: Option<String>,
    /// Client type (normal, master, slave, pubsub)
    pub client_type: RedisClientType,
    /// Total buffer memory usage (calculated)
    pub total_buffer_memory: u64,
    /// Additional attributes that may be present in newer Redis versions
    pub additional_attrs: HashMap<String, String>,
}

/// Redis client connection flags
///
/// These flags indicate the current state and characteristics of a client connection.
/// Multiple flags can be set simultaneously for a single client.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum RedisClientFlag {
    /// Slave connection
    Slave,
    /// Master connection
    Master,
    /// Connection is being monitored
    Monitor,
    /// Connection from within a MULTI/EXEC context
    Multi,
    /// Client is blocked
    Blocked,
    /// Connection is dirty (commands were executed)
    Dirty,
    /// Connection is in CLOSE_AFTER_REPLY state
    CloseAfterReply,
    /// Connection is not authenticated
    Unblocked,
    /// Connection is in READONLY mode
    ReadOnly,
    /// Connection tracking is enabled
    Tracking,
    /// Connection has opted-in to receive TRACKING notifications
    TrackingBroadcast,
    /// Unknown flag (for forward compatibility)
    Unknown(String),
}

/// Redis client connection type
///
/// Categorizes clients based on their role and usage pattern.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum RedisClientType {
    /// Regular client connection
    Normal,
    /// Master connection for replication
    Master,
    /// Slave/replica connection
    Slave,
    /// Pub/Sub only connection
    PubSub,
    /// Unknown type (for forward compatibility)
    Unknown(String),
}

impl Default for RedisClientInfo {
    /// Creates a new RedisClientInfo with all metrics set to zero
    /// Used as a starting point before populating with actual Redis data
    fn default() -> Self {
        Self {
            connected_clients: 0,
            cluster_connections: 0,
            maxclients: 0,
            client_recent_max_input_buffer: 0,
            client_recent_max_output_buffer: 0,
            blocked_clients: 0,
            tracking_clients: 0,
            pubsub_clients: 0,
            watching_clients: 0,
            clients_in_timeout_table: 0,
            total_watched_keys: 0,
            total_blocking_keys: 0,
            total_blocking_keys_on_nokey: 0,
            client_details: Vec::new(),
        }
    }
}

impl RedisClientDetail {}

impl Default for RedisClientDetail {
    fn default() -> Self {
        Self {
            id: 0,
            addr: String::new(),
            fd: None,
            name: None,
            age: 0,
            idle: 0,
            flags: Vec::new(),
            db: 0,
            sub: 0,
            psub: 0,
            multi: -1,
            qbuf: 0,
            qbuf_free: 0,
            obl: 0,
            oll: 0,
            omem: 0,
            events: String::new(),
            cmd: None,
            client_type: RedisClientType::Normal,
            total_buffer_memory: 0,
            additional_attrs: HashMap::new(),
        }
    }
}

impl RedisClientInfo {
    /// Calculates the average buffer size per connected client
    ///
    /// # Returns
    /// * Average input buffer size in bytes, or 0 if no clients connected
    pub fn avg_input_buffer_size(&self) -> u64 {
        if self.connected_clients == 0 {
            0
        } else {
            self.client_recent_max_input_buffer / self.connected_clients as u64
        }
    }

    /// Calculates the average output buffer size per connected client
    ///
    /// # Returns
    /// * Average output buffer size in bytes, or 0 if no clients connected
    pub fn avg_output_buffer_size(&self) -> u64 {
        if self.connected_clients == 0 {
            0
        } else {
            self.client_recent_max_output_buffer / self.connected_clients as u64
        }
    }

    /// Calculates the percentage of clients that are currently blocked
    ///
    /// # Returns
    /// * Percentage of blocked clients (0.0 to 100.0), or 0.0 if no clients connected
    pub fn blocked_clients_percentage(&self) -> f64 {
        if self.connected_clients == 0 {
            0.0
        } else {
            (self.blocked_clients as f64 / self.connected_clients as f64) * 100.0
        }
    }

    /// Checks if the server has any clients with large buffers that might indicate issues
    ///
    /// # Arguments
    /// * `threshold_bytes` - Buffer size threshold in bytes
    ///
    /// # Returns
    /// * True if any client has buffers exceeding the threshold
    pub fn has_large_buffers(&self, threshold_bytes: u64) -> bool {
        self.client_recent_max_input_buffer > threshold_bytes || self.client_recent_max_output_buffer > threshold_bytes
    }

    /// Checks if client-side caching is being used
    ///
    /// # Returns
    /// * True if any clients are being tracked for caching (Redis 6.0+ feature)
    pub fn is_client_caching_active(&self) -> bool {
        self.tracking_clients > 0
    }

    /// Checks if the client limit is being approached
    ///
    /// # Arguments
    /// * `threshold_percentage` - Warning threshold as percentage (0.0 to 100.0)
    ///
    /// # Returns
    /// * True if connected clients exceed the threshold percentage of maxclients
    pub fn is_approaching_client_limit(&self, threshold_percentage: f64) -> bool {
        if self.maxclients == 0 {
            false
        } else {
            let usage_percentage = (self.connected_clients as f64 / self.maxclients as f64) * 100.0;
            usage_percentage > threshold_percentage
        }
    }

    /// Gets the client connection utilization percentage
    ///
    /// # Returns
    /// * Percentage of maxclients currently in use (0.0 to 100.0)
    pub fn client_utilization_percentage(&self) -> f64 {
        if self.maxclients == 0 {
            0.0
        } else {
            (self.connected_clients as f64 / self.maxclients as f64) * 100.0
        }
    }

    /// Gets clients filtered by type
    ///
    /// # Arguments
    /// * `client_type` - The type of clients to filter for
    ///
    /// # Returns
    /// * Vector of client details matching the specified type
    pub fn get_clients_by_type(&self, client_type: &RedisClientType) -> Vec<&RedisClientDetail> {
        self.client_details.iter().filter(|client| &client.client_type == client_type).collect()
    }

    /// Gets clients that have been idle longer than the specified threshold
    ///
    /// # Arguments
    /// * `idle_threshold_seconds` - Minimum idle time in seconds
    ///
    /// # Returns
    /// * Vector of client details for clients exceeding the idle threshold
    pub fn get_idle_clients(&self, idle_threshold_seconds: u64) -> Vec<&RedisClientDetail> {
        self.client_details.iter().filter(|client| client.idle > idle_threshold_seconds).collect()
    }

    /// Gets clients with large buffer usage
    ///
    /// # Arguments
    /// * `buffer_threshold_bytes` - Minimum buffer size in bytes
    ///
    /// # Returns
    /// * Vector of client details for clients with large buffers
    pub fn get_clients_with_large_buffers(&self, buffer_threshold_bytes: u64) -> Vec<&RedisClientDetail> {
        self.client_details
            .iter()
            .filter(|client| client.qbuf > buffer_threshold_bytes || client.omem > buffer_threshold_bytes)
            .collect()
    }

    /// Gets total memory usage by all client output buffers
    ///
    /// # Returns
    /// * Total output buffer memory usage in bytes
    pub fn total_output_buffer_memory(&self) -> u64 {
        self.client_details.iter().map(|client| client.omem).sum()
    }

    /// Gets clients grouped by database
    ///
    /// # Returns
    /// * HashMap mapping database ID to vector of clients using that database
    pub fn get_clients_by_database(&self) -> HashMap<u32, Vec<&RedisClientDetail>> {
        let mut clients_by_db = HashMap::new();

        for client in &self.client_details {
            clients_by_db.entry(client.db).or_insert_with(Vec::new).push(client);
        }

        clients_by_db
    }

    /// Gets clients that are currently in a transaction (MULTI/EXEC)
    ///
    /// # Returns
    /// * Vector of client details for clients in transaction state
    pub fn get_clients_in_transaction(&self) -> Vec<&RedisClientDetail> {
        self.client_details.iter().filter(|client| client.multi >= 0).collect()
    }

    /// Gets clients with pub/sub subscriptions
    ///
    /// # Returns
    /// * Vector of client details for clients with active subscriptions
    pub fn get_pubsub_clients(&self) -> Vec<&RedisClientDetail> {
        self.client_details.iter().filter(|client| client.sub > 0 || client.psub > 0).collect()
    }
}

impl RedisClientDetail {
    fn parse_field<T: FromStr>(value: &str, field: &str) -> Result<T, String> {
        value.parse::<T>().map_err(|_| format!("Invalid {}: {}", field, value))
    }

    /// Parses a single line from CLIENT LIST output into a RedisClientDetail
    ///
    /// # Arguments
    /// * `line` - A single line from CLIENT LIST command output
    ///
    /// # Returns
    /// * Result containing the parsed client detail or an error message
    ///
    /// # Example line format:
    /// id=1 addr=127.0.0.1:12345 fd=6 name= age=123 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=ping
    pub fn parse_from_line(line: &str) -> Result<Self, String> {
        let mut client = RedisClientDetail {
            id: 0,
            addr: String::new(),
            fd: None,
            name: None,
            age: 0,
            idle: 0,
            flags: Vec::new(),
            db: 0,
            sub: 0,
            psub: 0,
            multi: -1,
            qbuf: 0,
            qbuf_free: 0,
            obl: 0,
            oll: 0,
            omem: 0,
            events: String::new(),
            cmd: None,
            client_type: RedisClientType::Normal,
            total_buffer_memory: 0,
            additional_attrs: HashMap::new(),
        };

        // Parse space-separated key=value pairs
        for pair in line.split_whitespace() {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() != 2 {
                continue;
            }

            let key = parts[0];
            let value = parts[1];

            match key {
                "id" => {
                    client.id = Self::parse_field(value, "id")?;
                }
                "addr" => {
                    client.addr = value.to_string();
                }
                "fd" => {
                    if !value.is_empty() {
                        client.fd = Some(Self::parse_field(value, "fd")?);
                    }
                }
                "name" => {
                    if !value.is_empty() {
                        client.name = Some(value.to_string());
                    }
                }
                "age" => {
                    client.age = Self::parse_field(value, "age")?;
                }
                "idle" => {
                    client.idle = Self::parse_field(value, "idle")?;
                }
                "flags" => {
                    client.flags = RedisClientFlag::parse_flags(value);
                    client.client_type = RedisClientType::from_flags(&client.flags);
                }
                "db" => {
                    client.db = Self::parse_field(value, "db")?;
                }
                "sub" => {
                    client.sub = Self::parse_field(value, "sub")?;
                }
                "psub" => {
                    client.psub = Self::parse_field(value, "psub")?;
                }
                "multi" => {
                    client.multi = Self::parse_field(value, "multi")?;
                }
                "qbuf" => {
                    client.qbuf = Self::parse_field(value, "qbuf")?;
                }
                "qbuf-free" => {
                    client.qbuf_free = Self::parse_field(value, "qbuf-free")?;
                }
                "obl" => {
                    client.obl = Self::parse_field(value, "obl")?;
                }
                "oll" => {
                    client.oll = Self::parse_field(value, "oll")?;
                }
                "omem" => {
                    client.omem = Self::parse_field(value, "omem")?;
                }
                "events" => {
                    client.events = value.to_string();
                }
                "cmd" => {
                    if !value.is_empty() {
                        client.cmd = Some(value.to_string());
                    }
                }
                _ => {
                    // Store unknown attributes for forward compatibility
                    client.additional_attrs.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(client)
    }

    /// Checks if this client is currently blocked
    pub fn is_blocked(&self) -> bool {
        self.flags.contains(&RedisClientFlag::Blocked)
    }

    /// Checks if this client is a replication connection
    pub fn is_replication(&self) -> bool {
        matches!(self.client_type, RedisClientType::Master | RedisClientType::Slave)
    }

    /// Checks if this client has active subscriptions
    pub fn has_subscriptions(&self) -> bool {
        self.sub > 0 || self.psub > 0
    }

    /// Checks if this client is in a transaction
    pub fn is_in_transaction(&self) -> bool {
        self.multi >= 0
    }

    /// Gets the total buffer memory usage for this client
    pub fn total_buffer_memory(&self) -> u64 {
        self.qbuf + self.omem
    }
}

impl RedisClientFlag {
    /// Parses flag string into vector of RedisClientFlag enums
    ///
    /// # Arguments
    /// * `flags_str` - String containing client flags (e.g., "SMr")
    ///
    /// # Returns
    /// * Vector of parsed client flags
    pub fn parse_flags(flags_str: &str) -> Vec<Self> {
        let mut flags = Vec::new();

        for ch in flags_str.chars() {
            let flag = match ch {
                'S' => RedisClientFlag::Slave,
                'M' => RedisClientFlag::Master,
                'x' => RedisClientFlag::Multi,
                'b' => RedisClientFlag::Blocked,
                'd' => RedisClientFlag::Dirty,
                'c' => RedisClientFlag::CloseAfterReply,
                'u' => RedisClientFlag::Unblocked,
                'r' => RedisClientFlag::ReadOnly,
                't' => RedisClientFlag::Tracking,
                'B' => RedisClientFlag::TrackingBroadcast,
                'O' => RedisClientFlag::Monitor,
                _ => RedisClientFlag::Unknown(ch.to_string()),
            };
            flags.push(flag);
        }

        flags
    }
}

impl RedisClientType {
    /// Determines client type based on flags
    ///
    /// # Arguments
    /// * `flags` - Vector of client flags
    ///
    /// # Returns
    /// * Appropriate client type based on the flags
    pub fn from_flags(flags: &[RedisClientFlag]) -> Self {
        if flags.contains(&RedisClientFlag::Master) {
            RedisClientType::Master
        } else if flags.contains(&RedisClientFlag::Slave) {
            RedisClientType::Slave
        } else if flags.iter().any(|f| matches!(f, RedisClientFlag::Unknown(_))) {
            // Check if it's a pub/sub only connection by looking at unknown flags
            // This is a heuristic as CLIENT LIST doesn't explicitly mark pub/sub clients
            RedisClientType::Normal
        } else {
            RedisClientType::Normal
        }
    }
}
