use crate::api::{AclUsersInput, Deserialize, Serialize};
use crate::metadata::stc::config::RedisConfigInfo;
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisSecurityInfo {
    pub acl_users: Vec<String>,
    pub acl_categories: Vec<String>,
    pub ssl_enabled: bool,
    pub ssl_cert_file: Option<String>,
    pub ssl_key_file: Option<String>,
    pub ssl_ca_cert_file: Option<String>,
    pub auth_required: bool,
    pub protected_mode: bool,

    // Enhanced security details
    /// Default ACL user configuration
    pub default_acl_user: Option<AclUserInfo>,
    /// Detailed information about ACL users
    pub acl_user_details: Vec<AclUserInfo>,
    /// SSL/TLS configuration details
    pub ssl_config: SslConfigInfo,
    /// Authentication configuration
    pub auth_config: AuthConfigInfo,
    /// Network security settings
    pub network_security: NetworkSecurityInfo,
    /// Command access control
    pub command_access_control: CommandAccessInfo,
    /// Security events and violations
    pub security_events: SecurityEventInfo,
}

impl MetadataCollection for RedisSecurityInfo {
    type Request = AclUsersInput;

    fn request(&self) -> Self::Request {
        AclUsersInput {}
    }
    fn description(&self) -> &'static str {
        "Return the security information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "security"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

impl Default for RedisSecurityInfo {
    fn default() -> Self {
        Self {
            acl_users: Vec::new(),
            acl_categories: Vec::new(),
            ssl_enabled: false,
            ssl_cert_file: None,
            ssl_key_file: None,
            ssl_ca_cert_file: None,
            auth_required: false,
            protected_mode: true,
            default_acl_user: None,
            acl_user_details: Vec::new(),
            ssl_config: SslConfigInfo::default(),
            auth_config: AuthConfigInfo::default(),
            network_security: NetworkSecurityInfo::default(),
            command_access_control: CommandAccessInfo::default(),
            security_events: SecurityEventInfo::default(),
        }
    }
}

impl Default for AclUserInfo {
    fn default() -> Self {
        Self {
            username: String::new(),
            flags: Vec::new(),
            passwords: Vec::new(),
            categories: Vec::new(),
            commands: Vec::new(),
            keys: Vec::new(),
            channels: Vec::new(),
            selectors: Vec::new(),
            is_enabled: true,
            is_default: false,
            created_at: None,
            last_login: None,
        }
    }
}

impl Default for AuthConfigInfo {
    fn default() -> Self {
        Self {
            requirepass: None,
            auth_users: Vec::new(),
            default_user: "default".to_string(),
            noauth_authentication_enabled: false,
            password_history_enabled: false,
            password_min_length: None,
            password_policy: None,
            max_login_attempts: None,
            lockout_duration: None,
        }
    }
}

impl Default for NetworkSecurityInfo {
    fn default() -> Self {
        Self {
            protected_mode: true,
            bind_addresses: vec!["127.0.0.1".to_string()],
            port: 6379,
            tcp_backlog: None,
            tcp_keepalive: None,
            timeout: None,
            tcp_user_timeout: None,
            maxclients: 10000,
            unixsocket: None,
            unixsocketperm: None,
            whitelist_enabled: false,
            allowed_ips: Vec::new(),
        }
    }
}

impl Default for CommandAccessInfo {
    fn default() -> Self {
        Self {
            rename_commands: std::collections::HashMap::new(),
            disabled_commands: Vec::new(),
            admin_commands_enabled: true,
            dangerous_commands_restricted: false,
            eval_enabled: true,
            script_security_enabled: false,
            lua_script_time_limit: Some(5000),
            pubsub_channels_restricted: false,
            module_commands_access: std::collections::HashMap::new(),
        }
    }
}

impl RedisSecurityInfo {
    #[allow(dead_code)]
    pub(crate) fn update_from_config(&mut self, config: &RedisConfigInfo) {
        // Check for authentication requirement
        if let Some(requirepass) = config.config.get("requirepass") {
            self.auth_required = !requirepass.is_empty();
        }

        // Check protected mode
        if let Some(protected_mode) = config.config.get("protected-mode") {
            self.protected_mode = protected_mode == "yes";
        }

        // Check SSL configuration
        if let Some(tls_port) = config.config.get("tls-port") {
            self.ssl_enabled = tls_port != "0";
        }

        self.ssl_cert_file = config.config.get("tls-cert-file").cloned();
        self.ssl_key_file = config.config.get("tls-key-file").cloned();
        self.ssl_ca_cert_file = config.config.get("tls-ca-cert-file").cloned();
    }
}

/// Detailed ACL user information
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct AclUserInfo {
    pub username: String,
    pub flags: Vec<String>,
    pub passwords: Vec<String>,  // Hashed passwords
    pub categories: Vec<String>, // Command categories allowed
    pub commands: Vec<String>,   // Specific commands allowed
    pub keys: Vec<String>,       // Key patterns allowed
    pub channels: Vec<String>,   // Pub/sub channels allowed
    pub selectors: Vec<String>,  // Key selectors (Redis 7.0+)
    pub is_enabled: bool,
    pub is_default: bool,
    pub created_at: Option<u64>,
    pub last_login: Option<u64>,
}

/// SSL/TLS configuration information
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct SslConfigInfo {
    pub tls_port: u16,
    pub tls_cert_file: Option<String>,
    pub tls_key_file: Option<String>,
    pub tls_ca_cert_file: Option<String>,
    pub tls_ca_cert_dir: Option<String>,
    pub tls_protocols: Vec<String>,
    pub tls_ciphers: Vec<String>,
    pub tls_prefer_server_ciphers: bool,
    pub tls_session_caching: bool,
    pub tls_session_cache_size: Option<u32>,
    pub tls_session_cache_timeout: Option<u32>,
    pub client_cert_auth_required: bool,
    pub ssl_enabled: bool,
}

/// Authentication configuration information
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct AuthConfigInfo {
    pub requirepass: Option<String>, // Password hash
    pub auth_users: Vec<String>,
    pub default_user: String,
    pub noauth_authentication_enabled: bool,
    pub password_history_enabled: bool,
    pub password_min_length: Option<u32>,
    pub password_policy: Option<String>,
    pub max_login_attempts: Option<u32>,
    pub lockout_duration: Option<u32>,
}

/// Network security settings
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct NetworkSecurityInfo {
    pub protected_mode: bool,
    pub bind_addresses: Vec<String>,
    pub port: u16,
    pub tcp_backlog: Option<u32>,
    pub tcp_keepalive: Option<u32>,
    pub timeout: Option<u32>,
    pub tcp_user_timeout: Option<u32>,
    pub maxclients: u32,
    pub unixsocket: Option<String>,
    pub unixsocketperm: Option<String>,
    pub whitelist_enabled: bool,
    pub allowed_ips: Vec<String>,
}

/// Command access control information
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct CommandAccessInfo {
    pub rename_commands: std::collections::HashMap<String, String>,
    pub disabled_commands: Vec<String>,
    pub admin_commands_enabled: bool,
    pub dangerous_commands_restricted: bool,
    pub eval_enabled: bool,
    pub script_security_enabled: bool,
    pub lua_script_time_limit: Option<u64>,
    pub pubsub_channels_restricted: bool,
    pub module_commands_access: std::collections::HashMap<String, Vec<String>>,
}

/// Security events and violations
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct SecurityEventInfo {
    pub failed_auth_attempts: u64,
    pub blocked_connections: u64,
    pub acl_denied_commands: u64,
    pub acl_denied_keys: u64,
    pub acl_denied_channels: u64,
    pub suspicious_activities: Vec<SuspiciousActivity>,
    pub last_security_scan: Option<u64>,
    pub security_violations: u64,
}

/// Suspicious activity record
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct SuspiciousActivity {
    pub timestamp: u64,
    pub activity_type: String,
    pub source_ip: Option<String>,
    pub user: Option<String>,
    pub command: Option<String>,
    pub description: String,
    pub severity: String, // "low", "medium", "high", "critical"
}
