//! Redis Monitor (TUI)
//!
//! A terminal dashboard for monitoring Redis databases with migration support.
//!
//! # Usage
//!     cargo run -- <source> <dest> [api_endpoint] [eden_source] [eden_dest]
//!
//! # Arguments
//!     source       Source Redis as host:port or just port (default host: 172.24.2.218)
//!     dest         Destination Redis as host:port or just port (default host: 172.24.2.218)
//!     api_endpoint Eden API endpoint (default: http://localhost:8000)
//!     eden_source  Eden's source Redis as host:port (when different from TUI connection)
//!     eden_dest    Eden's dest Redis as host:port (when different from TUI connection)
//!
//! # Examples
//!     cargo run -- 6379 6380                           # Both use default host
//!     cargo run -- 192.168.1.10:6379 192.168.1.20:6380 # Different hosts
//!     cargo run -- localhost:6379 localhost:6380 http://localhost:8000 172.24.2.211:6379 172.24.2.218:6379
//!                                                      # TUI uses localhost, Eden uses different IPs
//!
//! # Controls
//!     q / Ctrl+C         Quit
//!     c                  Complete running migration
//!     b                  Rollback completed/failed migration
//!     f                  Force coverage check now
//!     o                  Toggle populator launcher
//!     v                  Toggle ops/sec chart
//!     w                  Toggle workload view (when available)
//!     Tab                Toggle migration mode (BigBang / Canary)
//!     s                  Start migration setup (connect to Eden API)
//!     m                  Trigger migration
//!     r                  Refresh migration status (retry if completed)
//!     +/=                Increase canary traffic by 5% (canary mode only)
//!     -                  Decrease canary traffic by 5% (canary mode only)

use clap::{Args, ValueEnum};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, GraphType, Paragraph, Row, Table},
    Frame, Terminal,
};
use redis::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use url::Url;

const HISTORY_SIZE: usize = 120;
const DEFAULT_API_BASE: &str = "http://localhost:8000";
const OBSERVER_READY_FILE_ENV: &str = "REDIS_OBSERVER_READY_FILE";
const WORKLOAD_STATS_FILE_ENV: &str = "REDIS_WORKLOAD_STATS_FILE";
const POPULATE_STATS_FILE_ENV: &str = "REDIS_POPULATE_STATS_FILE";
const DEFAULT_EDEN_NEW_ORG_SECRET: &str = "neworgsecret";
const DEFAULT_EDEN_ADMIN_PASSWORD: &str = "adam-demo-pass";

fn default_eden_new_org_secret() -> String {
    env::var("EDEN_NEW_ORG_TOKEN").unwrap_or_else(|_| DEFAULT_EDEN_NEW_ORG_SECRET.to_string())
}

fn default_eden_admin_password() -> String {
    env::var("EDEN_ADMIN_PASS").unwrap_or_else(|_| DEFAULT_EDEN_ADMIN_PASSWORD.to_string())
}

#[derive(Args, Debug, Clone)]
pub struct ObserveConfig {
    /// Source Redis as host:port, port, or full redis:// / rediss:// URL
    #[clap(index = 1)]
    pub source: Option<String>,
    /// Destination Redis as host:port, port, or full redis:// / rediss:// URL
    #[clap(index = 2)]
    pub dest: Option<String>,
    /// Source Redis URL for the observer
    #[clap(long = "source-url", env = "REDIS_SOURCE_URL")]
    pub source_url: Option<String>,
    /// Destination Redis URL for the observer
    #[clap(long = "dest-url", env = "REDIS_DEST_URL")]
    pub dest_url: Option<String>,
    /// Default Redis URL for the launcher/interlay target
    #[clap(long = "redis-url", env = "REDIS_URL")]
    pub redis_url: Option<String>,
    /// Interlay port Eden should use during setup
    #[clap(long, env = "INTERLAY_PORT", default_value = "5731")]
    pub interlay_port: u16,
    /// Eden API endpoint
    #[clap(index = 3)]
    pub api_endpoint: Option<String>,
    /// Eden API endpoint
    #[clap(long = "api-url", env = "EDEN_API_URL")]
    pub api_url: Option<String>,
    /// Eden organization ID
    #[clap(long, env = "EDEN_ORG_ID", default_value = "adam-demo")]
    pub org_id: String,
    /// Organization creation token
    #[clap(long, env = "EDEN_NEW_ORG_SECRET", default_value_t = default_eden_new_org_secret())]
    pub org_token: String,
    /// Eden admin username
    #[clap(long, env = "EDEN_ADMIN_USER", default_value = "admin")]
    pub admin_user: String,
    /// Eden admin password
    #[clap(long, env = "EDEN_ADMIN_PASSWORD", default_value_t = default_eden_admin_password())]
    pub admin_pass: String,
    /// Default migration mode for setup
    #[clap(long, env = "MIGRATION_MODE", value_enum, default_value = "big-bang")]
    pub mode: MigrationMode,
    /// Default canary read percentage for setup
    #[clap(long, env = "CANARY_READ_PCT", default_value = "0.05")]
    pub canary_read_pct: f64,
    /// Enable the integrated populator launcher view
    #[clap(long, env = "REDIS_MIGRATOR_ENABLE_POPULATOR", default_value_t = true)]
    pub enable_populator: bool,
    /// Eden source Redis override for setup/migration calls
    #[clap(long = "eden-source-url", value_name = "EDEN_SOURCE")]
    pub eden_source: Option<String>,
    /// Eden source Redis override for setup/migration calls when passed positionally
    #[clap(index = 4, value_name = "EDEN_SOURCE")]
    pub eden_source_positional: Option<String>,
    /// Eden destination Redis override for setup/migration calls
    #[clap(long = "eden-dest-url", value_name = "EDEN_DEST")]
    pub eden_dest: Option<String>,
    /// Eden destination Redis override for setup/migration calls when passed positionally
    #[clap(index = 5, value_name = "EDEN_DEST")]
    pub eden_dest_positional: Option<String>,
}

// ============================================
// API Response Types
// ============================================

#[derive(Debug, Clone, Deserialize)]
struct LoginResponse {
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
struct EndpointResponseData {
    id: String,
    uuid: String,
}

#[derive(Debug, Clone, Deserialize)]
struct InterlayResponseData {
    id: String,
    #[allow(dead_code)]
    uuid: String,
}

#[derive(Debug, Clone, Deserialize)]
struct MigrationResponseData {
    id: String,
    #[allow(dead_code)]
    uuid: String,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Debug, Clone)]
struct RedisConnInfo {
    url: String,
    host: String,
    port: String,
    tls: bool,
    password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct UpdateTrafficResponse {
    #[allow(dead_code)]
    migration_id: String,
    old_percentage: f64,
    new_percentage: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct CompleteMigrationResponse {
    #[allow(dead_code)]
    migration_id: String,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RollbackInterlayResponse {
    #[allow(dead_code)]
    migration_id: String,
    interlay_id: String,
    status: String,
    #[allow(dead_code)]
    rolled_back_at: String,
    #[allow(dead_code)]
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkloadStatsSnapshot {
    status: String,
    key_prefix: String,
    num_keys: u64,
    write_pct: u8,
    value_size: usize,
    concurrency: usize,
    duration_secs: u64,
    elapsed_secs: f64,
    total_reads: u64,
    total_writes: u64,
    total_errors: u64,
    ops_per_sec: f64,
    reads_per_sec: f64,
    writes_per_sec: f64,
    avg_read_latency_us: f64,
    avg_write_latency_us: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct PopulateStatsSnapshot {
    status: String,
    url: String,
    key_prefix: String,
    data_type: String,
    megabytes: u64,
    key_size: u64,
    batch_size: usize,
    ttl: u64,
    elements_per_key: usize,
    clear: bool,
    pipes: usize,
    then_client_write_pct: Option<u8>,
    client_duration_secs: u64,
    client_concurrency: usize,
    total_bytes: u64,
    bytes_written: u64,
    target_keys: u64,
    written_keys: u64,
    elapsed_secs: f64,
    mb_per_sec: f64,
    keys_per_sec: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PopulateDataType {
    String,
    Json,
    Hash,
    List,
    Set,
    SortedSet,
    Mixed,
}

impl PopulateDataType {
    fn label(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Json => "json",
            Self::Hash => "hash",
            Self::List => "list",
            Self::Set => "set",
            Self::SortedSet => "zset",
            Self::Mixed => "mixed",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::String => Self::Json,
            Self::Json => Self::Hash,
            Self::Hash => Self::List,
            Self::List => Self::Set,
            Self::Set => Self::SortedSet,
            Self::SortedSet => Self::Mixed,
            Self::Mixed => Self::String,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::String => Self::Mixed,
            Self::Json => Self::String,
            Self::Hash => Self::Json,
            Self::List => Self::Hash,
            Self::Set => Self::List,
            Self::SortedSet => Self::Set,
            Self::Mixed => Self::SortedSet,
        }
    }
}

#[derive(Debug, Clone)]
struct PopulateLauncherState {
    selected_field: usize,
    url: String,
    megabytes: String,
    key_size: String,
    data_type: PopulateDataType,
    prefix: String,
    batch_size: String,
    ttl: String,
    elements_per_key: String,
    clear: bool,
    pipes: String,
    then_client_write_pct: String,
    client_duration: String,
    client_concurrency: String,
    status_message: Option<String>,
}

impl PopulateLauncherState {
    const FIELD_COUNT: usize = 13;

    fn new(default_url: String) -> Self {
        Self {
            selected_field: 0,
            url: default_url,
            megabytes: "100".to_string(),
            key_size: "1024".to_string(),
            data_type: PopulateDataType::Mixed,
            prefix: "pop".to_string(),
            batch_size: "10000".to_string(),
            ttl: "0".to_string(),
            elements_per_key: "10".to_string(),
            clear: false,
            pipes: "1".to_string(),
            then_client_write_pct: String::new(),
            client_duration: "60".to_string(),
            client_concurrency: "50".to_string(),
            status_message: None,
        }
    }

    fn move_next(&mut self) {
        self.selected_field = (self.selected_field + 1) % Self::FIELD_COUNT;
    }

    fn move_prev(&mut self) {
        if self.selected_field == 0 {
            self.selected_field = Self::FIELD_COUNT - 1;
        } else {
            self.selected_field -= 1;
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct PauseMigrationResponse {
    #[allow(dead_code)]
    migration_id: String,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    paused_at: String,
    #[allow(dead_code)]
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ResumeMigrationResponse {
    #[allow(dead_code)]
    migration_id: String,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    resumed_at: String,
    #[allow(dead_code)]
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToggleEnvironmentResponse {
    #[allow(dead_code)]
    migration_id: String,
    #[allow(dead_code)]
    previous_active: String,
    #[allow(dead_code)]
    new_active: String,
    #[allow(dead_code)]
    write_mode: String,
    #[allow(dead_code)]
    updated_at: String,
    #[allow(dead_code)]
    updated_by: String,
}

// ============================================
// Migration Mode Selection
// ============================================

#[derive(Debug, Clone, Copy, PartialEq, Default, ValueEnum)]
pub enum MigrationMode {
    #[default]
    BigBang,
    Canary,
    BlueGreen,
}

impl MigrationMode {
    fn toggle(&self) -> Self {
        match self {
            Self::BigBang => Self::Canary,
            Self::Canary => Self::BlueGreen,
            Self::BlueGreen => Self::BigBang,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::BigBang => "BigBang",
            Self::Canary => "Canary",
            Self::BlueGreen => "BlueGreen",
        }
    }
}

/// Canary-specific state for traffic management
#[derive(Debug, Clone)]
struct CanaryState {
    /// Current read percentage routed to new system (0.0 to 1.0)
    read_percentage: f64,
    /// Write consistency policy
    write_policy: &'static str,
}

impl Default for CanaryState {
    fn default() -> Self {
        Self {
            read_percentage: 0.05, // Start with 5%
            write_policy: "OldAuthoritative",
        }
    }
}

// ============================================
// Migration State Machine
// ============================================

#[derive(Debug, Clone, PartialEq)]
enum SetupStep {
    NotStarted,
    CreatingOrganization,
    LoggingIn,
    CreatingSourceEndpoint,
    CreatingDestEndpoint,
    CreatingInterlay,
    CreatingMigration,
    AddingInterlay,
    Ready,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq)]
enum MigrationStatus {
    NotSetup,
    Pending,
    Testing,
    Ready,
    Running,
    PartialFailure,
    Failed,
    Paused,
    Completed,
    RollingBack,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq)]
enum ApiCallStatus {
    Pending,
    InProgress,
    Success,
    Failed(String),
    Skipped,
}

#[derive(Debug, Clone)]
struct ApiCall {
    name: String,
    status: ApiCallStatus,
}

impl ApiCall {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            status: ApiCallStatus::Pending,
        }
    }
}

#[derive(Debug, Clone)]
struct MigrationState {
    setup_step: SetupStep,
    auth_token: Option<String>,
    org_id: String,
    api_base: String,
    source_endpoint_id: Option<String>,
    dest_endpoint_id: Option<String>,
    interlay_id: Option<String>,
    migration_id: Option<String>,
    status: MigrationStatus,
    last_error: Option<String>,
    api_calls: Vec<ApiCall>,
    /// Selected migration mode (BigBang or Canary)
    mode: MigrationMode,
    /// Canary-specific state (only relevant when mode is Canary)
    canary: CanaryState,
    /// Blue-green state: true if new (green) environment is active, false if old (blue) is active
    active_is_new: bool,
}

impl MigrationState {
    fn new(api_base: String, org_id: String) -> Self {
        Self {
            setup_step: SetupStep::NotStarted,
            auth_token: None,
            org_id,
            api_base,
            source_endpoint_id: None,
            dest_endpoint_id: None,
            interlay_id: None,
            migration_id: None,
            status: MigrationStatus::NotSetup,
            last_error: None,
            api_calls: vec![
                ApiCall::new("Create Organization"),
                ApiCall::new("Login"),
                ApiCall::new("Create Source Endpoint"),
                ApiCall::new("Create Dest Endpoint"),
                ApiCall::new("Create Interlay"),
                ApiCall::new("Create Migration"),
                ApiCall::new("Add Interlay to Migration"),
            ],
            mode: MigrationMode::default(),
            canary: CanaryState::default(),
            active_is_new: false,
        }
    }

    fn update_api_call(&mut self, index: usize, status: ApiCallStatus) {
        if index < self.api_calls.len() {
            self.api_calls[index].status = status;
        }
    }

    fn is_ready(&self) -> bool {
        self.setup_step == SetupStep::Ready
    }

    fn can_migrate(&self) -> bool {
        self.is_ready()
            && matches!(
                self.status,
                MigrationStatus::Pending | MigrationStatus::Testing | MigrationStatus::Ready
            )
    }

    fn can_update_traffic(&self) -> bool {
        self.is_ready()
            && self.mode == MigrationMode::Canary
            && self.status == MigrationStatus::Running
    }

    fn can_toggle_environment(&self) -> bool {
        self.is_ready()
            && self.mode == MigrationMode::BlueGreen
            && self.status == MigrationStatus::Running
    }

    fn can_complete(&self) -> bool {
        self.is_ready() && self.status == MigrationStatus::Running
    }

    fn can_rollback(&self) -> bool {
        self.is_ready()
            && self.interlay_id.is_some()
            && matches!(
                self.status,
                MigrationStatus::Completed
                    | MigrationStatus::Failed
                    | MigrationStatus::PartialFailure
                    | MigrationStatus::Paused
            )
    }

    fn can_pause(&self) -> bool {
        self.is_ready() && self.status == MigrationStatus::Running
    }

    fn can_resume(&self) -> bool {
        self.is_ready() && self.status == MigrationStatus::Paused
    }
}

fn parse_migration_status(status: Option<&str>) -> MigrationStatus {
    match status {
        Some("Pending") | None => MigrationStatus::Pending,
        Some("Testing") => MigrationStatus::Testing,
        Some("Ready") => MigrationStatus::Ready,
        Some("Running") => MigrationStatus::Running,
        Some("PartialFailure") => MigrationStatus::PartialFailure,
        Some("Failed") => MigrationStatus::Failed,
        Some("Paused") => MigrationStatus::Paused,
        Some("Completed") => MigrationStatus::Completed,
        Some("RollingBack") => MigrationStatus::RollingBack,
        Some("RolledBack") => MigrationStatus::RolledBack,
        Some(_) => MigrationStatus::Pending, // Default to Pending for unknown
    }
}

// ============================================
// Async Event Channel Messages
// ============================================

#[derive(Debug)]
enum ApiEvent {
    SetupProgress(SetupStep),
    ApiCallUpdate {
        index: usize,
        status: ApiCallStatus,
    },
    SetupComplete {
        auth_token: String,
        source_endpoint_id: String,
        dest_endpoint_id: String,
        interlay_id: String,
        migration_id: String,
    },
    SetupFailed(String),
    MigrationTriggered,
    /// Status update from API. `force` bypasses stale-response protection (for explicit refresh)
    MigrationStatusUpdate {
        status: MigrationStatus,
        force: bool,
    },
    MigrationError(String),
    /// Debug log message from async tasks
    DebugLog(String),
    /// Canary traffic split was updated
    TrafficUpdated {
        old_percentage: f64,
        new_percentage: f64,
    },
    /// Canary traffic update failed
    TrafficUpdateFailed(String),
    /// Migration was manually completed
    MigrationCompleted,
    /// Migration completion failed
    MigrationCompleteFailed(String),
    /// Migration rollback initiated
    MigrationRolledBack,
    /// Migration rollback failed
    MigrationRollbackFailed(String),
    /// Migration was paused
    MigrationPaused,
    /// Migration pause failed
    MigrationPauseFailed(String),
    /// Migration was resumed
    MigrationResumed,
    /// Migration resume failed
    MigrationResumeFailed(String),
    /// Environment was toggled in blue-green migration
    EnvironmentToggled {
        previous_active: String,
        new_active: String,
    },
    /// Environment toggle failed
    EnvironmentToggleFailed(String),
}

// ============================================
// Eden API Client
// ============================================

struct EdenApiClient {
    client: reqwest::Client,
    base_url: String,
    auth_token: Option<String>,
    org_id: String,
}

impl EdenApiClient {
    fn new(org_id: String, base_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
            auth_token: None,
            org_id,
        }
    }

    fn with_auth(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    async fn create_organization(
        &self,
        org_token: &str,
        username: &str,
        password: &str,
    ) -> Result<(), String> {
        let body = serde_json::json!({
            "id": &self.org_id,
            "description": format!("Organization {}", &self.org_id),
            "super_admins": [
                {
                    "username": username,
                    "password": password,
                    "description": null
                }
            ]
        });

        let response = self
            .client
            .post(format!("{}/api/v1/new", self.base_url))
            .header("Authorization", format!("Bearer {}", org_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create organization request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Create organization failed ({}): {}", status, text));
        }

        Ok(())
    }

    async fn login(&self, username: &str, password: &str) -> Result<String, String> {
        let response = self
            .client
            .post(format!("{}/api/v1/auth/login", self.base_url))
            .basic_auth(username, Some(password))
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Login request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Login failed ({}): {}", status, text));
        }

        let text = response.text().await.unwrap_or_default();
        let resp: LoginResponse = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse login response: {}", e))?;

        Ok(resp.token)
    }

    async fn grant_endpoint_data_access(
        &self,
        endpoint_id: &str,
        subject: &str,
        perms: &str,
    ) -> Result<(), String> {
        let response = self
            .client
            .put(format!(
                "{}/api/v1/iam/data/endpoints/{}/subjects/{}",
                self.base_url,
                encode_path_segment(endpoint_id),
                encode_path_segment(subject)
            ))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({ "perms": perms }))
            .send()
            .await
            .map_err(|e| format!("Grant endpoint data access request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Grant endpoint data access failed ({}): {}",
                status, text
            ));
        }

        Ok(())
    }

    async fn create_endpoint(
        &self,
        endpoint_id: &str,
        host: &str,
        port: u16,
        tls: bool,
        password: Option<&str>,
    ) -> Result<EndpointResponseData, String> {
        let body = serde_json::json!({
            "endpoint": endpoint_id,
            "kind": "redis",
            "config": {
                "read_conn": null,
                "write_conn": {
                    "host": host,
                    "port": port,
                    "tls": tls,
                    "password": password
                }
            },
            "description": format!("Redis endpoint at {}:{}", host, port)
        });

        let response = self
            .client
            .post(format!("{}/api/v1/endpoints", self.base_url))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create endpoint failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Create endpoint failed ({}): {}", status, text));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text).map_err(|e| format!("Failed to parse endpoint response: {}", e))
    }

    async fn create_interlay(
        &self,
        interlay_id: &str,
        endpoint_uuid: &str,
        port: u16,
    ) -> Result<InterlayResponseData, String> {
        let body = serde_json::json!({
            "id": interlay_id,
            "endpoint": endpoint_uuid,
            "port": port,
            "settings": {},
            "tls": false
        });

        let response = self
            .client
            .post(format!("{}/api/v1/interlays", self.base_url))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create interlay failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Create interlay failed ({}): {}", status, text));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text).map_err(|e| format!("Failed to parse interlay response: {}", e))
    }

    async fn create_migration(
        &self,
        migration_id: &str,
        mode: MigrationMode,
        canary_state: &CanaryState,
    ) -> Result<MigrationResponseData, String> {
        let body = match mode {
            MigrationMode::BigBang => serde_json::json!({
                "id": migration_id,
                "description": "Redis big bang migration",
                "strategy": {"type": "big_bang", "durability": true},
                "data": null,
                "failure_handling": null
            }),
            MigrationMode::Canary => serde_json::json!({
                "id": migration_id,
                "description": "Redis canary migration",
                "strategy": {
                    "type": "canary",
                    "read_percentage": canary_state.read_percentage,
                    "write_mode": {
                        "mode": "dual_write",
                        "policy": canary_state.write_policy
                    }
                },
                "data": null,
                "failure_handling": null
            }),
            MigrationMode::BlueGreen => serde_json::json!({
                "id": migration_id,
                "description": "Redis blue-green migration",
                "strategy": {
                    "type": "blue_green",
                    "active_is_new": false,
                    "write_mode": {
                        "mode": "dual_write",
                        "policy": "LastWriteWins"
                    }
                },
                "data": null,
                "failure_handling": null
            }),
        };

        let url = format!("{}/api/v1/migrations", self.base_url);
        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create migration request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Create migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        // Parse as Value first to handle different response formats
        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse migration response: {}", e))?;

        // Try to extract id and uuid from different possible response structures
        let id = json
            .get("id")
            .or_else(|| json.get("data").and_then(|d| d.get("id")))
            .and_then(|v| v.as_str())
            .unwrap_or(migration_id)
            .to_string();

        let uuid = json
            .get("uuid")
            .or_else(|| json.get("data").and_then(|d| d.get("uuid")))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| id.clone());

        let status = json
            .get("status")
            .or_else(|| json.get("data").and_then(|d| d.get("status")))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(MigrationResponseData { id, uuid, status })
    }

    async fn add_interlay_to_migration(
        &self,
        migration_id: &str,
        interlay_id: &str,
        dest_endpoint_id: &str,
        mode: MigrationMode,
        canary_state: &CanaryState,
    ) -> Result<(), String> {
        let body = match mode {
            MigrationMode::BigBang => serde_json::json!({
                "id": format!("{}_relay", migration_id),
                "endpoint": dest_endpoint_id,
                "description": "Migration interlay configuration",
                "migration_strategy": {
                    "type": "big_bang",
                    "durability": true
                },
                "migration_data": {
                    "Scan": {
                        "replace": "None"
                    }
                },
                "testing_validation": null,
                "migration_rules": {
                    "traffic": {
                        "read": "Replicated",
                        "write": "New"
                    },
                    "error": "DoNothing",
                    "rollback": "Ignore",
                    "completion": {
                        "milestone": "Immediate",
                        "require_manual_approval": false
                    }
                }
            }),
            MigrationMode::Canary => serde_json::json!({
                "id": format!("{}_relay", migration_id),
                "endpoint": dest_endpoint_id,
                "description": "Canary migration interlay configuration",
                "migration_strategy": {
                    "type": "canary",
                    "read_percentage": canary_state.read_percentage,
                    "write_mode": {
                        "mode": "dual_write",
                        "policy": canary_state.write_policy
                    }
                },
                "migration_data": {
                    "Scan": {
                        "replace": "None"
                    }
                },
                "testing_validation": null,
                "migration_rules": {
                    "traffic": {
                        "read": {
                            "Ratio": {
                                "strategy": {
                                    "Random": { "ratio": canary_state.read_percentage }
                                }
                            }
                        },
                        "write": {
                            "Replicated": {
                                "policy": canary_state.write_policy
                            }
                        }
                    },
                    "error": "DoNothing",
                    "rollback": "Ignore",
                    "completion": {
                        "milestone": {
                            "TotalRequests": 1000000
                        },
                        "require_manual_approval": false
                    }
                }
            }),
            MigrationMode::BlueGreen => serde_json::json!({
                "id": format!("{}_relay", migration_id),
                "endpoint": dest_endpoint_id,
                "description": "Blue-green migration interlay configuration",
                "migration_strategy": {
                    "type": "blue_green",
                    "active_is_new": false,
                    "write_mode": {
                        "mode": "dual_write",
                        "policy": "LastWriteWins"
                    }
                },
                "migration_data": {
                    "Scan": {
                        "replace": "None"
                    }
                },
                "testing_validation": null,
                "migration_rules": {
                    "traffic": {
                        "read": "Old",
                        "write": {
                            "Replicated": {
                                "policy": "LastWriteWins"
                            }
                        }
                    },
                    "error": "DoNothing",
                    "rollback": "Ignore",
                    "completion": {
                        "milestone": "Immediate",
                        "require_manual_approval": true
                    }
                }
            }),
        };

        let url = format!(
            "{}/api/v1/migrations/{}/interlay/{}",
            self.base_url, migration_id, interlay_id
        );
        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Add interlay request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Add interlay failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        Ok(())
    }

    /// Update canary traffic split percentage
    async fn update_traffic_split(
        &self,
        migration_id: &str,
        new_percentage: f64,
        reason: &str,
    ) -> Result<UpdateTrafficResponse, String> {
        let body = serde_json::json!({
            "read_percentage": new_percentage,
            "reason": reason
        });

        let url = format!(
            "{}/api/v1/migrations/{}/traffic",
            self.base_url, migration_id
        );
        let response = self
            .client
            .patch(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Update traffic split failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Update traffic split failed ({}) PATCH {}: {}",
                status, url, text
            ));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text).map_err(|e| format!("Failed to parse traffic update response: {}", e))
    }

    async fn trigger_migration(&self, migration_id: &str) -> Result<(), String> {
        let response = self
            .client
            .post(format!(
                "{}/api/v1/migrations/{}/migrate",
                self.base_url, migration_id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Trigger migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Trigger migration failed ({}): {}", status, text));
        }

        Ok(())
    }

    /// Manually complete a running migration
    async fn complete_migration(
        &self,
        migration_id: &str,
        reason: Option<&str>,
    ) -> Result<CompleteMigrationResponse, String> {
        let body = serde_json::json!({
            "reason": reason.unwrap_or("Manual completion from TUI")
        });

        let url = format!(
            "{}/api/v1/migrations/{}/complete",
            self.base_url, migration_id
        );
        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Complete migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Complete migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse complete migration response: {}", e))
    }

    /// Rollback a migration for a specific interlay
    async fn rollback_interlay(
        &self,
        migration_id: &str,
        interlay_id: &str,
        reason: Option<&str>,
    ) -> Result<RollbackInterlayResponse, String> {
        let body = serde_json::json!({
            "reason": reason.unwrap_or("Manual rollback from TUI"),
            "force": false,
            "preserve_config": true,
            "overwrite_on_reverse": false
        });

        let url = format!(
            "{}/api/v1/migrations/{}/interlay/{}/rollback",
            self.base_url, migration_id, interlay_id
        );
        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Rollback migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Rollback migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse rollback migration response: {}", e))
    }

    async fn pause_migration(
        &self,
        migration_id: &str,
        reason: Option<&str>,
    ) -> Result<PauseMigrationResponse, String> {
        let body = serde_json::json!({
            "reason": reason.unwrap_or("Manual pause from TUI")
        });

        let url = format!("{}/api/v1/migrations/{}/pause", self.base_url, migration_id);
        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Pause migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Pause migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse pause migration response: {}", e))
    }

    async fn resume_migration(
        &self,
        migration_id: &str,
        reason: Option<&str>,
    ) -> Result<ResumeMigrationResponse, String> {
        let body = serde_json::json!({
            "reason": reason.unwrap_or("Manual resume from TUI")
        });

        let url = format!(
            "{}/api/v1/migrations/{}/resume",
            self.base_url, migration_id
        );
        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Resume migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Resume migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse resume migration response: {}", e))
    }

    /// Toggle between blue and green environments in a blue-green migration
    async fn toggle_environment(
        &self,
        migration_id: &str,
        activate_new: bool,
        reason: Option<&str>,
    ) -> Result<ToggleEnvironmentResponse, String> {
        let body = serde_json::json!({
            "activate_new": activate_new,
            "reason": reason.unwrap_or("Manual toggle from TUI")
        });

        let url = format!(
            "{}/api/v1/migrations/{}/toggle",
            self.base_url, migration_id
        );
        let response = self
            .client
            .patch(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Toggle environment failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "Toggle environment failed ({}) PATCH {}: {}",
                status, url, text
            ));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse toggle environment response: {}", e))
    }

    async fn refresh_migration(&self, migration_id: &str) -> Result<MigrationResponseData, String> {
        // First call refresh endpoint
        let response = self
            .client
            .post(format!(
                "{}/api/v1/migrations/{}/refresh",
                self.base_url, migration_id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Refresh migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Refresh migration failed ({}): {}", status, text));
        }

        // Then get updated status
        self.get_migration(migration_id).await
    }

    async fn get_migration(&self, migration_id: &str) -> Result<MigrationResponseData, String> {
        let response = self
            .client
            .get(format!(
                "{}/api/v1/migrations/{}",
                self.base_url, migration_id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .header("X-Eden-Verbose", "true")
            .send()
            .await
            .map_err(|e| format!("Get migration failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Get migration failed ({}): {}", status, text));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text).map_err(|e| format!("Failed to parse migration response: {}", e))
    }

    async fn get_endpoint(&self, endpoint_id: &str) -> Result<EndpointResponseData, String> {
        let response = self
            .client
            .get(format!(
                "{}/api/v1/endpoints/{}",
                self.base_url, endpoint_id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Get endpoint failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Get endpoint failed ({}): {}", status, text));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text).map_err(|e| format!("Failed to parse endpoint response: {}", e))
    }

    async fn get_interlay(&self, interlay_id: &str) -> Result<InterlayResponseData, String> {
        let response = self
            .client
            .get(format!(
                "{}/api/v1/interlays/{}",
                self.base_url, interlay_id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", self.auth_token.as_ref().unwrap()),
            )
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Get interlay failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Get interlay failed ({}): {}", status, text));
        }

        let text = response.text().await.unwrap_or_default();
        parse_api_data(&text).map_err(|e| format!("Failed to parse interlay response: {}", e))
    }
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

fn parse_api_data<T: DeserializeOwned>(text: &str) -> Result<T, String> {
    let json = serde_json::from_str::<serde_json::Value>(text)
        .map_err(|e| format!("invalid JSON: {}", e))?;
    let payload = json.get("data").cloned().unwrap_or(json);
    serde_json::from_value(payload).map_err(|e| format!("schema mismatch: {}", e))
}

fn is_existing_resource_error(text: &str) -> bool {
    let normalized = text.to_ascii_lowercase();
    normalized.contains("409")
        || normalized.contains("already exists")
        || normalized.contains("conflict")
        || normalized.contains("already has an active migration")
        || normalized.contains("duplicate")
}

// ============================================
// Async Task Functions
// ============================================

async fn run_migration_setup(
    tx: mpsc::Sender<ApiEvent>,
    source_host: String,
    source_port: String,
    source_tls: bool,
    source_password: Option<String>,
    dest_host: String,
    dest_port: String,
    dest_tls: bool,
    dest_password: Option<String>,
    interlay_port: u16,
    org_token: String,
    admin_user: String,
    admin_pass: String,
    org_id: String,
    api_base: String,
    mode: MigrationMode,
    canary_state: CanaryState,
) {
    let client = EdenApiClient::new(org_id, api_base);

    // API call indices match the order in MigrationState::new()
    const CREATE_ORG: usize = 0;
    const LOGIN: usize = 1;
    const CREATE_SOURCE_EP: usize = 2;
    const CREATE_DEST_EP: usize = 3;
    const CREATE_INTERLAY: usize = 4;
    const CREATE_MIGRATION: usize = 5;
    const ADD_INTERLAY: usize = 6;

    // Step 1: Create organization (if it doesn't exist)
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingOrganization))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_ORG,
            status: ApiCallStatus::InProgress,
        })
        .await;

    match client
        .create_organization(&org_token, &admin_user, &admin_pass)
        .await
    {
        Ok(_) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_ORG,
                    status: ApiCallStatus::Success,
                })
                .await;
        }
        Err(e) => {
            // Check if it's an "already exists" type error
            if is_existing_resource_error(&e) {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_ORG,
                        status: ApiCallStatus::Skipped,
                    })
                    .await;
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_ORG,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    }

    // Step 2: Login
    let _ = tx.send(ApiEvent::SetupProgress(SetupStep::LoggingIn)).await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: LOGIN,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let token = match client.login(&admin_user, &admin_pass).await {
        Ok(t) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: LOGIN,
                    status: ApiCallStatus::Success,
                })
                .await;
            t
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: LOGIN,
                    status: ApiCallStatus::Failed(e.clone()),
                })
                .await;
            let _ = tx.send(ApiEvent::SetupFailed(e)).await;
            return;
        }
    };

    let client = client.with_auth(token.clone());

    // Step 3: Create source endpoint
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingSourceEndpoint))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_SOURCE_EP,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let source_ep_id = format!("redis_source_{}", source_port);
    let source_ep = match client
        .create_endpoint(
            &source_ep_id,
            &source_host,
            source_port.parse().unwrap_or(6379),
            source_tls,
            source_password.as_deref(),
        )
        .await
    {
        Ok(ep) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_SOURCE_EP,
                    status: ApiCallStatus::Success,
                })
                .await;
            ep
        }
        Err(e) => {
            // Check if it's an "already exists" type error
            if is_existing_resource_error(&e) {
                // Fetch the existing endpoint to get the real UUID
                match client.get_endpoint(&source_ep_id).await {
                    Ok(ep) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_SOURCE_EP,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        ep
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_SOURCE_EP,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_SOURCE_EP,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };
    if let Err(e) = client
        .grant_endpoint_data_access(&source_ep_id, &admin_user, "rwx")
        .await
    {
        let _ = tx
            .send(ApiEvent::ApiCallUpdate {
                index: CREATE_SOURCE_EP,
                status: ApiCallStatus::Failed(e.clone()),
            })
            .await;
        let _ = tx.send(ApiEvent::SetupFailed(e)).await;
        return;
    }

    // Step 4: Create destination endpoint
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingDestEndpoint))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_DEST_EP,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let dest_ep_id = format!("redis_dest_{}", dest_port);
    let dest_ep = match client
        .create_endpoint(
            &dest_ep_id,
            &dest_host,
            dest_port.parse().unwrap_or(6380),
            dest_tls,
            dest_password.as_deref(),
        )
        .await
    {
        Ok(ep) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_DEST_EP,
                    status: ApiCallStatus::Success,
                })
                .await;
            ep
        }
        Err(e) => {
            // Check if it's an "already exists" type error
            if is_existing_resource_error(&e) {
                // Fetch the existing endpoint to get the real UUID
                match client.get_endpoint(&dest_ep_id).await {
                    Ok(ep) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_DEST_EP,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        ep
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_DEST_EP,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_DEST_EP,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };
    if let Err(e) = client
        .grant_endpoint_data_access(&dest_ep_id, &admin_user, "rwx")
        .await
    {
        let _ = tx
            .send(ApiEvent::ApiCallUpdate {
                index: CREATE_DEST_EP,
                status: ApiCallStatus::Failed(e.clone()),
            })
            .await;
        let _ = tx.send(ApiEvent::SetupFailed(e)).await;
        return;
    }

    // Step 5: Create interlay
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingInterlay))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_INTERLAY,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let interlay_id = format!("redis_interlay_{}_{}", source_port, dest_port);
    let interlay = match client
        .create_interlay(&interlay_id, &source_ep.uuid, interlay_port)
        .await
    {
        Ok(il) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_INTERLAY,
                    status: ApiCallStatus::Success,
                })
                .await;
            il
        }
        Err(e) => {
            // Check if it's an "already exists" type error
            if is_existing_resource_error(&e) {
                // Fetch the existing interlay to get the real UUID
                match client.get_interlay(&interlay_id).await {
                    Ok(il) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_INTERLAY,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        il
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_INTERLAY,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_INTERLAY,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };

    // Step 6: Create migration
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingMigration))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_MIGRATION,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let mode_suffix = match mode {
        MigrationMode::BigBang => "bb",
        MigrationMode::Canary => "canary",
        MigrationMode::BlueGreen => "bg",
    };
    let migration_id = format!(
        "redis_migration_{}_{}_{}",
        source_port, dest_port, mode_suffix
    );
    let migration = match client
        .create_migration(&migration_id, mode, &canary_state)
        .await
    {
        Ok(m) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_MIGRATION,
                    status: ApiCallStatus::Success,
                })
                .await;
            m
        }
        Err(e) => {
            // Check if it's an "already exists" type error
            if is_existing_resource_error(&e) {
                // Fetch the existing migration to get the real UUID and current state
                let _ = tx
                    .send(ApiEvent::DebugLog(format!(
                        "Migration exists, fetching current state..."
                    )))
                    .await;
                match client.get_migration(&migration_id).await {
                    Ok(m) => {
                        let _ = tx
                            .send(ApiEvent::DebugLog(format!(
                                "Existing migration: id={}, status={:?}",
                                m.id, m.status
                            )))
                            .await;
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_MIGRATION,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        m
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_MIGRATION,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_MIGRATION,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };

    // Step 7: Add interlay to migration
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::AddingInterlay))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: ADD_INTERLAY,
            status: ApiCallStatus::InProgress,
        })
        .await;

    if let Err(e) = client
        .add_interlay_to_migration(
            &migration.id,
            &interlay.id,
            &dest_ep.id,
            mode,
            &canary_state,
        )
        .await
    {
        // Check if it's an "already exists" type error
        if is_existing_resource_error(&e) {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: ADD_INTERLAY,
                    status: ApiCallStatus::Skipped,
                })
                .await;
        } else {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: ADD_INTERLAY,
                    status: ApiCallStatus::Failed(e.clone()),
                })
                .await;
            let _ = tx.send(ApiEvent::SetupFailed(e)).await;
            return;
        }
    } else {
        let _ = tx
            .send(ApiEvent::ApiCallUpdate {
                index: ADD_INTERLAY,
                status: ApiCallStatus::Success,
            })
            .await;
    }

    // Setup complete
    let _ = tx
        .send(ApiEvent::SetupComplete {
            auth_token: token.clone(),
            source_endpoint_id: source_ep.id,
            dest_endpoint_id: dest_ep.id,
            interlay_id: interlay.id,
            migration_id: migration.id.clone(),
        })
        .await;
    let _ = tx.send(ApiEvent::SetupProgress(SetupStep::Ready)).await;

    // Collect status using get after setup completes - this syncs with actual system state
    let _ = tx
        .send(ApiEvent::DebugLog(
            "Fetching current migration status...".to_string(),
        ))
        .await;
    match client.get_migration(&migration.id).await {
        Ok(data) => {
            let status = parse_migration_status(data.status.as_deref());
            let _ = tx
                .send(ApiEvent::DebugLog(format!(
                    "Current migration status: {:?} (from API: {:?})",
                    status, data.status
                )))
                .await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Failed to fetch status: {}", e)))
                .await;
            // Fallback to status from create/get response
            let status = parse_migration_status(migration.status.as_deref());
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
    }
}

async fn trigger_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    match client.trigger_migration(&migration_id).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationTriggered).await;

            // Poll status every second until migration completes or fails
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                match client.get_migration(&migration_id).await {
                    Ok(data) => {
                        let status = parse_migration_status(data.status.as_deref());
                        let _ = tx
                            .send(ApiEvent::MigrationStatusUpdate {
                                status: status.clone(),
                                force: false,
                            })
                            .await;

                        // Stop polling when migration reaches a terminal state
                        match status {
                            MigrationStatus::Completed
                            | MigrationStatus::Failed
                            | MigrationStatus::RolledBack => break,
                            _ => {}
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(ApiEvent::MigrationError(e)).await;
                        break;
                    }
                }
            }
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::MigrationError(e)).await;
        }
    }
}

async fn refresh_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    // First call refresh endpoint to sync state
    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/refresh",
            migration_id
        )))
        .await;
    if let Err(e) = client.refresh_migration(&migration_id).await {
        let _ = tx
            .send(ApiEvent::DebugLog(format!("Refresh failed: {}", e)))
            .await;
        let _ = tx.send(ApiEvent::MigrationError(e)).await;
        return;
    }

    // Then collect status using get
    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "GET /migrations/{}",
            migration_id
        )))
        .await;
    match client.get_migration(&migration_id).await {
        Ok(data) => {
            let status = parse_migration_status(data.status.as_deref());
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Status: {:?}", status)))
                .await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Get failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationError(e)).await;
        }
    }
}

async fn update_traffic_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
    new_percentage: f64,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let reason = format!("Adjusting canary traffic to {:.0}%", new_percentage * 100.0);
    match client
        .update_traffic_split(&migration_id, new_percentage, &reason)
        .await
    {
        Ok(response) => {
            let _ = tx
                .send(ApiEvent::TrafficUpdated {
                    old_percentage: response.old_percentage,
                    new_percentage: response.new_percentage,
                })
                .await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::TrafficUpdateFailed(e)).await;
        }
    }
}

async fn complete_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    match client.complete_migration(&migration_id, None).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationCompleted).await;
            // Also send status update to sync the UI
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status: MigrationStatus::Completed,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::MigrationCompleteFailed(e)).await;
        }
    }
}

async fn toggle_environment_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
    current_active_is_new: bool,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    // Toggle to the opposite of current state
    let activate_new = !current_active_is_new;
    match client
        .toggle_environment(&migration_id, activate_new, None)
        .await
    {
        Ok(response) => {
            let _ = tx
                .send(ApiEvent::EnvironmentToggled {
                    previous_active: response.previous_active,
                    new_active: response.new_active,
                })
                .await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::EnvironmentToggleFailed(e)).await;
        }
    }
}

async fn rollback_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    interlay_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/interlay/{}/rollback",
            migration_id, interlay_id
        )))
        .await;

    match client
        .rollback_interlay(&migration_id, &interlay_id, None)
        .await
    {
        Ok(response) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!(
                    "Rollback response: status={}, interlay={}",
                    response.status, response.interlay_id
                )))
                .await;
            let _ = tx.send(ApiEvent::MigrationRolledBack).await;
            // Use the status from the API response (RollingBack if data movement needed, RolledBack if immediate)
            let status = parse_migration_status(Some(&response.status));
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Rollback failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationRollbackFailed(e)).await;
        }
    }
}

async fn pause_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/pause",
            migration_id
        )))
        .await;

    match client.pause_migration(&migration_id, None).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationPaused).await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status: MigrationStatus::Paused,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Pause failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationPauseFailed(e)).await;
        }
    }
}

async fn resume_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/resume",
            migration_id
        )))
        .await;

    match client.resume_migration(&migration_id, None).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationResumed).await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status: MigrationStatus::Running,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Resume failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationResumeFailed(e)).await;
        }
    }
}

// ============================================
// Application Config and State
// ============================================

struct Config {
    // TUI connection addresses (what we connect to locally)
    source_url: String,
    source_host: String,
    source_port: String,
    dest_url: String,
    dest_host: String,
    dest_port: String,
    // Eden API addresses (what Eden should connect to - may differ from TUI)
    eden_source_host: String,
    eden_source_port: String,
    eden_source_tls: bool,
    eden_source_password: Option<String>,
    eden_dest_host: String,
    eden_dest_port: String,
    eden_dest_tls: bool,
    eden_dest_password: Option<String>,
    api_base: String,
    org_id: String,
    org_token: String,
    admin_user: String,
    admin_pass: String,
    launcher_redis_url: String,
    interlay_port: u16,
    default_mode: MigrationMode,
    default_canary_read_pct: f64,
    enable_populator: bool,
    workload_stats_file: Option<String>,
    // OpenRouter AI model config
    openrouter_api_key: Option<String>,
    openrouter_model: Option<String>,
}

#[derive(Clone)]
struct DbStats {
    port: String,
    keys: i64,
    keys_delta: i64,
    ops_per_sec: i64,
    connected_clients: i64,
    unique_keys: Option<usize>,
    keys_history: Vec<(f64, f64)>,
    ops_history: Vec<(f64, f64)>,
    coverage: Option<f64>,
    status: DbStatus,
}

#[derive(Clone, PartialEq)]
enum DbStatus {
    Connected,
    Error,
}

impl DbStats {
    fn new(port: String) -> Self {
        Self {
            port,
            keys: 0,
            keys_delta: 0,
            ops_per_sec: 0,
            connected_clients: 0,
            unique_keys: None,
            keys_history: Vec::with_capacity(HISTORY_SIZE),
            ops_history: Vec::with_capacity(HISTORY_SIZE),
            coverage: None,
            status: DbStatus::Connected,
        }
    }

    fn push_history(&mut self, tick: u64) {
        let x = tick as f64;

        if self.keys_history.len() >= HISTORY_SIZE {
            self.keys_history.remove(0);
        }
        if self.ops_history.len() >= HISTORY_SIZE {
            self.ops_history.remove(0);
        }

        self.keys_history.push((x, self.keys.max(0) as f64));
        self.ops_history.push((x, self.ops_per_sec.max(0) as f64));
    }
}

struct App {
    clients: Vec<(String, Client)>,
    db_stats: Vec<DbStats>,
    config: Config,
    start_time: Instant,
    last_update: Instant,
    total_ticks: u64,
    coverage_countdown: u64,
    should_quit: bool,
    force_coverage: bool,
    show_ops: bool,
    show_workload: bool,
    show_populator: bool,
    show_debug: bool,
    debug_log: Vec<String>,
    workload_stats: Option<WorkloadStatsSnapshot>,
    populate_stats: Option<PopulateStatsSnapshot>,
    populate_launcher: PopulateLauncherState,
    populate_stats_file: Option<String>,
    // Migration fields
    migration_state: MigrationState,
    api_event_tx: mpsc::Sender<ApiEvent>,
    api_event_rx: mpsc::Receiver<ApiEvent>,
    runtime: tokio::runtime::Handle,
}

impl App {
    fn new_with_clients(
        config: Config,
        source_client: Client,
        dest_client: Client,
        api_event_tx: mpsc::Sender<ApiEvent>,
        api_event_rx: mpsc::Receiver<ApiEvent>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let clients = vec![
            (config.source_port.clone(), source_client),
            (config.dest_port.clone(), dest_client),
        ];

        let db_stats = clients
            .iter()
            .map(|(port, _)| DbStats::new(port.clone()))
            .collect();

        let api_base = config.api_base.clone();
        let org_id = config.org_id.clone();

        let populate_default_url = config.launcher_redis_url.clone();
        let default_mode = config.default_mode;
        let default_canary_read_pct = config.default_canary_read_pct;
        let migration_state = {
            let mut state = MigrationState::new(api_base, org_id);
            state.mode = default_mode;
            state.canary.read_percentage = default_canary_read_pct;
            state
        };

        Self {
            clients,
            db_stats,
            config,
            start_time: Instant::now(),
            last_update: Instant::now(),
            total_ticks: 0,
            coverage_countdown: 0, // Run immediately on first tick
            should_quit: false,
            force_coverage: false,
            show_ops: true,
            show_workload: false,
            show_populator: false,
            show_debug: false,
            debug_log: Vec::new(),
            workload_stats: None,
            populate_stats: None,
            populate_launcher: PopulateLauncherState::new(populate_default_url),
            populate_stats_file: env::var(POPULATE_STATS_FILE_ENV).ok(),
            migration_state,
            api_event_tx,
            api_event_rx,
            runtime,
        }
    }

    fn log_debug(&mut self, msg: String) {
        log::debug!("{}", msg);
        // Keep last 20 messages (reduced from 50)
        if self.debug_log.len() >= 20 {
            self.debug_log.remove(0);
        }
        self.debug_log.push(msg);
    }

    fn process_api_events(&mut self) {
        while let Ok(event) = self.api_event_rx.try_recv() {
            match event {
                ApiEvent::SetupProgress(step) => {
                    self.migration_state.setup_step = step;
                }
                ApiEvent::ApiCallUpdate { index, ref status } => {
                    // Only log final states (success/fail/skip), not in-progress
                    if !matches!(status, ApiCallStatus::InProgress | ApiCallStatus::Pending) {
                        let name = self
                            .migration_state
                            .api_calls
                            .get(index)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("Call {}", index));
                        match status {
                            ApiCallStatus::Success => {
                                self.log_debug(format!("{}: OK", name));
                            }
                            ApiCallStatus::Failed(e) => {
                                self.log_debug(format!("{}: FAIL - {}", name, e));
                            }
                            ApiCallStatus::Skipped => {
                                self.log_debug(format!("{}: skipped", name));
                            }
                            _ => {}
                        }
                    }
                    self.migration_state.update_api_call(index, status.clone());
                }
                ApiEvent::SetupComplete {
                    auth_token,
                    source_endpoint_id,
                    dest_endpoint_id,
                    interlay_id,
                    migration_id,
                } => {
                    self.log_debug("Setup complete".to_string());
                    self.migration_state.auth_token = Some(auth_token);
                    self.migration_state.source_endpoint_id = Some(source_endpoint_id);
                    self.migration_state.dest_endpoint_id = Some(dest_endpoint_id);
                    self.migration_state.interlay_id = Some(interlay_id);
                    self.migration_state.migration_id = Some(migration_id);
                    self.migration_state.setup_step = SetupStep::Ready;
                    // Don't set status here - let MigrationStatusUpdate handle it
                    // based on the actual state from the API
                    self.migration_state.last_error = None;
                }
                ApiEvent::SetupFailed(err) => {
                    self.log_debug(format!("Setup FAILED: {}", err));
                    self.migration_state.setup_step = SetupStep::Failed(err.clone());
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationTriggered => {
                    self.log_debug("Migration started".to_string());
                    self.migration_state.status = MigrationStatus::Running;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationStatusUpdate { ref status, force } => {
                    // Protect against stale API responses overwriting authoritative local state
                    // (unless force=true, which means explicit user action like refresh)
                    let current = &self.migration_state.status;

                    let should_skip = if force {
                        false // Explicit refresh always updates
                    } else {
                        // Terminal/protected states should not be overwritten by non-terminal states
                        let current_is_protected = matches!(
                            current,
                            MigrationStatus::Completed
                                | MigrationStatus::Failed
                                | MigrationStatus::RolledBack
                                | MigrationStatus::RollingBack
                                | MigrationStatus::Paused
                        );
                        let new_is_non_terminal = matches!(
                            status,
                            MigrationStatus::Pending
                                | MigrationStatus::Testing
                                | MigrationStatus::Ready
                                | MigrationStatus::Running
                        );

                        // Also don't downgrade Running to pre-running states
                        let is_pre_running = matches!(
                            status,
                            MigrationStatus::Pending
                                | MigrationStatus::Testing
                                | MigrationStatus::Ready
                        );
                        let running_downgrade =
                            *current == MigrationStatus::Running && is_pre_running;

                        (current_is_protected && new_is_non_terminal) || running_downgrade
                    };

                    if should_skip {
                        // Skip this update - keep current status (stale API response)
                        self.log_debug(format!(
                            "Ignoring stale status {:?} (current: {:?})",
                            status, current
                        ));
                    } else {
                        // Only log significant status changes
                        match status {
                            MigrationStatus::Completed => {
                                self.log_debug("Migration completed".to_string())
                            }
                            MigrationStatus::Failed => {
                                self.log_debug("Migration failed".to_string())
                            }
                            MigrationStatus::PartialFailure => {
                                self.log_debug("Migration partial failure".to_string())
                            }
                            MigrationStatus::RolledBack => {
                                self.log_debug("Migration rolled back".to_string())
                            }
                            MigrationStatus::RollingBack => {
                                self.log_debug("Migration rolling back".to_string())
                            }
                            _ => {} // Don't log pending/running repeatedly
                        }
                        self.migration_state.status = status.clone();
                    }
                }
                ApiEvent::MigrationError(err) => {
                    self.log_debug(format!("Error: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::TrafficUpdated {
                    old_percentage,
                    new_percentage,
                } => {
                    self.log_debug(format!(
                        "Traffic: {:.0}% → {:.0}%",
                        old_percentage * 100.0,
                        new_percentage * 100.0
                    ));
                    self.migration_state.canary.read_percentage = new_percentage;
                }
                ApiEvent::TrafficUpdateFailed(err) => {
                    self.log_debug(format!("Traffic update failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationCompleted => {
                    self.log_debug("Migration manually completed".to_string());
                    self.migration_state.status = MigrationStatus::Completed;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationCompleteFailed(err) => {
                    self.log_debug(format!("Complete failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationRolledBack => {
                    self.log_debug("Migration rollback initiated".to_string());
                    self.migration_state.status = MigrationStatus::RollingBack;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationRollbackFailed(err) => {
                    self.log_debug(format!("Rollback failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::EnvironmentToggled {
                    previous_active,
                    new_active,
                } => {
                    self.log_debug(format!(
                        "Environment toggled: {} → {}",
                        previous_active, new_active
                    ));
                    // Update the active state based on the new active environment
                    self.migration_state.active_is_new = new_active.to_lowercase().contains("new")
                        || new_active.to_lowercase().contains("green");
                    self.migration_state.last_error = None;
                }
                ApiEvent::EnvironmentToggleFailed(err) => {
                    self.log_debug(format!("Environment toggle failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationPaused => {
                    self.log_debug("Migration paused".to_string());
                    self.migration_state.status = MigrationStatus::Paused;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationPauseFailed(err) => {
                    self.log_debug(format!("Pause failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationResumed => {
                    self.log_debug("Migration resumed".to_string());
                    self.migration_state.status = MigrationStatus::Running;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationResumeFailed(err) => {
                    self.log_debug(format!("Resume failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::DebugLog(msg) => {
                    self.log_debug(msg);
                }
            }
        }
    }

    fn handle_migrate_key(&mut self) {
        if self.migration_state.can_migrate() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(trigger_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    fn handle_refresh_key(&mut self) {
        if self.migration_state.is_ready() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(refresh_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    fn handle_setup_key(&mut self) {
        // Only start setup if not already started
        if self.migration_state.setup_step == SetupStep::NotStarted {
            let tx = self.api_event_tx.clone();
            // Use Eden hosts/ports (may differ from TUI when running locally)
            let source_host = self.config.eden_source_host.clone();
            let source_port = self.config.eden_source_port.clone();
            let source_tls = self.config.eden_source_tls;
            let source_password = self.config.eden_source_password.clone();
            let dest_host = self.config.eden_dest_host.clone();
            let dest_port = self.config.eden_dest_port.clone();
            let dest_tls = self.config.eden_dest_tls;
            let dest_password = self.config.eden_dest_password.clone();
            let org_token = self.config.org_token.clone();
            let admin_user = self.config.admin_user.clone();
            let admin_pass = self.config.admin_pass.clone();
            let org_id = self.migration_state.org_id.clone();
            let api_base = self.migration_state.api_base.clone();
            let mode = self.migration_state.mode;
            let canary_state = self.migration_state.canary.clone();

            // Log what Eden is connecting to
            self.log_debug(format!(
                "Eden endpoints: {}:{} → {}:{} (interlay {})",
                source_host, source_port, dest_host, dest_port, self.config.interlay_port
            ));

            self.runtime.spawn(run_migration_setup(
                tx,
                source_host,
                source_port,
                source_tls,
                source_password,
                dest_host,
                dest_port,
                dest_tls,
                dest_password,
                self.config.interlay_port,
                org_token,
                admin_user,
                admin_pass,
                org_id,
                api_base,
                mode,
                canary_state,
            ));
        }
    }

    fn handle_toggle_mode(&mut self) {
        // Only allow toggling before setup starts
        if self.migration_state.setup_step == SetupStep::NotStarted {
            self.migration_state.mode = self.migration_state.mode.toggle();
            self.log_debug(format!("Mode: {}", self.migration_state.mode.name()));
        }
    }

    fn handle_complete_key(&mut self) {
        if self.migration_state.can_complete() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(complete_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    fn handle_toggle_environment(&mut self) {
        if self.migration_state.can_toggle_environment() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();
            let current_active_is_new = self.migration_state.active_is_new;

            self.runtime.spawn(toggle_environment_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
                current_active_is_new,
            ));
        }
    }

    fn handle_rollback_key(&mut self) {
        if self.migration_state.can_rollback() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let interlay_id = self.migration_state.interlay_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(rollback_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                interlay_id,
                api_base,
            ));
        }
    }

    fn handle_pause_key(&mut self) {
        if self.migration_state.can_pause() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(pause_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    fn handle_resume_key(&mut self) {
        if self.migration_state.can_resume() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(resume_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    fn handle_traffic_increase(&mut self) {
        if self.migration_state.can_update_traffic() {
            let new_percentage = (self.migration_state.canary.read_percentage + 0.05).min(1.0);
            self.update_canary_traffic(new_percentage);
        }
    }

    fn handle_traffic_decrease(&mut self) {
        if self.migration_state.can_update_traffic() {
            let new_percentage = (self.migration_state.canary.read_percentage - 0.05).max(0.0);
            self.update_canary_traffic(new_percentage);
        }
    }

    fn toggle_populator_view(&mut self) {
        if !self.config.enable_populator {
            self.log_debug("Populator launcher is disabled by configuration".to_string());
            return;
        }
        self.show_populator = !self.show_populator;
        if self.show_populator {
            self.show_workload = false;
        }
    }

    fn handle_populator_key(&mut self, key: KeyCode) -> bool {
        if !self.show_populator {
            return false;
        }

        match key {
            KeyCode::Up => {
                self.populate_launcher.move_prev();
                true
            }
            KeyCode::Down => {
                self.populate_launcher.move_next();
                true
            }
            KeyCode::Left => {
                match self.populate_launcher.selected_field {
                    3 => {
                        self.populate_launcher.data_type =
                            self.populate_launcher.data_type.previous();
                    }
                    8 => {
                        self.populate_launcher.clear = !self.populate_launcher.clear;
                    }
                    _ => {}
                }
                true
            }
            KeyCode::Right => {
                match self.populate_launcher.selected_field {
                    3 => {
                        self.populate_launcher.data_type = self.populate_launcher.data_type.next();
                    }
                    8 => {
                        self.populate_launcher.clear = !self.populate_launcher.clear;
                    }
                    _ => {}
                }
                true
            }
            KeyCode::Backspace => {
                self.handle_populator_backspace();
                true
            }
            KeyCode::Enter => {
                self.launch_populator();
                true
            }
            KeyCode::Char(' ') => {
                match self.populate_launcher.selected_field {
                    3 => {
                        self.populate_launcher.data_type = self.populate_launcher.data_type.next();
                    }
                    8 => {
                        self.populate_launcher.clear = !self.populate_launcher.clear;
                    }
                    _ => {}
                }
                true
            }
            KeyCode::Char(ch) => {
                self.handle_populator_char(ch);
                true
            }
            _ => false,
        }
    }

    fn handle_populator_backspace(&mut self) {
        match self.populate_launcher.selected_field {
            0 => {
                self.populate_launcher.url.pop();
            }
            1 => {
                self.populate_launcher.megabytes.pop();
            }
            2 => {
                self.populate_launcher.key_size.pop();
            }
            4 => {
                self.populate_launcher.prefix.pop();
            }
            5 => {
                self.populate_launcher.batch_size.pop();
            }
            6 => {
                self.populate_launcher.ttl.pop();
            }
            7 => {
                self.populate_launcher.elements_per_key.pop();
            }
            9 => {
                self.populate_launcher.pipes.pop();
            }
            10 => {
                self.populate_launcher.then_client_write_pct.pop();
            }
            11 => {
                self.populate_launcher.client_duration.pop();
            }
            12 => {
                self.populate_launcher.client_concurrency.pop();
            }
            _ => {}
        }
    }

    fn handle_populator_char(&mut self, ch: char) {
        if ch.is_control() {
            return;
        }

        match self.populate_launcher.selected_field {
            0 => self.populate_launcher.url.push(ch),
            1 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.megabytes.push(ch);
                }
            }
            2 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.key_size.push(ch);
                }
            }
            4 => self.populate_launcher.prefix.push(ch),
            5 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.batch_size.push(ch);
                }
            }
            6 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.ttl.push(ch);
                }
            }
            7 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.elements_per_key.push(ch);
                }
            }
            9 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.pipes.push(ch);
                }
            }
            10 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.then_client_write_pct.push(ch);
                }
            }
            11 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.client_duration.push(ch);
                }
            }
            12 => {
                if ch.is_ascii_digit() {
                    self.populate_launcher.client_concurrency.push(ch);
                }
            }
            _ => {}
        }
    }

    fn launch_populator(&mut self) {
        if self.populate_stats_file.is_none() {
            let path = unique_temp_file("redis-populate-stats");
            self.populate_stats_file = Some(path.display().to_string());
        }
        if self.config.workload_stats_file.is_none() {
            let path = unique_temp_file("redis-workload-stats");
            self.config.workload_stats_file = Some(path.display().to_string());
        }

        match build_populate_launch_command(
            &self.populate_launcher,
            self.populate_stats_file.as_deref(),
            self.config.workload_stats_file.as_deref(),
        ) {
            Ok(command) => {
                match launch_populator_terminal(
                    &self.populate_launcher,
                    self.populate_stats_file.as_deref().unwrap(),
                    self.config.workload_stats_file.as_deref(),
                ) {
                    Ok(()) => {
                        self.populate_launcher.status_message =
                            Some(format!("Launched populate: {}", command.summary));
                        self.log_debug(format!("Populate launched: {}", command.summary));
                    }
                    Err(err) => {
                        self.populate_launcher.status_message =
                            Some(format!("Launch failed: {}", err));
                        self.log_debug(format!("Populate launch failed: {}", err));
                    }
                }
            }
            Err(err) => {
                self.populate_launcher.status_message = Some(err);
            }
        }
    }

    fn update_canary_traffic(&mut self, new_percentage: f64) {
        let tx = self.api_event_tx.clone();
        let token = self.migration_state.auth_token.clone().unwrap();
        let org_id = self.migration_state.org_id.clone();
        let migration_id = self.migration_state.migration_id.clone().unwrap();
        let api_base = self.migration_state.api_base.clone();

        self.runtime.spawn(update_traffic_task(
            tx,
            token,
            org_id,
            migration_id,
            api_base,
            new_percentage,
        ));
    }

    fn update(&mut self) {
        self.total_ticks += 1;
        self.refresh_workload_stats();
        self.refresh_populate_stats();

        for (i, (_, client)) in self.clients.iter().enumerate() {
            let stats = &mut self.db_stats[i];
            let old_keys = stats.keys;

            match client.get_connection() {
                Ok(mut conn) => {
                    stats.status = DbStatus::Connected;

                    if let Ok(count) = redis::cmd("DBSIZE").query::<i64>(&mut conn) {
                        stats.keys = count;
                        stats.keys_delta = count - old_keys;
                    }

                    if let Ok(info) = redis::cmd("INFO").arg("stats").query::<String>(&mut conn) {
                        stats.ops_per_sec =
                            parse_info_field(&info, "instantaneous_ops_per_sec").unwrap_or(0);
                    }

                    if let Ok(info) = redis::cmd("INFO").arg("clients").query::<String>(&mut conn) {
                        stats.connected_clients =
                            parse_info_field(&info, "connected_clients").unwrap_or(0);
                    }
                }
                Err(_) => {
                    stats.status = DbStatus::Error;
                }
            }

            stats.push_history(self.total_ticks);
        }

        // Coverage check every 15 seconds
        if self.coverage_countdown > 0 {
            self.coverage_countdown -= 1;
        }

        if self.force_coverage || self.coverage_countdown == 0 {
            self.run_coverage_check();
            self.coverage_countdown = 15;
            self.force_coverage = false;
        }

        self.last_update = Instant::now();
    }

    fn refresh_workload_stats(&mut self) {
        let Some(path) = self.config.workload_stats_file.as_deref() else {
            self.workload_stats = None;
            return;
        };

        match fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str::<WorkloadStatsSnapshot>(&contents) {
                Ok(stats) => self.workload_stats = Some(stats),
                Err(err) => {
                    self.log_debug(format!("Failed to parse workload stats: {}", err));
                }
            },
            Err(_) => {
                self.workload_stats = None;
            }
        }
    }

    fn refresh_populate_stats(&mut self) {
        let Some(path) = self.populate_stats_file.as_deref() else {
            self.populate_stats = None;
            return;
        };

        match fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str::<PopulateStatsSnapshot>(&contents) {
                Ok(stats) => self.populate_stats = Some(stats),
                Err(err) => {
                    self.log_debug(format!("Failed to parse populate stats: {}", err));
                }
            },
            Err(_) => {
                self.populate_stats = None;
            }
        }
    }

    fn run_coverage_check(&mut self) {
        if self.clients.len() < 2 {
            return;
        }

        // Collect all key sets
        let key_sets: Vec<HashSet<String>> = self
            .clients
            .iter()
            .filter_map(|(_, client)| get_all_keys(client))
            .collect();

        if key_sets.len() != self.clients.len() {
            return; // Failed to get keys from all instances
        }

        // Union of all keys across all databases
        let all_keys: HashSet<&String> = key_sets.iter().flat_map(|s| s.iter()).collect();
        let total_unique = all_keys.len();

        // For each instance:
        // - unique = keys only in this instance (not in others)
        // - coverage = my_keys / total_unique
        for (i, stats) in self.db_stats.iter_mut().enumerate() {
            let my_keys = &key_sets[i];

            // Keys unique to this instance (not in any other)
            let my_unique = my_keys
                .iter()
                .filter(|k| {
                    key_sets
                        .iter()
                        .enumerate()
                        .all(|(j, other)| j == i || !other.contains(*k))
                })
                .count();

            stats.unique_keys = Some(my_unique);

            if total_unique > 0 {
                stats.coverage = Some((my_keys.len() as f64 / total_unique as f64) * 100.0);
            } else {
                stats.coverage = Some(100.0);
            }
        }
    }

    fn runtime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

fn get_all_keys(client: &Client) -> Option<HashSet<String>> {
    let mut conn = client.get_connection().ok()?;
    let mut keys = HashSet::new();
    let mut cursor: u64 = 0;

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(1000)
            .query(&mut conn)
            .ok()?;

        keys.extend(batch);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    Some(keys)
}

fn parse_info_field(info: &str, field: &str) -> Option<i64> {
    info.lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|val| val.trim().parse().ok())
}

const DEFAULT_REDIS_HOST: &str = "172.24.2.218";

fn parse_redis_arg(arg: &str) -> Option<RedisConnInfo> {
    let normalized = if arg.contains("://") {
        arg.to_string()
    } else if let Some(idx) = arg.rfind(':') {
        let host = &arg[..idx];
        let port = &arg[idx + 1..];
        if !host.is_empty() && port.parse::<u16>().is_ok() {
            format!("redis://{}", arg)
        } else {
            format!("redis://{}:{}", DEFAULT_REDIS_HOST, arg)
        }
    } else {
        format!("redis://{}:{}", DEFAULT_REDIS_HOST, arg)
    };

    let parsed = Url::parse(&normalized).ok()?;
    let tls = parsed.scheme() == "rediss";
    let host = parsed.host_str().unwrap_or("localhost").to_string();
    let port = parsed
        .port()
        .unwrap_or(if tls { 6380 } else { 6379 })
        .to_string();
    let password = parsed.password().map(|p| {
        urlencoding::decode(p)
            .unwrap_or_else(|_| p.into())
            .into_owned()
    });

    Some(RedisConnInfo {
        url: normalized,
        host,
        port,
        tls,
        password,
    })
}

fn signal_ready_file() {
    if let Ok(path) = env::var(OBSERVER_READY_FILE_ENV) {
        if let Err(err) = fs::write(&path, b"ready\n") {
            eprintln!(
                "Warning: failed to write observer ready file at {}: {}",
                path, err
            );
        }
    }
}

fn build_config(args: ObserveConfig) -> Option<Config> {
    let source_input = args.source_url.or(args.source)?;
    let dest_input = args.dest_url.or(args.dest)?;
    let source = parse_redis_arg(&source_input)?;
    let dest = parse_redis_arg(&dest_input)?;
    let api_base = args
        .api_url
        .or(args.api_endpoint)
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string());

    let eden_source = args
        .eden_source
        .or(args.eden_source_positional)
        .as_deref()
        .and_then(parse_redis_arg)
        .unwrap_or_else(|| source.clone());

    let eden_dest = args
        .eden_dest
        .or(args.eden_dest_positional)
        .as_deref()
        .and_then(parse_redis_arg)
        .unwrap_or_else(|| dest.clone());

    let launcher_redis_url = args.redis_url.unwrap_or_else(|| {
        format!(
            "redis://{}:{}",
            Url::parse(&api_base)
                .ok()
                .and_then(|url| url.host_str().map(|host| host.to_string()))
                .unwrap_or_else(|| "localhost".to_string()),
            args.interlay_port
        )
    });
    let workload_stats_file = env::var(WORKLOAD_STATS_FILE_ENV).ok();
    let openrouter_api_key = env::var("OPENROUTER_API_KEY").ok();
    let openrouter_model = env::var("OPENROUTER_MODEL").ok();

    Some(Config {
        source_url: source.url,
        source_host: source.host,
        source_port: source.port,
        dest_url: dest.url,
        dest_host: dest.host,
        dest_port: dest.port,
        eden_source_host: eden_source.host,
        eden_source_port: eden_source.port,
        eden_source_tls: eden_source.tls,
        eden_source_password: eden_source.password,
        eden_dest_host: eden_dest.host,
        eden_dest_port: eden_dest.port,
        eden_dest_tls: eden_dest.tls,
        eden_dest_password: eden_dest.password,
        api_base,
        org_id: args.org_id,
        org_token: args.org_token,
        admin_user: args.admin_user,
        admin_pass: args.admin_pass,
        launcher_redis_url,
        interlay_port: args.interlay_port,
        default_mode: args.mode,
        default_canary_read_pct: args.canary_read_pct,
        enable_populator: args.enable_populator,
        workload_stats_file,
        openrouter_api_key,
        openrouter_model,
    })
}

struct PopulateLaunchCommand {
    summary: String,
    args: Vec<String>,
}

fn parse_required_u64(name: &str, value: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{} must be a whole number", name))
}

fn parse_required_usize(name: &str, value: &str) -> Result<usize, String> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("{} must be a whole number", name))
}

fn build_populate_launch_command(
    state: &PopulateLauncherState,
    _populate_stats_file: Option<&str>,
    _workload_stats_file: Option<&str>,
) -> Result<PopulateLaunchCommand, String> {
    if state.url.trim().is_empty() {
        return Err("Populate URL cannot be empty".to_string());
    }

    let megabytes = parse_required_u64("MB", &state.megabytes)?;
    let key_size = parse_required_u64("Key Size", &state.key_size)?;
    let batch_size = parse_required_usize("Batch Size", &state.batch_size)?;
    let ttl = parse_required_u64("TTL", &state.ttl)?;
    let elements_per_key = parse_required_usize("Elements/Key", &state.elements_per_key)?;
    let pipes = parse_required_usize("Pipes", &state.pipes)?;
    let client_duration = parse_required_u64("Client Duration", &state.client_duration)?;
    let client_concurrency = parse_required_usize("Client Concurrency", &state.client_concurrency)?;
    let then_client_write_pct = if state.then_client_write_pct.trim().is_empty() {
        None
    } else {
        let pct = state
            .then_client_write_pct
            .trim()
            .parse::<u8>()
            .map_err(|_| "Then-client write % must be between 0 and 100".to_string())?;
        if pct > 100 {
            return Err("Then-client write % must be between 0 and 100".to_string());
        }
        Some(pct)
    };

    let mut args = vec![
        "populate".to_string(),
        "--url".to_string(),
        state.url.trim().to_string(),
        "--mb".to_string(),
        megabytes.to_string(),
        "--size".to_string(),
        key_size.to_string(),
        "--prefix".to_string(),
        state.prefix.clone(),
        "--batch-size".to_string(),
        batch_size.to_string(),
        "--ttl".to_string(),
        ttl.to_string(),
        "--elements-per-key".to_string(),
        elements_per_key.to_string(),
        "--pipes".to_string(),
        pipes.to_string(),
    ];

    match state.data_type {
        PopulateDataType::String => args.push("--string".to_string()),
        PopulateDataType::Json => args.push("--json".to_string()),
        PopulateDataType::Hash => args.push("--hash".to_string()),
        PopulateDataType::List => args.push("--list".to_string()),
        PopulateDataType::Set => args.push("--set".to_string()),
        PopulateDataType::SortedSet => args.push("--zset".to_string()),
        PopulateDataType::Mixed => args.push("--mixed".to_string()),
    }

    if state.clear {
        args.push("--clear".to_string());
    }

    if let Some(pct) = then_client_write_pct {
        args.push("--then-client".to_string());
        args.push(pct.to_string());
        args.push("--duration".to_string());
        args.push(client_duration.to_string());
        args.push("--client-concurrency".to_string());
        args.push(client_concurrency.to_string());
    }

    Ok(PopulateLaunchCommand {
        summary: format!(
            "{}MB {} to {}",
            megabytes,
            state.data_type.label(),
            state.url.trim()
        ),
        args,
    })
}

fn unique_temp_file(prefix: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    env::temp_dir().join(format!("{}-{}-{}.tmp", prefix, std::process::id(), millis))
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn sh_quote_path(path: &Path) -> String {
    sh_quote(&path.display().to_string())
}

#[cfg(target_os = "macos")]
fn applescript_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn ensure_exists(path: &Path, label: &str) -> anyhow::Result<()> {
    if path.exists() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Missing {} at {}", label, path.display()))
    }
}

#[cfg(target_os = "macos")]
fn launch_populator_terminal(
    state: &PopulateLauncherState,
    populate_stats_file: &str,
    workload_stats_file: Option<&str>,
) -> anyhow::Result<()> {
    let command =
        build_populate_launch_command(state, Some(populate_stats_file), workload_stats_file)
            .map_err(anyhow::Error::msg)?;
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest_path = manifest_dir.join("Cargo.toml");

    ensure_exists(&manifest_dir, "redis-migrator directory")?;
    ensure_exists(&manifest_path, "redis-migrator Cargo.toml")?;

    let workload_env = workload_stats_file
        .map(|path| format!(" REDIS_WORKLOAD_STATS_FILE={}", sh_quote(path)))
        .unwrap_or_default();
    let shell_args = command
        .args
        .iter()
        .map(|arg| sh_quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let shell_command = format!(
        "cd {} && REDIS_POPULATE_STATS_FILE={}{} cargo run --release --manifest-path {} -- {}",
        sh_quote_path(&manifest_dir),
        sh_quote(populate_stats_file),
        workload_env,
        sh_quote_path(&manifest_path),
        shell_args,
    );

    let status = Command::new("osascript")
        .arg("-e")
        .arg("tell application \"Terminal\" to activate")
        .arg("-e")
        .arg(format!(
            "tell application \"Terminal\" to do script \"{}\"",
            applescript_escape(&shell_command)
        ))
        .status()?;

    if !status.success() {
        return Err(anyhow::anyhow!(
            "osascript failed to open the populate window"
        ));
    }

    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn launch_populator_terminal(
    state: &PopulateLauncherState,
    populate_stats_file: &str,
    workload_stats_file: Option<&str>,
) -> anyhow::Result<()> {
    let command =
        build_populate_launch_command(state, Some(populate_stats_file), workload_stats_file)
            .map_err(anyhow::Error::msg)?;
    Err(anyhow::anyhow!(
        "Automatic populator launch is only implemented on macOS. Run manually with REDIS_POPULATE_STATS_FILE={}{} cargo run --manifest-path examples/redis-migrator/Cargo.toml -- {}",
        populate_stats_file,
        workload_stats_file
            .map(|path| format!(" REDIS_WORKLOAD_STATS_FILE={}", path))
            .unwrap_or_default(),
        command.args.join(" ")
    ))
}

fn coverage_color(pct: f64) -> Color {
    if pct >= 99.0 {
        Color::Green
    } else if pct >= 90.0 {
        Color::Yellow
    } else {
        Color::Red
    }
}

fn format_delta(delta: i64) -> (String, Color) {
    if delta > 0 {
        (format!("+{}", delta), Color::Green)
    } else if delta < 0 {
        (format!("{}", delta), Color::Red)
    } else {
        ("—".to_string(), Color::DarkGray)
    }
}

fn draw_db_table(f: &mut Frame, area: Rect, app: &App) {
    // Check if Eden ports differ from TUI ports
    let eden_ports_differ = app.config.eden_source_port != app.config.source_port
        || app.config.eden_dest_port != app.config.dest_port;

    let title_suffix = if eden_ports_differ {
        format!(
            " (TUI: {}+{} | Eden: {}+{}) ",
            app.config.source_port,
            app.config.dest_port,
            app.config.eden_source_port,
            app.config.eden_dest_port
        )
    } else {
        " Instances ".to_string()
    };

    let header = Row::new(vec![
        "port", "active", "keys", "Δ", "unique", "ops/s", "conn", "coverage",
    ])
    .style(Style::default().fg(Color::DarkGray))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .db_stats
        .iter()
        .enumerate()
        .map(|(i, stats)| {
            let status_color = if stats.status == DbStatus::Connected {
                Color::Cyan
            } else {
                Color::Red
            };

            let (delta_str, delta_color) = format_delta(stats.keys_delta);

            let unique_span = match stats.unique_keys {
                Some(n) => Span::styled(format!("{}", n), Style::default().fg(Color::White)),
                None => Span::styled("—", Style::default().fg(Color::DarkGray)),
            };

            let coverage_span = match stats.coverage {
                Some(pct) => Span::styled(
                    format!("{:.1}%", pct),
                    Style::default().fg(coverage_color(pct)),
                ),
                None => Span::styled("—", Style::default().fg(Color::DarkGray)),
            };

            // Determine if this endpoint is active in BlueGreen mode
            let is_active = if app.migration_state.mode == MigrationMode::BlueGreen
                && app.migration_state.status == MigrationStatus::Running
            {
                // i==0 is old/source, i==1 is new/dest
                // active_is_new==false means old is active, true means new is active
                (i == 0 && !app.migration_state.active_is_new)
                    || (i == 1 && app.migration_state.active_is_new)
            } else {
                false
            };

            let active_span = if is_active {
                Span::styled("●", Style::default().fg(Color::Green).bold())
            } else {
                Span::styled("○", Style::default().fg(Color::DarkGray))
            };

            Row::new(vec![
                Span::styled(
                    format!(":{}", stats.port),
                    Style::default().fg(status_color),
                ),
                active_span,
                Span::styled(format!("{}", stats.keys), Style::default().fg(Color::White)),
                Span::styled(delta_str, Style::default().fg(delta_color)),
                unique_span,
                Span::styled(
                    format!("{}", stats.ops_per_sec),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled(
                    format!("{}", stats.connected_clients),
                    Style::default().fg(Color::Magenta),
                ),
                coverage_span,
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(title_suffix)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    f.render_widget(table, area);
}

fn draw_keys_chart(f: &mut Frame, area: Rect, app: &App) {
    let colors = [Color::Cyan, Color::Yellow, Color::Green];

    // Calculate shared bounds - Y always starts at 0
    let max_val = app
        .db_stats
        .iter()
        .flat_map(|s| s.keys_history.iter().map(|(_, y)| *y))
        .fold(1.0_f64, f64::max);

    let y_max = max_val * 1.05;

    let x_min = app.total_ticks.saturating_sub(HISTORY_SIZE as u64) as f64;
    let x_max = app.total_ticks as f64;

    let datasets: Vec<Dataset> = app
        .db_stats
        .iter()
        .enumerate()
        .map(|(i, stats)| {
            Dataset::default()
                .name(format!(":{}", stats.port))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors[i % colors.len()]))
                .data(&stats.keys_history)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(" Keys (overlaid) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .x_axis(Axis::default().bounds([x_min, x_max]).labels(vec![
            Span::styled(
                format!("-{}s", HISTORY_SIZE),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("now", Style::default().fg(Color::DarkGray)),
        ]))
        .y_axis(Axis::default().bounds([0.0, y_max]).labels(vec![
            Span::styled("0", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", max_val as i64),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

    f.render_widget(chart, area);
}

fn draw_ops_chart(f: &mut Frame, area: Rect, app: &App) {
    let colors = [Color::Cyan, Color::Yellow, Color::Green];

    let all_values: Vec<f64> = app
        .db_stats
        .iter()
        .flat_map(|s| s.ops_history.iter().map(|(_, y)| *y))
        .collect();

    let max_val = all_values.iter().cloned().fold(1.0_f64, f64::max);

    let x_min = app.total_ticks.saturating_sub(HISTORY_SIZE as u64) as f64;
    let x_max = app.total_ticks as f64;

    let datasets: Vec<Dataset> = app
        .db_stats
        .iter()
        .enumerate()
        .map(|(i, stats)| {
            Dataset::default()
                .name(format!(":{}", stats.port))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors[i % colors.len()]))
                .data(&stats.ops_history)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(" Ops/sec ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .x_axis(Axis::default().bounds([x_min, x_max]).labels(vec![
            Span::styled(
                format!("-{}s", HISTORY_SIZE),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("now", Style::default().fg(Color::DarkGray)),
        ]))
        .y_axis(Axis::default().bounds([0.0, max_val * 1.1]).labels(vec![
            Span::styled("0", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", max_val as i64),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

    f.render_widget(chart, area);
}

fn draw_workload_panel(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .title(" Workload ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(stats) = app.workload_stats.as_ref() else {
        let empty = Paragraph::new(
            "No workload stats yet.\nStart via observe-client or set REDIS_WORKLOAD_STATS_FILE.",
        )
        .style(Style::default().fg(Color::DarkGray));
        f.render_widget(empty, inner);
        return;
    };

    let progress_pct = if stats.duration_secs > 0 {
        ((stats.elapsed_secs / stats.duration_secs as f64) * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(3),
            Constraint::Min(4),
        ])
        .split(inner);

    let status_color = match stats.status.as_str() {
        "running" => Color::Yellow,
        "completed" => Color::Green,
        _ => Color::White,
    };

    let details = vec![
        Line::from(vec![
            Span::styled("Status: ", Style::default().fg(Color::White)),
            Span::styled(
                stats.status.clone(),
                Style::default().fg(status_color).bold(),
            ),
            Span::styled("   Prefix: ", Style::default().fg(Color::White)),
            Span::styled(stats.key_prefix.clone(), Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::styled("Keys: ", Style::default().fg(Color::White)),
            Span::raw(format!("{}", stats.num_keys)),
            Span::styled("   Mix: ", Style::default().fg(Color::White)),
            Span::raw(format!(
                "{}% reads / {}% writes",
                100 - stats.write_pct,
                stats.write_pct
            )),
        ]),
        Line::from(vec![
            Span::styled("Workers: ", Style::default().fg(Color::White)),
            Span::raw(format!("{}", stats.concurrency)),
            Span::styled("   Value: ", Style::default().fg(Color::White)),
            Span::raw(format!("{} bytes", stats.value_size)),
        ]),
        Line::from(vec![
            Span::styled("Elapsed: ", Style::default().fg(Color::White)),
            Span::raw(format!("{:.1}s", stats.elapsed_secs)),
            Span::styled("   Duration: ", Style::default().fg(Color::White)),
            Span::raw(if stats.duration_secs > 0 {
                format!("{}s", stats.duration_secs)
            } else {
                "until interrupted".to_string()
            }),
        ]),
    ];
    f.render_widget(Paragraph::new(details), chunks[0]);

    let gauge_label = if stats.duration_secs > 0 {
        format!("{:.0}%", progress_pct)
    } else {
        format!("{:.0}s", stats.elapsed_secs)
    };
    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(Color::Cyan))
        .label(gauge_label)
        .percent(progress_pct.round() as u16);
    f.render_widget(gauge, chunks[1]);

    let totals = stats.total_reads + stats.total_writes;
    let metrics = vec![
        Line::from(format!(
            "ops/s {:.0}   reads/s {:.0}   writes/s {:.0}",
            stats.ops_per_sec, stats.reads_per_sec, stats.writes_per_sec
        )),
        Line::from(format!(
            "total ops {}   reads {}   writes {}   errors {}",
            totals, stats.total_reads, stats.total_writes, stats.total_errors
        )),
        Line::from(format!(
            "avg latency   read {:.0}us   write {:.0}us",
            stats.avg_read_latency_us, stats.avg_write_latency_us
        )),
    ];
    f.render_widget(Paragraph::new(metrics), chunks[2]);
}

fn draw_populator_panel(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .title(" Populator Launcher ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(15),
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(4),
        ])
        .split(inner);

    let fields = vec![
        ("URL", app.populate_launcher.url.clone()),
        ("MB", app.populate_launcher.megabytes.clone()),
        ("Key Size", app.populate_launcher.key_size.clone()),
        ("Type", app.populate_launcher.data_type.label().to_string()),
        ("Prefix", app.populate_launcher.prefix.clone()),
        ("Batch Size", app.populate_launcher.batch_size.clone()),
        ("TTL", app.populate_launcher.ttl.clone()),
        (
            "Elements/Key",
            app.populate_launcher.elements_per_key.clone(),
        ),
        (
            "Clear",
            if app.populate_launcher.clear {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("Pipes", app.populate_launcher.pipes.clone()),
        (
            "Then Client %",
            if app.populate_launcher.then_client_write_pct.is_empty() {
                "off".to_string()
            } else {
                app.populate_launcher.then_client_write_pct.clone()
            },
        ),
        (
            "Client Duration",
            app.populate_launcher.client_duration.clone(),
        ),
        (
            "Client Concurrency",
            app.populate_launcher.client_concurrency.clone(),
        ),
    ];

    let field_lines: Vec<Line> = fields
        .iter()
        .enumerate()
        .map(|(idx, (label, value))| {
            let selected = idx == app.populate_launcher.selected_field;
            let label_style = if selected {
                Style::default().fg(Color::Black).bg(Color::Green).bold()
            } else {
                Style::default().fg(Color::White)
            };
            let value_style = if selected {
                Style::default().fg(Color::Black).bg(Color::Green)
            } else {
                Style::default().fg(Color::Cyan)
            };

            Line::from(vec![
                Span::styled(format!("{:<18}", label), label_style),
                Span::styled(value.clone(), value_style),
            ])
        })
        .collect();
    f.render_widget(Paragraph::new(field_lines), chunks[0]);

    let status_text = app
        .populate_launcher
        .status_message
        .clone()
        .unwrap_or_else(|| {
            "Up/Down select, type to edit, Space toggles, Enter launches a new populate window."
                .to_string()
        });
    f.render_widget(
        Paragraph::new(status_text).style(Style::default().fg(Color::DarkGray)),
        chunks[1],
    );

    if let Some(stats) = app.populate_stats.as_ref() {
        let pct = if stats.total_bytes > 0 {
            ((stats.bytes_written as f64 / stats.total_bytes as f64) * 100.0).clamp(0.0, 100.0)
        } else {
            0.0
        };
        let gauge = Gauge::default()
            .gauge_style(Style::default().fg(Color::Green))
            .label(format!("{:.0}%  {:.1} MB/s", pct, stats.mb_per_sec))
            .percent(pct.round() as u16);
        f.render_widget(gauge, chunks[2]);

        let details = vec![
            Line::from(format!(
                "status {}   type {}   target {}MB   key size {}",
                stats.status, stats.data_type, stats.megabytes, stats.key_size
            )),
            Line::from(format!(
                "written {} / {} bytes   keys {} / {}   url {}",
                stats.bytes_written,
                stats.total_bytes,
                stats.written_keys,
                stats.target_keys,
                stats.url
            )),
            Line::from(format!(
                "elapsed {:.1}s   {:.0} keys/s   prefix {}   clear {}",
                stats.elapsed_secs,
                stats.keys_per_sec,
                stats.key_prefix,
                if stats.clear { "yes" } else { "no" }
            )),
            Line::from(format!(
                "batch {}   ttl {}   elems {}   pipes {}   then-client {}   dur {}s   conc {}",
                stats.batch_size,
                stats.ttl,
                stats.elements_per_key,
                stats.pipes,
                stats
                    .then_client_write_pct
                    .map(|pct| format!("{}%", pct))
                    .unwrap_or_else(|| "off".to_string()),
                stats.client_duration_secs,
                stats.client_concurrency
            )),
        ];
        f.render_widget(Paragraph::new(details), chunks[3]);
    } else {
        f.render_widget(
            Paragraph::new(
                "No populate run yet.\nLaunch one here and this panel will track bytes, keys, and throughput.",
            )
            .style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }
}

fn draw_debug_panel(f: &mut Frame, area: Rect, app: &App) {
    let state = &app.migration_state;

    // Build state summary line
    let status_color = match state.status {
        MigrationStatus::Running => Color::Yellow,
        MigrationStatus::Completed => Color::Green,
        MigrationStatus::Failed | MigrationStatus::PartialFailure => Color::Red,
        MigrationStatus::RollingBack => Color::Cyan,
        MigrationStatus::RolledBack => Color::Blue,
        _ => Color::White,
    };

    let state_line = Line::from(vec![
        Span::styled("State: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:?}", state.status),
            Style::default().fg(status_color).bold(),
        ),
        Span::styled(" | Mode: ", Style::default().fg(Color::DarkGray)),
        Span::styled(state.mode.name(), Style::default().fg(Color::Cyan)),
        Span::styled(" | Setup: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if state.is_ready() {
                "Ready"
            } else {
                "Not Ready"
            },
            Style::default().fg(if state.is_ready() {
                Color::Green
            } else {
                Color::Yellow
            }),
        ),
        Span::styled(" | Interlay: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if state.interlay_id.is_some() {
                "Yes"
            } else {
                "No"
            },
            Style::default().fg(if state.interlay_id.is_some() {
                Color::Green
            } else {
                Color::Red
            }),
        ),
        Span::styled(" | Rollback: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if state.can_rollback() {
                "Available"
            } else {
                "N/A"
            },
            Style::default().fg(if state.can_rollback() {
                Color::Magenta
            } else {
                Color::DarkGray
            }),
        ),
    ]);

    // Build log lines
    let log_lines: Vec<Line> = app
        .debug_log
        .iter()
        .rev()
        .take(area.height.saturating_sub(4) as usize)
        .rev()
        .map(|msg| {
            let color = if msg.contains("FAIL") || msg.contains("Error") || msg.contains("error") {
                Color::Red
            } else if msg.contains("OK") || msg.contains("complete") || msg.contains("Complete") {
                Color::Green
            } else if msg.contains("skipped") || msg.contains("Skipped") {
                Color::Cyan
            } else if msg.contains("started")
                || msg.contains("Started")
                || msg.contains("initiated")
            {
                Color::Yellow
            } else {
                Color::White
            };
            Line::from(Span::styled(
                format!("  {}", msg),
                Style::default().fg(color),
            ))
        })
        .collect();

    // Combine state line and log lines
    let mut all_lines = vec![state_line, Line::from("")];
    all_lines.extend(log_lines);

    let paragraph = Paragraph::new(all_lines).block(
        Block::default()
            .title(" Debug ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    f.render_widget(paragraph, area);
}

fn draw_api_panel(f: &mut Frame, area: Rect, app: &App) {
    let state = &app.migration_state;

    let mut lines = vec![];

    // Mode selector with tab indicator
    let mode_color = match state.mode {
        MigrationMode::BigBang => Color::Cyan,
        MigrationMode::Canary => Color::Yellow,
        MigrationMode::BlueGreen => Color::Blue,
    };
    let mode_can_change = state.setup_step == SetupStep::NotStarted;
    lines.push(Line::from(vec![
        Span::styled("Mode: ", Style::default().fg(Color::White)),
        Span::styled(state.mode.name(), Style::default().fg(mode_color).bold()),
        if mode_can_change {
            Span::styled(" (Tab)", Style::default().fg(Color::DarkGray))
        } else {
            Span::styled("", Style::default())
        },
    ]));
    lines.push(Line::from(vec![
        Span::styled("Interlay: ", Style::default().fg(Color::White)),
        Span::styled(
            app.config.interlay_port.to_string(),
            Style::default().fg(Color::Green).bold(),
        ),
    ]));

    // Show canary percentage if in canary mode
    if state.mode == MigrationMode::Canary {
        let pct = state.canary.read_percentage * 100.0;
        let pct_color = if pct >= 75.0 {
            Color::Green
        } else if pct >= 25.0 {
            Color::Yellow
        } else {
            Color::Cyan
        };
        lines.push(Line::from(vec![
            Span::styled("Traffic: ", Style::default().fg(Color::White)),
            Span::styled(
                format!("{:.0}%", pct),
                Style::default().fg(pct_color).bold(),
            ),
            Span::styled(" to new", Style::default().fg(Color::DarkGray)),
            if state.can_update_traffic() {
                Span::styled(" (+/-)", Style::default().fg(Color::DarkGray))
            } else {
                Span::styled("", Style::default())
            },
        ]));
    }

    // Show active environment if in BlueGreen mode
    if state.mode == MigrationMode::BlueGreen && state.status == MigrationStatus::Running {
        let (active_env, env_color) = if state.active_is_new {
            ("New (Green)", Color::Green)
        } else {
            ("Old (Blue)", Color::Blue)
        };
        lines.push(Line::from(vec![
            Span::styled("Active: ", Style::default().fg(Color::White)),
            Span::styled(active_env, Style::default().fg(env_color).bold()),
            if state.can_toggle_environment() {
                Span::styled(" (t)", Style::default().fg(Color::DarkGray))
            } else {
                Span::styled("", Style::default())
            },
        ]));
    }
    lines.push(Line::from(""));

    // Header
    lines.push(Line::from(Span::styled(
        "API Calls",
        Style::default().fg(Color::White).bold(),
    )));

    // Show hint if setup hasn't started
    if state.setup_step == SetupStep::NotStarted {
        lines.push(Line::from(Span::styled(
            "Press 's' to start setup",
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::from(""));
    }

    // API call list with status indicators
    for call in &state.api_calls {
        let (icon, color) = match &call.status {
            ApiCallStatus::Pending => ("○", Color::DarkGray),
            ApiCallStatus::InProgress => ("◐", Color::Yellow),
            ApiCallStatus::Success => ("●", Color::Green),
            ApiCallStatus::Failed(_) => ("✗", Color::Red),
            ApiCallStatus::Skipped => ("–", Color::Cyan),
        };

        let status_text = match &call.status {
            ApiCallStatus::Failed(msg) => format!(" {}", msg),
            _ => String::new(),
        };

        lines.push(Line::from(vec![
            Span::styled(format!("{} ", icon), Style::default().fg(color)),
            Span::styled(&call.name, Style::default().fg(color)),
            Span::styled(status_text, Style::default().fg(Color::Red)),
        ]));
    }

    // Add migration status at bottom
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("Status: ", Style::default().fg(Color::White)),
        match &state.status {
            MigrationStatus::NotSetup => {
                Span::styled("Not configured", Style::default().fg(Color::DarkGray))
            }
            MigrationStatus::Pending => {
                Span::styled("Pending", Style::default().fg(Color::DarkGray))
            }
            MigrationStatus::Testing => {
                Span::styled("Testing...", Style::default().fg(Color::Yellow))
            }
            MigrationStatus::Ready => {
                Span::styled("Ready to migrate", Style::default().fg(Color::Cyan))
            }
            MigrationStatus::Running => {
                Span::styled("Running...", Style::default().fg(Color::Yellow))
            }
            MigrationStatus::PartialFailure => {
                Span::styled("Partial failure", Style::default().fg(Color::Red))
            }
            MigrationStatus::Failed => Span::styled("Failed", Style::default().fg(Color::Red)),
            MigrationStatus::Paused => Span::styled("Paused", Style::default().fg(Color::Yellow)),
            MigrationStatus::Completed => {
                Span::styled("Completed", Style::default().fg(Color::Green))
            }
            MigrationStatus::RollingBack => {
                Span::styled("Rolling back...", Style::default().fg(Color::Yellow))
            }
            MigrationStatus::RolledBack => {
                Span::styled("Rolled back", Style::default().fg(Color::Magenta))
            }
        },
    ]));

    // Migration ID if available
    if let Some(ref id) = state.migration_id {
        lines.push(Line::from(vec![
            Span::styled("ID: ", Style::default().fg(Color::DarkGray)),
            Span::styled(id.clone(), Style::default().fg(Color::DarkGray)),
        ]));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(" Migration Setup ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );

    f.render_widget(paragraph, area);
}

fn draw_ui(f: &mut Frame, app: &App) {
    // Main vertical split for debug panel
    let main_area = if app.show_debug {
        let vertical_split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(15),    // Main content
                Constraint::Length(12), // Debug panel
            ])
            .split(f.area());
        draw_debug_panel(f, vertical_split[1], app);
        vertical_split[0]
    } else {
        f.area()
    };

    // Main horizontal split: left panel (API status) | right panel (everything else)
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(32), // Left panel for API status
            Constraint::Min(50),    // Right panel for stats/charts
        ])
        .split(main_area);

    // Left panel - API call status
    draw_api_panel(f, horizontal_chunks[0], app);

    // Right panel layout
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Title bar
            Constraint::Length(6), // Stats table
            Constraint::Min(8),    // Charts
            Constraint::Length(1), // Status bar
        ])
        .split(horizontal_chunks[1]);

    // Title bar
    let runtime = app.runtime();
    let title = Line::from(vec![
        Span::styled(" redis-monitor ", Style::default().fg(Color::Cyan).bold()),
        Span::styled(
            format!(
                "{}:{:02}:{:02}",
                runtime.as_secs() / 3600,
                (runtime.as_secs() % 3600) / 60,
                runtime.as_secs() % 60
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(title), right_chunks[0]);

    // Stats table
    draw_db_table(f, right_chunks[1], app);

    if app.show_populator {
        draw_populator_panel(f, right_chunks[2], app);
    } else if app.show_workload {
        draw_workload_panel(f, right_chunks[2], app);
    } else {
        // Charts - overlaid view
        let chart_constraints = if app.show_ops {
            vec![Constraint::Percentage(50), Constraint::Percentage(50)]
        } else {
            vec![Constraint::Percentage(100)]
        };

        let chart_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(chart_constraints)
            .split(right_chunks[2]);

        draw_keys_chart(f, chart_chunks[0], app);

        if app.show_ops && chart_chunks.len() > 1 {
            draw_ops_chart(f, chart_chunks[1], app);
        }
    }

    // Status bar with migration keys
    let mut status_spans = vec![
        Span::styled(" q", Style::default().fg(Color::White)),
        Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("Tab", Style::default().fg(Color::White)),
        Span::styled(" mode  ", Style::default().fg(Color::DarkGray)),
        Span::styled("s", Style::default().fg(Color::White)),
        Span::styled(" setup  ", Style::default().fg(Color::DarkGray)),
        Span::styled("m", Style::default().fg(Color::White)),
        Span::styled(" migrate  ", Style::default().fg(Color::DarkGray)),
    ];

    if app.config.enable_populator {
        status_spans.push(Span::styled(
            if app.show_populator { "esc" } else { "o" },
            Style::default().fg(Color::Green),
        ));
        status_spans.push(Span::styled(
            if app.show_populator {
                " close launcher  "
            } else {
                " populate  "
            },
            Style::default().fg(Color::DarkGray),
        ));
    }

    if app.config.workload_stats_file.is_some() {
        status_spans.push(Span::styled("w", Style::default().fg(Color::Cyan)));
        status_spans.push(Span::styled(
            if app.show_workload {
                " db view  "
            } else {
                " workload  "
            },
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show +/- for canary traffic control when applicable
    if app.migration_state.mode == MigrationMode::Canary && app.migration_state.can_update_traffic()
    {
        status_spans.push(Span::styled("+/-", Style::default().fg(Color::Yellow)));
        status_spans.push(Span::styled(
            " traffic  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show toggle for blue-green environment switching when applicable
    if app.migration_state.can_toggle_environment() {
        status_spans.push(Span::styled("t", Style::default().fg(Color::Cyan)));
        status_spans.push(Span::styled(
            " toggle  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show complete when migration is running
    if app.migration_state.can_complete() {
        status_spans.push(Span::styled("c", Style::default().fg(Color::Green)));
        status_spans.push(Span::styled(
            " complete  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show pause when migration is running
    if app.migration_state.can_pause() {
        status_spans.push(Span::styled("p", Style::default().fg(Color::Yellow)));
        status_spans.push(Span::styled(
            " pause  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show resume when migration is paused
    if app.migration_state.can_resume() {
        status_spans.push(Span::styled("p", Style::default().fg(Color::Cyan)));
        status_spans.push(Span::styled(
            " resume  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show rollback when migration is completed, failed, or paused
    if app.migration_state.can_rollback() {
        status_spans.push(Span::styled("b", Style::default().fg(Color::Magenta)));
        status_spans.push(Span::styled(
            " rollback  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show refresh/retry - highlight when completed to indicate retry is available
    let can_retry = app.migration_state.status == MigrationStatus::Completed;
    if can_retry {
        status_spans.push(Span::styled("r", Style::default().fg(Color::Yellow)));
        status_spans.push(Span::styled(
            " retry  ",
            Style::default().fg(Color::DarkGray),
        ));
    } else {
        status_spans.push(Span::styled("r", Style::default().fg(Color::White)));
        status_spans.push(Span::styled(
            " refresh  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    status_spans.extend(vec![
        Span::styled("d", Style::default().fg(Color::White)),
        Span::styled(
            if app.show_debug { " debug" } else { " debug" },
            Style::default().fg(Color::DarkGray),
        ),
    ]);

    if app.show_populator {
        status_spans.extend(vec![
            Span::styled("  ↑↓", Style::default().fg(Color::Green)),
            Span::styled(" field  ", Style::default().fg(Color::DarkGray)),
            Span::styled("enter", Style::default().fg(Color::Green)),
            Span::styled(" launch", Style::default().fg(Color::DarkGray)),
        ]);
    }

    let status = Line::from(status_spans);
    f.render_widget(Paragraph::new(status), right_chunks[3]);
}

fn check_redis_connection(
    label: &str,
    url: &str,
    host: &str,
    port: &str,
) -> Result<Client, String> {
    println!("Connecting to {} Redis at {}:{}...", label, host, port);

    let client =
        Client::open(url).map_err(|e| format!("Failed to create {} Redis client: {}", label, e))?;

    let mut conn = client.get_connection().map_err(|e| {
        format!(
            "Failed to connect to {} Redis at {}:{}: {}",
            label, host, port, e
        )
    })?;

    let _: String = redis::cmd("PING")
        .query(&mut conn)
        .map_err(|e| format!("Failed to ping {} Redis at {}:{}: {}", label, host, port, e))?;

    log::info!("  Connected to {} Redis", label);
    Ok(client)
}

fn init_logging() -> anyhow::Result<()> {
    // Clear the log file if it exists
    let log_file = "redis-migrator.log";
    if std::path::Path::new(log_file).exists() {
        std::fs::remove_file(log_file)?;
    }

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] {}: {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(fern::log_file(log_file)?)
        .apply()?;
    Ok(())
}

pub fn run(args: ObserveConfig) -> anyhow::Result<()> {
    // Load .env file if present (ignore if missing)
    let _ = dotenvy::dotenv();

    init_logging()?;
    let config = build_config(args)
        .ok_or_else(|| anyhow::anyhow!("Invalid Redis observer configuration"))?;

    // Health check: verify Redis connections BEFORE entering TUI
    log::info!("Checking Redis connections...");
    let source_client = match check_redis_connection(
        "source",
        &config.source_url,
        &config.source_host,
        &config.source_port,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    let dest_client = match check_redis_connection(
        "dest",
        &config.dest_url,
        &config.dest_host,
        &config.dest_port,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    signal_ready_file();
    log::info!("All connections verified. Starting TUI...");

    // Create tokio runtime for async API calls
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    // Create channel for API events
    let (tx, rx) = mpsc::channel::<ApiEvent>(100);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new_with_clients(
        config,
        source_client,
        dest_client,
        tx,
        rx,
        runtime.handle().clone(),
    );

    let tick_rate = Duration::from_secs(1);

    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        // Check for API events (non-blocking)
        app.process_api_events();

        let timeout = tick_rate.saturating_sub(app.last_update.elapsed());

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => app.should_quit = true,
                        KeyCode::Esc => {
                            if app.show_populator {
                                app.show_populator = false;
                            } else {
                                app.should_quit = true;
                            }
                        }
                        _ if app.handle_populator_key(key.code) => {}
                        KeyCode::Char('c') => app.handle_complete_key(),
                        KeyCode::Char('b') => app.handle_rollback_key(),
                        KeyCode::Char('p') => {
                            if app.migration_state.can_pause() {
                                app.handle_pause_key();
                            } else if app.migration_state.can_resume() {
                                app.handle_resume_key();
                            }
                        }
                        KeyCode::Char('t') => app.handle_toggle_environment(),
                        KeyCode::Char('f') => app.force_coverage = true,
                        KeyCode::Char('v') => app.show_ops = !app.show_ops,
                        KeyCode::Char('o') => app.toggle_populator_view(),
                        KeyCode::Char('w') => {
                            if app.config.workload_stats_file.is_some() {
                                app.show_workload = !app.show_workload;
                                if app.show_workload {
                                    app.show_populator = false;
                                }
                            }
                        }
                        KeyCode::Char('d') => app.show_debug = !app.show_debug,
                        KeyCode::Tab => app.handle_toggle_mode(),
                        KeyCode::Char('s') => app.handle_setup_key(),
                        KeyCode::Char('m') => app.handle_migrate_key(),
                        KeyCode::Char('r') => app.handle_refresh_key(),
                        KeyCode::Char('+') | KeyCode::Char('=') => app.handle_traffic_increase(),
                        KeyCode::Char('-') => app.handle_traffic_decrease(),
                        _ => {}
                    }
                }
            }
        }

        if app.last_update.elapsed() >= tick_rate {
            app.update();
        }

        if app.should_quit {
            break;
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    println!("\n--- Summary ---");
    println!("Runtime: {}s", app.runtime().as_secs());
    for stats in &app.db_stats {
        let unique_str = stats
            .unique_keys
            .map(|n| format!("{}", n))
            .unwrap_or_else(|| "—".to_string());
        let coverage_str = stats
            .coverage
            .map(|p| format!("{:.1}%", p))
            .unwrap_or_else(|| "—".to_string());
        println!(
            ":{} keys={} unique={} coverage={}",
            stats.port, stats.keys, unique_str, coverage_str
        );
    }

    // Migration summary
    println!("\n--- Migration ---");
    match &app.migration_state.status {
        MigrationStatus::NotSetup => println!("Migration not configured"),
        MigrationStatus::Pending => println!("Migration pending"),
        MigrationStatus::Testing => println!("Migration testing"),
        MigrationStatus::Ready => println!("Migration ready"),
        MigrationStatus::Running => println!("Migration running"),
        MigrationStatus::PartialFailure => println!("Migration partial failure"),
        MigrationStatus::Failed => println!("Migration failed"),
        MigrationStatus::Paused => println!("Migration paused"),
        MigrationStatus::Completed => println!("Migration completed successfully"),
        MigrationStatus::RollingBack => println!("Migration rolling back"),
        MigrationStatus::RolledBack => println!("Migration rolled back"),
    }
    if let Some(ref id) = app.migration_state.migration_id {
        println!("Migration ID: {}", id);
    }

    Ok(())
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct ObserveCli {
        #[command(flatten)]
        observe: ObserveConfig,
    }

    // Helper to create a MigrationState with specific setup step and status
    fn create_test_state(
        setup_step: SetupStep,
        status: MigrationStatus,
        interlay_id: Option<&str>,
        mode: MigrationMode,
    ) -> MigrationState {
        MigrationState {
            setup_step,
            auth_token: Some("test_token".to_string()),
            org_id: "adam-demo".to_string(),
            api_base: "http://localhost:8000".to_string(),
            source_endpoint_id: Some("src_123".to_string()),
            dest_endpoint_id: Some("dst_456".to_string()),
            interlay_id: interlay_id.map(|s| s.to_string()),
            migration_id: Some("mig_789".to_string()),
            status,
            last_error: None,
            api_calls: vec![],
            mode,
            canary: CanaryState::default(),
            active_is_new: false,
        }
    }

    // ==========================================
    // parse_migration_status tests
    // ==========================================

    #[test]
    fn test_parse_migration_status_pending() {
        assert_eq!(
            parse_migration_status(Some("Pending")),
            MigrationStatus::Pending
        );
    }

    #[test]
    fn test_parse_migration_status_none_defaults_to_pending() {
        assert_eq!(parse_migration_status(None), MigrationStatus::Pending);
    }

    #[test]
    fn test_parse_migration_status_testing() {
        assert_eq!(
            parse_migration_status(Some("Testing")),
            MigrationStatus::Testing
        );
    }

    #[test]
    fn test_parse_migration_status_ready() {
        assert_eq!(
            parse_migration_status(Some("Ready")),
            MigrationStatus::Ready
        );
    }

    #[test]
    fn test_parse_migration_status_running() {
        assert_eq!(
            parse_migration_status(Some("Running")),
            MigrationStatus::Running
        );
    }

    #[test]
    fn test_parse_migration_status_partial_failure() {
        assert_eq!(
            parse_migration_status(Some("PartialFailure")),
            MigrationStatus::PartialFailure
        );
    }

    #[test]
    fn test_parse_migration_status_failed() {
        assert_eq!(
            parse_migration_status(Some("Failed")),
            MigrationStatus::Failed
        );
    }

    #[test]
    fn test_parse_migration_status_paused() {
        assert_eq!(
            parse_migration_status(Some("Paused")),
            MigrationStatus::Paused
        );
    }

    #[test]
    fn test_parse_migration_status_completed() {
        assert_eq!(
            parse_migration_status(Some("Completed")),
            MigrationStatus::Completed
        );
    }

    #[test]
    fn test_parse_migration_status_rolling_back() {
        assert_eq!(
            parse_migration_status(Some("RollingBack")),
            MigrationStatus::RollingBack
        );
    }

    #[test]
    fn test_parse_migration_status_rolled_back() {
        assert_eq!(
            parse_migration_status(Some("RolledBack")),
            MigrationStatus::RolledBack
        );
    }

    #[test]
    fn test_parse_migration_status_unknown_defaults_to_pending() {
        assert_eq!(
            parse_migration_status(Some("UnknownStatus")),
            MigrationStatus::Pending
        );
    }

    // ==========================================
    // MigrationState::is_ready tests
    // ==========================================

    #[test]
    fn test_is_ready_when_setup_ready() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_not_started() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::NotSetup,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_creating_organization() {
        let state = create_test_state(
            SetupStep::CreatingOrganization,
            MigrationStatus::NotSetup,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_failed() {
        let state = create_test_state(
            SetupStep::Failed("error".to_string()),
            MigrationStatus::NotSetup,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.is_ready());
    }

    // ==========================================
    // MigrationState::can_migrate tests
    // ==========================================

    #[test]
    fn test_can_migrate_when_ready_and_pending() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_migrate());
    }

    #[test]
    fn test_can_migrate_when_ready_and_testing() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Testing,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_migrate());
    }

    #[test]
    fn test_can_migrate_when_ready_and_status_ready() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Ready,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_migrate());
    }

    #[test]
    fn test_cannot_migrate_when_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_migrate());
    }

    #[test]
    fn test_cannot_migrate_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Pending,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_migrate());
    }

    // ==========================================
    // MigrationState::can_complete tests
    // ==========================================

    #[test]
    fn test_can_complete_when_ready_and_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_complete());
    }

    #[test]
    fn test_cannot_complete_when_pending() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_complete());
    }

    #[test]
    fn test_cannot_complete_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Running,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_complete());
    }

    #[test]
    fn test_cannot_complete_when_already_completed() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Completed,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_complete());
    }

    // ==========================================
    // MigrationState::can_rollback tests
    // ==========================================

    #[test]
    fn test_can_rollback_when_completed() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Completed,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    #[test]
    fn test_can_rollback_when_failed() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Failed,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    #[test]
    fn test_can_rollback_when_partial_failure() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::PartialFailure,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_pending() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_testing() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Testing,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Running,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_without_interlay_id() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_rolling_back() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::RollingBack,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_rolled_back() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::RolledBack,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    // ==========================================
    // MigrationState::can_update_traffic tests
    // ==========================================

    #[test]
    fn test_can_update_traffic_in_canary_mode_when_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::Canary,
        );
        assert!(state.can_update_traffic());
    }

    #[test]
    fn test_cannot_update_traffic_in_bigbang_mode() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_update_traffic());
    }

    #[test]
    fn test_cannot_update_traffic_when_not_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::Canary,
        );
        assert!(!state.can_update_traffic());
    }

    #[test]
    fn test_cannot_update_traffic_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Running,
            None,
            MigrationMode::Canary,
        );
        assert!(!state.can_update_traffic());
    }

    // ==========================================
    // MigrationMode::toggle tests
    // ==========================================

    #[test]
    fn test_toggle_bigbang_to_canary() {
        assert_eq!(MigrationMode::BigBang.toggle(), MigrationMode::Canary);
    }

    #[test]
    fn test_toggle_canary_to_bigbang() {
        assert_eq!(MigrationMode::Canary.toggle(), MigrationMode::BigBang);
    }

    // ==========================================
    // MigrationMode::name tests
    // ==========================================

    #[test]
    fn test_bigbang_name() {
        assert_eq!(MigrationMode::BigBang.name(), "BigBang");
    }

    #[test]
    fn test_canary_name() {
        assert_eq!(MigrationMode::Canary.name(), "Canary");
    }

    // ==========================================
    // ApiCall tests
    // ==========================================

    #[test]
    fn test_api_call_new() {
        let call = ApiCall::new("Test API Call");
        assert_eq!(call.name, "Test API Call");
        assert_eq!(call.status, ApiCallStatus::Pending);
    }

    // ==========================================
    // MigrationState::update_api_call tests
    // ==========================================

    #[test]
    fn test_update_api_call_valid_index() {
        let mut state =
            MigrationState::new("http://localhost:8000".to_string(), "adam-demo".to_string());
        state.update_api_call(0, ApiCallStatus::InProgress);
        assert_eq!(state.api_calls[0].status, ApiCallStatus::InProgress);
    }

    #[test]
    fn test_update_api_call_success() {
        let mut state =
            MigrationState::new("http://localhost:8000".to_string(), "adam-demo".to_string());
        state.update_api_call(1, ApiCallStatus::Success);
        assert_eq!(state.api_calls[1].status, ApiCallStatus::Success);
    }

    #[test]
    fn test_update_api_call_failed() {
        let mut state =
            MigrationState::new("http://localhost:8000".to_string(), "adam-demo".to_string());
        state.update_api_call(2, ApiCallStatus::Failed("error message".to_string()));
        assert_eq!(
            state.api_calls[2].status,
            ApiCallStatus::Failed("error message".to_string())
        );
    }

    #[test]
    fn test_update_api_call_invalid_index_does_nothing() {
        let mut state =
            MigrationState::new("http://localhost:8000".to_string(), "adam-demo".to_string());
        let original_len = state.api_calls.len();
        state.update_api_call(100, ApiCallStatus::Success);
        // Should not panic or modify anything
        assert_eq!(state.api_calls.len(), original_len);
    }

    // ==========================================
    // CanaryState tests
    // ==========================================

    #[test]
    fn test_canary_state_default() {
        let canary = CanaryState::default();
        assert_eq!(canary.read_percentage, 0.05);
        assert_eq!(canary.write_policy, "OldAuthoritative");
    }

    #[test]
    fn test_is_existing_resource_error_variants() {
        assert!(is_existing_resource_error(
            "Create migration failed (409 Conflict): already exists"
        ));
        assert!(is_existing_resource_error(
            "Interlay already has an active migration"
        ));
        assert!(is_existing_resource_error(
            "duplicate key value violates unique constraint"
        ));
        assert!(!is_existing_resource_error("request timed out"));
    }

    #[test]
    fn test_observe_config_accepts_eden_override_flags() {
        let cli = ObserveCli::try_parse_from([
            "observe",
            "--source-url",
            "redis://localhost:6378",
            "--dest-url",
            "redis://localhost:6377",
            "--api-url",
            "http://localhost:8000",
            "--eden-source-url",
            "redis://host.docker.internal:6378",
            "--eden-dest-url",
            "redis://host.docker.internal:6377",
        ])
        .expect("observe config should accept Eden override flags");

        assert_eq!(
            cli.observe.eden_source.as_deref(),
            Some("redis://host.docker.internal:6378")
        );
        assert_eq!(
            cli.observe.eden_dest.as_deref(),
            Some("redis://host.docker.internal:6377")
        );
        assert_eq!(cli.observe.eden_source_positional, None);
        assert_eq!(cli.observe.eden_dest_positional, None);
    }

    #[test]
    fn test_observe_config_accepts_positional_eden_overrides() {
        let cli = ObserveCli::try_parse_from([
            "observe",
            "redis://localhost:6378",
            "redis://localhost:6377",
            "http://localhost:8000",
            "redis://host.docker.internal:6378",
            "redis://host.docker.internal:6377",
        ])
        .expect("observe config should accept positional Eden overrides");

        assert_eq!(cli.observe.eden_source, None);
        assert_eq!(cli.observe.eden_dest, None);
        assert_eq!(
            cli.observe.eden_source_positional.as_deref(),
            Some("redis://host.docker.internal:6378")
        );
        assert_eq!(
            cli.observe.eden_dest_positional.as_deref(),
            Some("redis://host.docker.internal:6377")
        );
    }
}
