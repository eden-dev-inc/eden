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
//!     c                  Force coverage check now
//!     v                  Toggle ops/sec chart
//!     s                  Start migration setup (connect to Eden API)
//!     m                  Trigger migration
//!     r                  Refresh migration status

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Row, Table},
};
use redis::Client;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::io;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const HISTORY_SIZE: usize = 120;
const DEFAULT_API_BASE: &str = "http://localhost:8000";

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
    Cancelled,
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
}

impl MigrationState {
    fn new(api_base: String) -> Self {
        Self {
            setup_step: SetupStep::NotStarted,
            auth_token: None,
            org_id: "TestOrg".to_string(),
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
        Some("Cancelled") => MigrationStatus::Cancelled,
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
    ApiCallUpdate { index: usize, status: ApiCallStatus },
    SetupComplete {
        auth_token: String,
        source_endpoint_id: String,
        dest_endpoint_id: String,
        interlay_id: String,
        migration_id: String,
    },
    SetupFailed(String),
    MigrationTriggered,
    MigrationStatusUpdate(MigrationStatus),
    MigrationError(String),
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
            .header("Authorization", "Bearer neworgsecret")
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
        let body = serde_json::json!({
            "id": &self.org_id
        });

        let response = self
            .client
            .post(format!("{}/api/v1/auth/login", self.base_url))
            .basic_auth(username, Some(password))
            .header("Content-Type", "application/json")
            .header("X-Org-Id", &self.org_id)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Login request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("Login failed ({}): {}", status, text));
        }

        let resp: LoginResponse = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse login response: {}", e))?;

        Ok(resp.token)
    }

    async fn create_endpoint(
        &self,
        endpoint_id: &str,
        host: &str,
        port: u16,
    ) -> Result<EndpointResponseData, String> {
        let body = serde_json::json!({
            "endpoint": endpoint_id,
            "kind": "redis",
            "config": {
                "read_conn": null,
                "write_conn": {
                    "host": host,
                    "port": port,
                    "tls": false
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

        response
            .json()
            .await
            .map_err(|e| format!("Failed to parse endpoint response: {}", e))
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

        response
            .json()
            .await
            .map_err(|e| format!("Failed to parse interlay response: {}", e))
    }

    async fn create_migration(&self, migration_id: &str) -> Result<MigrationResponseData, String> {
        let body = serde_json::json!({
            "id": migration_id,
            "description": "Redis big bang migration",
            "strategy": {"type": "big_bang", "durability": true},
            "data": null,
            "failure_handling": null
        });

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
    ) -> Result<(), String> {
        let body = serde_json::json!({
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
        });

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

    async fn refresh_migration(
        &self,
        migration_id: &str,
    ) -> Result<MigrationResponseData, String> {
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

        response
            .json()
            .await
            .map_err(|e| format!("Failed to parse migration response: {}", e))
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

        response
            .json()
            .await
            .map_err(|e| format!("Failed to parse endpoint response: {}", e))
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

        response
            .json()
            .await
            .map_err(|e| format!("Failed to parse interlay response: {}", e))
    }
}

// ============================================
// Async Task Functions
// ============================================

async fn run_migration_setup(
    tx: mpsc::Sender<ApiEvent>,
    source_host: String,
    source_port: String,
    dest_host: String,
    dest_port: String,
    org_id: String,
    api_base: String,
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

    match client.create_organization("admin", "password").await {
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
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
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

    let token = match client.login("admin", "password").await {
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
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
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
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
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
        .create_interlay(&interlay_id, &source_ep.uuid, 6366)
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
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
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

    let migration_id = format!("redis_migration_{}_{}", source_port, dest_port);
    let migration = match client.create_migration(&migration_id).await {
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
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
                // Fetch the existing migration to get the real UUID
                match client.get_migration(&migration_id).await {
                    Ok(m) => {
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
        .add_interlay_to_migration(&migration.id, &interlay.id, &dest_ep.id)
        .await
    {
        // Check if it's an "already exists" type error
        if e.contains("409")
            || e.contains("already exists")
            || e.contains("Conflict")
            || e.contains("already has an active migration")
        {
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
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::Ready))
        .await;

    // Collect status using get after startup completes
    match client.get_migration(&migration.id).await {
        Ok(data) => {
            let status = parse_migration_status(data.status.as_deref());
            let _ = tx.send(ApiEvent::MigrationStatusUpdate(status)).await;
        }
        Err(_) => {
            // Fallback to status from create/get response
            let status = parse_migration_status(migration.status.as_deref());
            let _ = tx.send(ApiEvent::MigrationStatusUpdate(status)).await;
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
                        let _ = tx.send(ApiEvent::MigrationStatusUpdate(status.clone())).await;

                        // Stop polling when migration reaches a terminal state
                        match status {
                            MigrationStatus::Completed
                            | MigrationStatus::Failed
                            | MigrationStatus::Cancelled
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
    if let Err(e) = client.refresh_migration(&migration_id).await {
        let _ = tx.send(ApiEvent::MigrationError(e)).await;
        return;
    }

    // Then collect status using get
    match client.get_migration(&migration_id).await {
        Ok(data) => {
            let status = parse_migration_status(data.status.as_deref());
            let _ = tx.send(ApiEvent::MigrationStatusUpdate(status)).await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::MigrationError(e)).await;
        }
    }
}

// ============================================
// Application Config and State
// ============================================

struct Config {
    // TUI connection addresses (what we connect to locally)
    source_host: String,
    source_port: String,
    dest_host: String,
    dest_port: String,
    // Eden API addresses (what Eden should connect to - may differ from TUI)
    eden_source_host: String,
    eden_source_port: String,
    eden_dest_host: String,
    eden_dest_port: String,
    api_base: String,
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
    show_debug: bool,
    debug_log: Vec<String>,
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
            show_debug: false,
            debug_log: Vec::new(),
            migration_state: MigrationState::new(api_base),
            api_event_tx,
            api_event_rx,
            runtime,
        }
    }

    fn log_debug(&mut self, msg: String) {
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
                ApiEvent::MigrationStatusUpdate(ref status) => {
                    // Only log significant status changes
                    match status {
                        MigrationStatus::Completed => self.log_debug("Migration completed".to_string()),
                        MigrationStatus::Failed => self.log_debug("Migration failed".to_string()),
                        MigrationStatus::PartialFailure => self.log_debug("Migration partial failure".to_string()),
                        MigrationStatus::Cancelled => self.log_debug("Migration cancelled".to_string()),
                        MigrationStatus::RolledBack => self.log_debug("Migration rolled back".to_string()),
                        _ => {} // Don't log pending/running repeatedly
                    }
                    self.migration_state.status = status.clone();
                }
                ApiEvent::MigrationError(err) => {
                    self.log_debug(format!("Error: {}", err));
                    self.migration_state.last_error = Some(err);
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

            self.runtime
                .spawn(trigger_migration_task(tx, token, org_id, migration_id, api_base));
        }
    }

    fn handle_refresh_key(&mut self) {
        if self.migration_state.is_ready() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime
                .spawn(refresh_migration_task(tx, token, org_id, migration_id, api_base));
        }
    }

    fn handle_setup_key(&mut self) {
        // Only start setup if not already started
        if self.migration_state.setup_step == SetupStep::NotStarted {
            let tx = self.api_event_tx.clone();
            // Use Eden hosts/ports (may differ from TUI when running locally)
            let source_host = self.config.eden_source_host.clone();
            let source_port = self.config.eden_source_port.clone();
            let dest_host = self.config.eden_dest_host.clone();
            let dest_port = self.config.eden_dest_port.clone();
            let org_id = self.migration_state.org_id.clone();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(run_migration_setup(
                tx,
                source_host,
                source_port,
                dest_host,
                dest_port,
                org_id,
                api_base,
            ));
        }
    }

    fn update(&mut self) {
        self.total_ticks += 1;

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

fn parse_host_port(arg: &str) -> (String, String) {
    if let Some(idx) = arg.rfind(':') {
        let host = &arg[..idx];
        let port = &arg[idx + 1..];
        // If host is empty or port is not a number, treat whole thing as port with default host
        if !host.is_empty() && port.parse::<u16>().is_ok() {
            return (host.to_string(), port.to_string());
        }
    }
    // No colon or invalid format - treat as port only with default host
    (DEFAULT_REDIS_HOST.to_string(), arg.to_string())
}

fn parse_args() -> Option<Config> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.len() < 2 {
        return None;
    }

    let (source_host, source_port) = parse_host_port(&args[0]);
    let (dest_host, dest_port) = parse_host_port(&args[1]);
    let api_base = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string());

    // Optional 4th arg: Eden source as host:port
    // Optional 5th arg: Eden dest as host:port
    let (eden_source_host, eden_source_port) = args
        .get(3)
        .map(|s| parse_host_port(s))
        .unwrap_or_else(|| (source_host.clone(), source_port.clone()));

    let (eden_dest_host, eden_dest_port) = args
        .get(4)
        .map(|s| parse_host_port(s))
        .unwrap_or_else(|| (dest_host.clone(), dest_port.clone()));

    Some(Config {
        source_host,
        source_port,
        dest_host,
        dest_port,
        eden_source_host,
        eden_source_port,
        eden_dest_host,
        eden_dest_port,
        api_base,
    })
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
    let header = Row::new(vec![
        "port", "keys", "Δ", "unique", "ops/s", "conn", "coverage",
    ])
    .style(Style::default().fg(Color::DarkGray))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .db_stats
        .iter()
        .map(|stats| {
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

            Row::new(vec![
                Span::styled(
                    format!(":{}", stats.port),
                    Style::default().fg(status_color),
                ),
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
            .title(" Instances ")
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

fn draw_debug_panel(f: &mut Frame, area: Rect, app: &App) {
    let lines: Vec<Line> = app
        .debug_log
        .iter()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .rev()
        .map(|msg| {
            let color = if msg.contains("FAILED") || msg.contains("ERROR") {
                Color::Red
            } else if msg.contains("Success") || msg.contains("complete") {
                Color::Green
            } else if msg.contains("Skipped") {
                Color::Cyan
            } else if msg.contains("Starting") {
                Color::Yellow
            } else {
                Color::White
            };
            Line::from(Span::styled(msg.clone(), Style::default().fg(color)))
        })
        .collect();

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(" Log ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    f.render_widget(paragraph, area);
}

fn draw_api_panel(f: &mut Frame, area: Rect, app: &App) {
    let state = &app.migration_state;

    let mut lines = vec![];

    // Header
    lines.push(Line::from(Span::styled(
        "API Calls",
        Style::default().fg(Color::White).bold(),
    )));
    lines.push(Line::from(""));

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
            MigrationStatus::NotSetup => Span::styled("Not configured", Style::default().fg(Color::DarkGray)),
            MigrationStatus::Pending => Span::styled("Pending", Style::default().fg(Color::DarkGray)),
            MigrationStatus::Testing => Span::styled("Testing...", Style::default().fg(Color::Yellow)),
            MigrationStatus::Ready => Span::styled("Ready to migrate", Style::default().fg(Color::Cyan)),
            MigrationStatus::Running => Span::styled("Running...", Style::default().fg(Color::Yellow)),
            MigrationStatus::PartialFailure => Span::styled("Partial failure", Style::default().fg(Color::Red)),
            MigrationStatus::Failed => Span::styled("Failed", Style::default().fg(Color::Red)),
            MigrationStatus::Paused => Span::styled("Paused", Style::default().fg(Color::Yellow)),
            MigrationStatus::Cancelled => Span::styled("Cancelled", Style::default().fg(Color::Red)),
            MigrationStatus::Completed => Span::styled("Completed", Style::default().fg(Color::Green)),
            MigrationStatus::RollingBack => Span::styled("Rolling back...", Style::default().fg(Color::Yellow)),
            MigrationStatus::RolledBack => Span::styled("Rolled back", Style::default().fg(Color::Magenta)),
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
                Constraint::Min(15),      // Main content
                Constraint::Length(12),   // Debug panel
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

    // Status bar with migration keys
    let status = Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::White)),
        Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("c", Style::default().fg(Color::White)),
        Span::styled(
            format!(" coverage ({}s)  ", app.coverage_countdown),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled("v", Style::default().fg(Color::White)),
        Span::styled(
            if app.show_ops {
                " hide ops  "
            } else {
                " show ops  "
            },
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled("s", Style::default().fg(Color::White)),
        Span::styled(" setup  ", Style::default().fg(Color::DarkGray)),
        Span::styled("m", Style::default().fg(Color::White)),
        Span::styled(" migrate  ", Style::default().fg(Color::DarkGray)),
        Span::styled("r", Style::default().fg(Color::White)),
        Span::styled(" refresh  ", Style::default().fg(Color::DarkGray)),
        Span::styled("d", Style::default().fg(Color::White)),
        Span::styled(
            if app.show_debug { " hide debug" } else { " debug" },
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(status), right_chunks[3]);
}

fn check_redis_connection(label: &str, host: &str, port: &str) -> Result<Client, String> {
    let url = format!("redis://{}:{}", host, port);
    println!("Connecting to {} Redis at {}:{}...", label, host, port);

    let client = Client::open(url.as_str())
        .map_err(|e| format!("Failed to create {} Redis client: {}", label, e))?;

    let mut conn = client
        .get_connection()
        .map_err(|e| format!("Failed to connect to {} Redis at {}:{}: {}", label, host, port, e))?;

    let _: String = redis::cmd("PING")
        .query(&mut conn)
        .map_err(|e| format!("Failed to ping {} Redis at {}:{}: {}", label, host, port, e))?;

    println!("  Connected to {} Redis", label);
    Ok(client)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match parse_args() {
        Some(c) => c,
        None => {
            eprintln!("Usage: cargo run -- <source> <dest> [api_endpoint] [eden_source] [eden_dest]");
            eprintln!();
            eprintln!("Arguments:");
            eprintln!("  source       Source Redis as host:port or just port (default host: {})", DEFAULT_REDIS_HOST);
            eprintln!("  dest         Destination Redis as host:port or just port (default host: {})", DEFAULT_REDIS_HOST);
            eprintln!("  api_endpoint Eden API endpoint (default: {})", DEFAULT_API_BASE);
            eprintln!("  eden_source  Eden's source Redis as host:port (when different from TUI connection)");
            eprintln!("  eden_dest    Eden's dest Redis as host:port (when different from TUI connection)");
            eprintln!();
            eprintln!("Examples:");
            eprintln!("  cargo run -- 6379 6380                           # Both use default host");
            eprintln!("  cargo run -- 192.168.1.10:6379 192.168.1.20:6380 # Different hosts");
            eprintln!("  cargo run -- localhost:6379 localhost:6380 http://localhost:8000 172.24.2.211:6379 172.24.2.218:6379");
            eprintln!("                                                   # TUI uses localhost, Eden uses different IPs");
            std::process::exit(1);
        }
    };

    // Health check: verify Redis connections BEFORE entering TUI
    println!("Checking Redis connections...");
    let source_client = match check_redis_connection("source", &config.source_host, &config.source_port) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    let dest_client = match check_redis_connection("dest", &config.dest_host, &config.dest_port) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    println!("All connections verified. Starting TUI...\n");

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

    let mut app = App::new_with_clients(config, source_client, dest_client, tx, rx, runtime.handle().clone());

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
                        KeyCode::Char('c') => app.force_coverage = true,
                        KeyCode::Char('v') => app.show_ops = !app.show_ops,
                        KeyCode::Char('d') => app.show_debug = !app.show_debug,
                        KeyCode::Esc => app.should_quit = true,
                        KeyCode::Char('s') => app.handle_setup_key(),
                        KeyCode::Char('m') => app.handle_migrate_key(),
                        KeyCode::Char('r') => app.handle_refresh_key(),
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
        MigrationStatus::Cancelled => println!("Migration cancelled"),
        MigrationStatus::Completed => println!("Migration completed successfully"),
        MigrationStatus::RollingBack => println!("Migration rolling back"),
        MigrationStatus::RolledBack => println!("Migration rolled back"),
    }
    if let Some(ref id) = app.migration_state.migration_id {
        println!("Migration ID: {}", id);
    }

    Ok(())
}
