//! Eden API client for migration orchestration.

use crate::api_types::*;
use crate::migration::{CanaryState, MigrationMode};
use serde::de::DeserializeOwned;

pub const DEFAULT_EDEN_NEW_ORG_SECRET: &str = "neworgsecret";
pub const DEFAULT_EDEN_ADMIN_USER: &str = "admin";
pub const DEFAULT_EDEN_ADMIN_PASSWORD: &str = "adam-demo-pass";

pub fn eden_admin_user() -> String {
    std::env::var("EDEN_ADMIN_USER").unwrap_or_else(|_| DEFAULT_EDEN_ADMIN_USER.to_string())
}

pub fn eden_admin_password() -> String {
    std::env::var("EDEN_ADMIN_PASSWORD")
        .or_else(|_| std::env::var("EDEN_ADMIN_PASS"))
        .unwrap_or_else(|_| DEFAULT_EDEN_ADMIN_PASSWORD.to_string())
}

fn eden_new_org_secret() -> String {
    std::env::var("EDEN_NEW_ORG_SECRET")
        .or_else(|_| std::env::var("EDEN_NEW_ORG_TOKEN"))
        .unwrap_or_else(|_| DEFAULT_EDEN_NEW_ORG_SECRET.to_string())
}

pub struct EdenApiClient {
    client: reqwest::Client,
    pub base_url: String,
    pub auth_token: Option<String>,
    pub org_id: String,
}

impl EdenApiClient {
    pub fn new(org_id: String, base_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
            auth_token: None,
            org_id,
        }
    }

    pub fn with_auth(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.auth_token.as_ref().unwrap())
    }

    pub async fn create_organization(&self, username: &str, password: &str) -> Result<(), String> {
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

        let url = format!("{}/api/v1/new", self.base_url);
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", eden_new_org_secret()))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create organization request failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Create organization failed ({}): {}", status, text));
        }

        Ok(())
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<String, String> {
        let url = format!("{}/api/v1/auth/login", self.base_url);
        log::debug!(">>> POST {}", url);

        let response = self
            .client
            .post(&url)
            .basic_auth(username, Some(password))
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Login request failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Login failed ({}): {}", status, text));
        }

        let resp: LoginResponse = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse login response: {}", e))?;

        Ok(resp.token)
    }

    pub async fn grant_endpoint_data_access(
        &self,
        endpoint_id: &str,
        subject: &str,
        perms: &str,
    ) -> Result<(), String> {
        let url = format!(
            "{}/api/v1/iam/data/endpoints/{}/subjects/{}",
            self.base_url,
            encode_path_segment(endpoint_id),
            encode_path_segment(subject)
        );
        let body = serde_json::json!({ "perms": perms });
        log::debug!(">>> PUT {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .put(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Grant endpoint data access request failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Grant endpoint data access failed ({}) PUT {}: {}",
                status, url, text
            ));
        }

        Ok(())
    }

    pub async fn create_endpoint(
        &self,
        endpoint_id: &str,
        url: &str,
    ) -> Result<EndpointResponseData, String> {
        let body = serde_json::json!({
            "endpoint": endpoint_id,
            "kind": "postgres",
            "config": {
                "read_conn": null,
                "write_conn": {
                    "url": url
                }
            },
            "description": format!("PostgreSQL endpoint {}", endpoint_id)
        });

        let url = format!("{}/api/v1/endpoints", self.base_url);
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create endpoint failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Create endpoint failed ({}): {}", status, text));
        }

        parse_api_data(&text).map_err(|e| format!("Failed to parse endpoint response: {}", e))
    }

    pub async fn create_interlay(
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

        let url = format!("{}/api/v1/interlays", self.base_url);
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create interlay failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Create interlay failed ({}): {}", status, text));
        }

        parse_api_data(&text).map_err(|e| format!("Failed to parse interlay response: {}", e))
    }

    pub async fn create_migration(
        &self,
        migration_id: &str,
        mode: MigrationMode,
        canary_state: &CanaryState,
    ) -> Result<MigrationResponseData, String> {
        let body = match mode {
            MigrationMode::BigBang => serde_json::json!({
                "id": migration_id,
                "description": "PostgreSQL big bang migration",
                "strategy": {"type": "big_bang", "durability": true},
                "data": null,
                "failure_handling": null
            }),
            MigrationMode::Canary => serde_json::json!({
                "id": migration_id,
                "description": "PostgreSQL canary migration",
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
                "description": "PostgreSQL blue-green migration",
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
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Create migration request failed: {}", e))?;

        let resp_status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", resp_status, url);
        log::debug!("<<< Body: {}", text);

        if !resp_status.is_success() {
            return Err(format!(
                "Create migration failed ({}) POST {}: {}",
                resp_status, url, text
            ));
        }

        // Parse as Value first to handle different response formats
        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse migration response: {}", e))?;

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

    pub async fn add_interlay_to_migration(
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
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Add interlay request failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Add interlay failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        Ok(())
    }

    pub async fn update_traffic_split(
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
        log::debug!(">>> PATCH {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .patch(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Update traffic split failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Update traffic split failed ({}) PATCH {}: {}",
                status, url, text
            ));
        }

        parse_api_data(&text).map_err(|e| format!("Failed to parse traffic update response: {}", e))
    }

    pub async fn trigger_migration(&self, migration_id: &str) -> Result<(), String> {
        let url = format!(
            "{}/api/v1/migrations/{}/migrate",
            self.base_url, migration_id
        );
        log::debug!(">>> POST {}", url);

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Trigger migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Trigger migration failed ({}): {}", status, text));
        }

        Ok(())
    }

    pub async fn complete_migration(
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
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Complete migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Complete migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse complete migration response: {}", e))
    }

    pub async fn rollback_interlay(
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
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Rollback migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Rollback migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse rollback migration response: {}", e))
    }

    pub async fn pause_migration(
        &self,
        migration_id: &str,
        reason: Option<&str>,
    ) -> Result<PauseMigrationResponse, String> {
        let body = serde_json::json!({
            "reason": reason.unwrap_or("Manual pause from TUI")
        });

        let url = format!("{}/api/v1/migrations/{}/pause", self.base_url, migration_id);
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Pause migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Pause migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse pause migration response: {}", e))
    }

    pub async fn resume_migration(
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
        log::debug!(">>> POST {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Resume migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Resume migration failed ({}) POST {}: {}",
                status, url, text
            ));
        }

        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse resume migration response: {}", e))
    }

    pub async fn toggle_environment(
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
        log::debug!(">>> PATCH {}", url);
        log::debug!(
            ">>> Body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let response = self
            .client
            .patch(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Toggle environment failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!(
                "Toggle environment failed ({}) PATCH {}: {}",
                status, url, text
            ));
        }

        parse_api_data(&text)
            .map_err(|e| format!("Failed to parse toggle environment response: {}", e))
    }

    pub async fn refresh_migration(
        &self,
        migration_id: &str,
    ) -> Result<MigrationResponseData, String> {
        let url = format!(
            "{}/api/v1/migrations/{}/refresh",
            self.base_url, migration_id
        );
        log::debug!(">>> POST {}", url);

        let response = self
            .client
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Refresh migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Refresh migration failed ({}): {}", status, text));
        }

        self.get_migration(migration_id).await
    }

    pub async fn get_migration(&self, migration_id: &str) -> Result<MigrationResponseData, String> {
        let url = format!("{}/api/v1/migrations/{}", self.base_url, migration_id);
        log::debug!(">>> GET {}", url);

        let response = self
            .client
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .header("X-Eden-Verbose", "true")
            .send()
            .await
            .map_err(|e| format!("Get migration failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Get migration failed ({}): {}", status, text));
        }

        parse_api_data(&text).map_err(|e| format!("Failed to parse migration response: {}", e))
    }

    pub async fn get_endpoint(&self, endpoint_id: &str) -> Result<EndpointResponseData, String> {
        let url = format!("{}/api/v1/endpoints/{}", self.base_url, endpoint_id);
        log::debug!(">>> GET {}", url);

        let response = self
            .client
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Get endpoint failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Get endpoint failed ({}): {}", status, text));
        }

        parse_api_data(&text).map_err(|e| format!("Failed to parse endpoint response: {}", e))
    }

    pub async fn get_interlay(&self, interlay_id: &str) -> Result<InterlayResponseData, String> {
        let url = format!("{}/api/v1/interlays/{}", self.base_url, interlay_id);
        log::debug!(">>> GET {}", url);

        let response = self
            .client
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("X-Org-Id", &self.org_id)
            .send()
            .await
            .map_err(|e| format!("Get interlay failed: {}", e))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log::debug!("<<< {} {}", status, url);
        log::debug!("<<< Body: {}", text);

        if !status.is_success() {
            return Err(format!("Get interlay failed ({}): {}", status, text));
        }

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
