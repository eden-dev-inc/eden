use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tracing::{info, warn};

const DEFAULT_EDEN_ADMIN_USER: &str = "admin";
const DEFAULT_EDEN_ADMIN_ACCESS_LEVEL: &str = "Admin";

/// Client for the Eden API — handles auth, endpoint registration, and queries.
#[derive(Clone)]
pub struct EdenClient {
    client: Client,
    base_url: String,
    org_id: String,
    token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ApiResponse {
    pub status: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct EndpointInfo {
    pub id: String,
    pub uuid: String,
    pub kind: String,
}

impl EdenClient {
    pub fn new(base_url: &str, org_id: &str, token: &str) -> Self {
        Self {
            client: build_client(30),
            base_url: base_url.trim_end_matches('/').to_string(),
            org_id: org_id.to_string(),
            token: token.to_string(),
        }
    }

    pub async fn login(
        base_url: &str,
        org_id: &str,
        username: &str,
        password: &str,
        timeout_secs: u64,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = build_client(timeout_secs);
        let base = base_url.trim_end_matches('/');
        let token = Self::login_token(&client, base, org_id, username, password).await?;

        Ok(Self {
            client,
            base_url: base.to_string(),
            org_id: org_id.to_string(),
            token,
        })
    }

    /// Create a new organization and authenticate, with retries and backoff.
    pub async fn setup(
        base_url: &str,
        org_id: &str,
        new_org_secret: &str,
        timeout_secs: u64,
        max_retries: u32,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = build_client(timeout_secs);
        let base = base_url.trim_end_matches('/');

        // Retry loop — Eden may not be ready yet (especially in Docker)
        let mut last_err: Box<dyn std::error::Error + Send + Sync> = "no attempts made".into();

        for attempt in 1..=max_retries {
            match Self::try_setup(&client, base, org_id, new_org_secret).await {
                Ok(eden) => return Ok(eden),
                Err(e) => {
                    last_err = e;
                    if attempt < max_retries {
                        let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                        warn!(
                            "Eden setup attempt {}/{} failed: {}. Retrying in {:?}...",
                            attempt, max_retries, last_err, delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(format!(
            "Eden setup failed after {} attempts: {}",
            max_retries, last_err
        )
        .into())
    }

    /// Single setup attempt: create org + login.
    async fn try_setup(
        client: &Client,
        base: &str,
        org_id: &str,
        new_org_secret: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // 1. Create organization
        info!("Creating organization '{}'...", org_id);
        let resp = client
            .post(format!("{}/api/v1/new", base))
            .header("Authorization", format!("Bearer {}", new_org_secret))
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "id": org_id,
                "super_admins": [{"username": "admin", "password": "adam-demo-pass"}]
            }))
            .send()
            .await?;

        if resp.status().is_success() {
            info!("Organization '{}' created", org_id);
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if body.contains("already exists") || body.contains("duplicate") {
                info!(
                    "Organization '{}' already exists, proceeding to login",
                    org_id
                );
            } else {
                return Err(format!("Create org returned {}: {}", status, body).into());
            }
        }

        // 2. Login to get JWT
        let token = Self::login_token(client, base, org_id, "admin", "adam-demo-pass").await?;

        Ok(Self {
            client: client.clone(),
            base_url: base.to_string(),
            org_id: org_id.to_string(),
            token,
        })
    }

    async fn login_token(
        client: &Client,
        base: &str,
        org_id: &str,
        username: &str,
        password: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!("Authenticating...");
        let resp = client
            .post(format!("{}/api/v1/auth/login", base))
            .basic_auth(username, Some(password))
            .header("X-Org-Id", org_id)
            .send()
            .await?;

        let status = resp.status();
        let body: Value = resp.json().await?;

        if !status.is_success() {
            return Err(format!("Login returned {}: {}", status, body).into());
        }

        let token = login_token_from_body(&body)
            .ok_or_else(|| format!("No token in login response: {}", body))?
            .to_string();

        info!("Authenticated successfully");

        Ok(token)
    }

    /// Register a database endpoint with Eden. Retries on transient failures.
    pub async fn create_endpoint(
        &self,
        id: &str,
        kind: &str,
        config: Value,
        description: &str,
    ) -> Result<EndpointInfo, Box<dyn std::error::Error + Send + Sync>> {
        let max_retries = 3u32;
        let mut last_err: Box<dyn std::error::Error + Send + Sync> = "no attempts made".into();

        for attempt in 1..=max_retries {
            match self
                .try_create_endpoint(id, kind, config.clone(), description)
                .await
            {
                Ok(info) => match self
                    .set_endpoint_access_aliases(
                        &info.id,
                        Some(info.uuid.as_str()),
                        &[(DEFAULT_EDEN_ADMIN_USER, DEFAULT_EDEN_ADMIN_ACCESS_LEVEL)],
                        true,
                        true,
                    )
                    .await
                {
                    Ok(()) => return Ok(info),
                    Err(e) => {
                        last_err = e;
                        if attempt < max_retries {
                            let delay = Duration::from_secs(2u64.pow(attempt));
                            warn!(
                                "Endpoint '{}' RBAC grant attempt {}/{} failed: {}. Retrying in {:?}...",
                                id, attempt, max_retries, last_err, delay
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                },
                Err(e) => {
                    last_err = e;
                    if attempt < max_retries {
                        let delay = Duration::from_secs(2u64.pow(attempt));
                        warn!(
                            "Endpoint '{}' registration attempt {}/{} failed: {}. Retrying in {:?}...",
                            id, attempt, max_retries, last_err, delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(format!(
            "Failed to register endpoint '{}' after {} attempts: {}",
            id, max_retries, last_err
        )
        .into())
    }

    async fn try_create_endpoint(
        &self,
        id: &str,
        kind: &str,
        config: Value,
        description: &str,
    ) -> Result<EndpointInfo, Box<dyn std::error::Error + Send + Sync>> {
        info!("Registering endpoint '{}' ({})...", id, kind);

        let payload = serde_json::json!({
            "endpoint": id,
            "kind": kind,
            "config": config,
            "description": description
        });
        info!(
            "  Payload: {}",
            serde_json::to_string(&redacted_json(payload.clone())).unwrap_or_default()
        );

        let resp = self
            .client
            .post(format!("{}/api/v1/endpoints", self.base_url))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = resp.status();
        let raw_body = resp.text().await?;
        info!(
            "  Response ({}): {}",
            status,
            &raw_body[..raw_body.len().min(500)]
        );

        let body: Value = serde_json::from_str(&raw_body).map_err(|e| {
            format!(
                "Failed to parse response as JSON ({}): {} — body: {}",
                status,
                e,
                &raw_body[..raw_body.len().min(200)]
            )
        })?;

        if !status.is_success() {
            // May already exist
            let msg = body["error"]
                .as_str()
                .or_else(|| body["message"].as_str())
                .unwrap_or("");
            if msg.contains("already exists") || msg.contains("duplicate") {
                info!("Endpoint '{}' already exists, updating config...", id);
                return self.update_endpoint(id, kind, config).await;
            }
            return Err(
                format!("Failed to create endpoint '{}' ({}): {}", id, status, body).into(),
            );
        }

        // EdenResponse::Response serializes flat: {"id": "...", "uuid": "..."}
        let uuid = body["uuid"]
            .as_str()
            .or_else(|| body["data"]["uuid"].as_str())
            .or_else(|| body["data"]["id"].as_str())
            .unwrap_or(id)
            .to_string();

        info!("  Endpoint '{}' registered (uuid: {})", id, uuid);

        Ok(EndpointInfo {
            id: id.to_string(),
            uuid,
            kind: kind.to_string(),
        })
    }

    async fn update_endpoint(
        &self,
        id: &str,
        kind: &str,
        config: Value,
    ) -> Result<EndpointInfo, Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::json!({ "config": config });
        let resp = self
            .client
            .patch(format!("{}/api/v1/endpoints/{}", self.base_url, id))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = resp.status();
        let raw_body = resp.text().await?;
        info!(
            "  Update response ({}): {}",
            status,
            &raw_body[..raw_body.len().min(500)]
        );

        if !status.is_success() {
            return Err(format!(
                "Failed to update endpoint '{}' ({}): {}",
                id, status, raw_body
            )
            .into());
        }

        let body: Value = serde_json::from_str(&raw_body).unwrap_or(Value::Null);
        let uuid = body["uuid"]
            .as_str()
            .or_else(|| body["data"]["uuid"].as_str())
            .or_else(|| body["data"]["id"].as_str())
            .unwrap_or(id)
            .to_string();

        Ok(EndpointInfo {
            id: id.to_string(),
            uuid,
            kind: kind.to_string(),
        })
    }

    /// Get existing endpoint info.
    pub async fn get_endpoint(
        &self,
        id: &str,
    ) -> Result<EndpointInfo, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .client
            .get(format!("{}/api/v1/endpoints/{}", self.base_url, id))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .send()
            .await?;

        let status = resp.status();
        let body: Value = resp.json().await?;

        if !status.is_success() {
            return Err(format!("GET endpoint '{}' returned {}: {}", id, status, body).into());
        }

        let uuid = body["data"]["uuid"]
            .as_str()
            .or_else(|| body["data"]["id"].as_str())
            .unwrap_or(id)
            .to_string();
        let kind = body["data"]["kind"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();

        Ok(EndpointInfo {
            id: id.to_string(),
            uuid,
            kind,
        })
    }

    /// Execute a read query against an endpoint.
    /// The query body is wrapped in {"request": ...} as required by the Eden API.
    pub async fn query(
        &self,
        endpoint_id: &str,
        query: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let wrapped = serde_json::json!({"request": query});
        let resp = self
            .client
            .post(format!(
                "{}/api/v1/endpoints/{}/read",
                self.base_url, endpoint_id
            ))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&wrapped)
            .send()
            .await?;

        let status = resp.status();
        let raw = resp.text().await?;
        let body: Value = serde_json::from_str(&raw).map_err(|e| {
            format!(
                "Failed to parse response from '{}' (status {}): {} — body: {}",
                endpoint_id,
                status,
                e,
                &raw[..raw.len().min(500)]
            )
        })?;

        if !status.is_success() {
            return Err(format!(
                "Query to '{}' returned {}: {}",
                endpoint_id,
                status,
                body.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or(&raw[..raw.len().min(200)])
            )
            .into());
        }

        Ok(body)
    }

    /// Delete an endpoint. Missing endpoints are treated as success.
    pub async fn delete_endpoint(
        &self,
        id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .client
            .delete(format!("{}/api/v1/endpoints/{}", self.base_url, id))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .send()
            .await?;

        let status = resp.status();
        let raw_body = resp.text().await.unwrap_or_default();

        if status.is_success()
            || status == reqwest::StatusCode::NOT_FOUND
            || raw_body.contains("not found")
        {
            return Ok(());
        }

        Err(format!(
            "Failed to delete endpoint '{}' ({}): {}",
            id, status, raw_body
        )
        .into())
    }

    /// Execute a write operation against an endpoint.
    #[allow(dead_code)]
    pub async fn write(
        &self,
        endpoint_id: &str,
        payload: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .client
            .post(format!(
                "{}/api/v1/endpoints/{}/write",
                self.base_url, endpoint_id
            ))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = resp.status();
        let body: Value = resp.json().await?;

        if !status.is_success() {
            return Err(format!(
                "Write to '{}' returned {}: {}",
                endpoint_id,
                status,
                body.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
            )
            .into());
        }

        Ok(body)
    }

    /// Create a user in the organization.
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        email: &str,
        display_name: &str,
        description: &str,
        access_level: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = create_user_payload(
            username,
            password,
            email,
            display_name,
            description,
            access_level,
        )?;
        let resp = self
            .client
            .post(format!("{}/api/v1/iam/humans", self.base_url))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Org-Id", &self.org_id)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        if body.contains("already exists") || body.contains("duplicate") {
            return Ok(());
        }
        Err(format!("Create user '{}' returned {}: {}", username, status, body).into())
    }

    /// Set exact org-level control-plane permissions for users.
    /// subjects: list of (username, access_level) pairs.
    pub async fn set_org_control_access(
        &self,
        subjects: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (subject, access_level) in subjects {
            let subject_segment = encode_path_segment(subject);
            let url = format!(
                "{}/api/v1/iam/control/organizations/subjects/{}",
                self.base_url, subject_segment
            );
            self.set_exact_subject_perms(
                &url,
                control_perms_for_access_level(access_level)?,
                &format!("organization control access for '{}'", subject),
            )
            .await?;
        }

        Ok(())
    }

    /// Set exact per-endpoint control-plane permissions for users.
    /// subjects: list of (username, access_level) pairs.
    pub async fn set_endpoint_control_access(
        &self,
        endpoint_id: &str,
        subjects: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (subject, access_level) in subjects {
            let endpoint_segment = encode_path_segment(endpoint_id);
            let subject_segment = encode_path_segment(subject);
            let url = format!(
                "{}/api/v1/iam/control/endpoints/{}/subjects/{}",
                self.base_url, endpoint_segment, subject_segment
            );
            self.set_exact_subject_perms(
                &url,
                control_perms_for_access_level(access_level)?,
                &format!(
                    "endpoint '{}' control access for '{}'",
                    endpoint_id, subject
                ),
            )
            .await?;
        }

        Ok(())
    }

    /// Set exact per-endpoint shared runtime permissions for users.
    /// subjects: list of (username, access_level) pairs.
    pub async fn set_endpoint_data_access(
        &self,
        endpoint_id: &str,
        subjects: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (subject, access_level) in subjects {
            let endpoint_segment = encode_path_segment(endpoint_id);
            let subject_segment = encode_path_segment(subject);
            let url = format!(
                "{}/api/v1/iam/data/endpoints/{}/subjects/{}",
                self.base_url, endpoint_segment, subject_segment
            );
            self.set_exact_subject_perms(
                &url,
                data_perms_for_access_level(access_level)?,
                &format!("endpoint '{}' data access for '{}'", endpoint_id, subject),
            )
            .await?;
        }

        Ok(())
    }

    /// Set per-endpoint permissions for every known endpoint alias.
    /// Some Eden surfaces key endpoint permissions by endpoint id, while others
    /// use the endpoint UUID, so we keep both in sync.
    pub async fn set_endpoint_access_aliases(
        &self,
        endpoint_id: &str,
        endpoint_uuid: Option<&str>,
        subjects: &[(&str, &str)],
        include_control: bool,
        include_data: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for endpoint_ref in endpoint_permission_targets(endpoint_id, endpoint_uuid) {
            if include_control {
                self.set_endpoint_control_access(&endpoint_ref, subjects)
                    .await?;
            }
            if include_data {
                self.set_endpoint_data_access(&endpoint_ref, subjects)
                    .await?;
            }
        }

        Ok(())
    }

    async fn set_exact_subject_perms(
        &self,
        url: &str,
        perms: &str,
        operation: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = if perms.is_empty() {
            self.client
                .delete(url)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("X-Org-Id", &self.org_id)
                .send()
                .await?
        } else {
            self.client
                .put(url)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("X-Org-Id", &self.org_id)
                .header("Content-Type", "application/json")
                .json(&perms_payload(perms))
                .send()
                .await?
        };

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Set {} returned {}: {}", operation, status, body).into());
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn org_id(&self) -> &str {
        &self.org_id
    }

    #[allow(dead_code)]
    pub fn token(&self) -> &str {
        &self.token
    }
}

fn build_client(timeout_secs: u64) -> Client {
    Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build HTTP client")
}

fn redacted_json(mut value: Value) -> Value {
    redact_json_value(&mut value);
    value
}

fn redact_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if is_sensitive_key(key) {
                    *child = Value::String("[REDACTED]".to_string());
                } else {
                    redact_json_value(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_json_value(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "access_token"
            | "api_key"
            | "application_key"
            | "authorization"
            | "client_secret"
            | "inline_api_key"
            | "password"
            | "private-token"
            | "secret"
            | "token"
    )
}

fn login_token_from_body(body: &Value) -> Option<&str> {
    body["token"]
        .as_str()
        .or_else(|| body["data"]["token"].as_str())
}

fn canonical_access_level(access_level: &str) -> Result<&'static str, String> {
    match access_level.trim().to_ascii_lowercase().as_str() {
        "superadmin" | "super_admin" => Ok("SuperAdmin"),
        "admin" => Ok("Admin"),
        "write" => Ok("Write"),
        "read" => Ok("Read"),
        "none" => Ok("None"),
        other => Err(format!("Unsupported access level '{}'", other)),
    }
}

fn control_perms_for_access_level(access_level: &str) -> Result<&'static str, String> {
    match canonical_access_level(access_level)? {
        "SuperAdmin" => Ok("RCPGDA"),
        "Admin" => Ok("RCPGA"),
        "Write" => Ok("RCA"),
        "Read" => Ok("R"),
        "None" => Ok(""),
        _ => unreachable!(),
    }
}

fn data_perms_for_access_level(access_level: &str) -> Result<&'static str, String> {
    match canonical_access_level(access_level)? {
        "SuperAdmin" | "Admin" => Ok("rwx"),
        "Write" => Ok("rw"),
        "Read" => Ok("r"),
        "None" => Ok(""),
        _ => unreachable!(),
    }
}

fn create_user_payload(
    username: &str,
    password: &str,
    email: &str,
    display_name: &str,
    description: &str,
    access_level: &str,
) -> Result<Value, String> {
    Ok(serde_json::json!({
        "username": username,
        "password": password,
        "description": description,
        "email": email,
        "display_name": display_name,
        "access_level": canonical_access_level(access_level)?
    }))
}

fn perms_payload(perms: &str) -> Value {
    serde_json::json!({
        "perms": perms
    })
}

fn endpoint_permission_targets(endpoint_id: &str, endpoint_uuid: Option<&str>) -> Vec<String> {
    let mut targets = Vec::with_capacity(2);

    for candidate in [Some(endpoint_id), endpoint_uuid] {
        let Some(candidate) = candidate.map(str::trim).filter(|value| !value.is_empty()) else {
            continue;
        };

        if targets.iter().any(|existing| existing == candidate) {
            continue;
        }

        targets.push(candidate.to_string());
    }

    targets
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn login_token_from_body_supports_flat_and_wrapped_responses() {
        assert_eq!(
            login_token_from_body(&json!({ "token": "flat-token" })),
            Some("flat-token")
        );
        assert_eq!(
            login_token_from_body(&json!({ "data": { "token": "wrapped-token" } })),
            Some("wrapped-token")
        );
        assert_eq!(login_token_from_body(&json!({ "status": "ok" })), None);
    }

    #[test]
    fn create_user_payload_matches_humans_contract() {
        let payload = create_user_payload(
            "runner",
            "secret",
            "runner@local.eden",
            "Runner",
            "Scoped runtime user",
            "read",
        )
        .expect("payload should build");

        assert_eq!(
            payload,
            json!({
                "username": "runner",
                "password": "secret",
                "description": "Scoped runtime user",
                "email": "runner@local.eden",
                "display_name": "Runner",
                "access_level": "Read"
            })
        );
        assert!(payload.get("id").is_none());
    }

    #[test]
    fn perms_payload_matches_new_put_contract() {
        assert_eq!(perms_payload("R"), json!({ "perms": "R" }));
        assert_eq!(perms_payload("rw"), json!({ "perms": "rw" }));
    }

    #[test]
    fn endpoint_permission_targets_include_id_and_uuid_without_duplicates() {
        assert_eq!(
            endpoint_permission_targets("tech_cve", Some("2e66937e-b686-4345-8826-74eef5c38454")),
            vec![
                "tech_cve".to_string(),
                "2e66937e-b686-4345-8826-74eef5c38454".to_string()
            ]
        );
        assert_eq!(
            endpoint_permission_targets("tech_cve", Some("tech_cve")),
            vec!["tech_cve".to_string()]
        );
        assert_eq!(
            endpoint_permission_targets("tech_cve", Some("   ")),
            vec!["tech_cve".to_string()]
        );
    }

    #[test]
    fn control_perms_for_access_level_matches_service_mapping() {
        assert_eq!(
            control_perms_for_access_level("SuperAdmin").unwrap(),
            "RCPGDA"
        );
        assert_eq!(control_perms_for_access_level("Admin").unwrap(), "RCPGA");
        assert_eq!(control_perms_for_access_level("Write").unwrap(), "RCA");
        assert_eq!(control_perms_for_access_level("Read").unwrap(), "R");
        assert_eq!(control_perms_for_access_level("None").unwrap(), "");
        assert!(control_perms_for_access_level("owner").is_err());
    }

    #[test]
    fn data_perms_for_access_level_matches_service_mapping() {
        assert_eq!(data_perms_for_access_level("SuperAdmin").unwrap(), "rwx");
        assert_eq!(data_perms_for_access_level("Admin").unwrap(), "rwx");
        assert_eq!(data_perms_for_access_level("Write").unwrap(), "rw");
        assert_eq!(data_perms_for_access_level("Read").unwrap(), "r");
        assert_eq!(data_perms_for_access_level("None").unwrap(), "");
        assert!(data_perms_for_access_level("owner").is_err());
    }

    #[test]
    fn encode_path_segment_escapes_reserved_characters() {
        assert_eq!(
            encode_path_segment("john.doe@company.com/slash space"),
            "john.doe%40company.com%2Fslash%20space"
        );
        assert_eq!(encode_path_segment("adam-demo-runner"), "adam-demo-runner");
    }

    #[test]
    fn redacted_json_masks_nested_secrets() {
        let payload = json!({
            "config": {
                "headers": {
                    "Authorization": "Bearer secret"
                },
                "inline_api_key": "abc123",
                "nested": {
                    "password": "super-secret"
                }
            }
        });

        assert_eq!(
            redacted_json(payload),
            json!({
                "config": {
                    "headers": {
                        "Authorization": "[REDACTED]"
                    },
                    "inline_api_key": "[REDACTED]",
                    "nested": {
                        "password": "[REDACTED]"
                    }
                }
            })
        );
    }
}
