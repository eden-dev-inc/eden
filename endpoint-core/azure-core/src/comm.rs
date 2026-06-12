use crate::connection::AzureConnection;
use error::EpError;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct AzureClient {
    client: Client,
    subscription_id: Option<String>,
    base_url: String,
    auth_url: String,
    tenant_id: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    token: Arc<RwLock<TokenState>>,
}

#[derive(Debug, Clone)]
struct TokenState {
    access_token: String,
    expiry: Instant,
}

impl AzureClient {
    pub async fn new(conn: &AzureConnection) -> Result<Self, EpError> {
        let tenant_id = conn.tenant_id.trim();
        if tenant_id.is_empty() {
            return Err(EpError::connect("Azure connection tenant_id cannot be empty"));
        }

        let base_url = conn.endpoint_url.as_deref().unwrap_or("https://management.azure.com").trim_end_matches('/').to_string();

        let auth_url = "https://login.microsoftonline.com".to_string();

        let client = Client::builder().build().map_err(EpError::connect)?;

        let token_state = if let Some(token) = &conn.access_token {
            // Pre-provided token — assume valid for 1 hour
            TokenState {
                access_token: token.clone(),
                expiry: Instant::now() + Duration::from_secs(3600),
            }
        } else {
            // Acquire via OAuth2 client_credentials
            let client_id =
                conn.client_id.as_deref().ok_or_else(|| EpError::connect("Azure client_id required when access_token not provided"))?;
            let client_secret = conn
                .client_secret
                .as_deref()
                .ok_or_else(|| EpError::connect("Azure client_secret required when access_token not provided"))?;

            acquire_token(&client, &auth_url, tenant_id, client_id, client_secret, &base_url).await?
        };

        Ok(Self {
            client,
            subscription_id: conn.subscription_id.clone(),
            base_url,
            auth_url,
            tenant_id: tenant_id.to_string(),
            client_id: conn.client_id.clone(),
            client_secret: conn.client_secret.clone(),
            token: Arc::new(RwLock::new(token_state)),
        })
    }

    pub fn subscription_id(&self) -> Option<&str> {
        self.subscription_id.as_deref()
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let token = self.get_token().await?;
        let url = format!("{}/subscriptions?api-version=2022-12-01", self.base_url);

        let resp = self.client.get(&url).header("Authorization", format!("Bearer {token}")).send().await.map_err(EpError::request)?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("Azure health check failed with status: {}", resp.status())))
        }
    }

    /// Execute an Azure REST API request against the management plane.
    pub async fn execute(
        &self,
        method: &str,
        path: &str,
        api_version: &str,
        body: Option<&Value>,
        extra_query: Option<&str>,
    ) -> Result<Value, EpError> {
        let url = build_url(&self.base_url, path, api_version, extra_query);
        self.execute_raw(method, &url, body).await
    }

    /// Execute an Azure REST API request against a data plane endpoint.
    /// Used for services like Key Vault, Storage, Cosmos DB where the base URL differs.
    pub async fn execute_data_plane(
        &self,
        data_plane_url: &str,
        method: &str,
        path: &str,
        api_version: &str,
        body: Option<&Value>,
    ) -> Result<Value, EpError> {
        let base = data_plane_url.trim_end_matches('/');
        let url = build_url(base, path, api_version, None);
        self.execute_raw(method, &url, body).await
    }

    async fn execute_raw(&self, method: &str, url: &str, body: Option<&Value>) -> Result<Value, EpError> {
        let token = self.get_token().await?;

        let http_method: reqwest::Method = method.parse().map_err(|_| EpError::request(format!("invalid HTTP method '{method}'")))?;

        let mut builder = self
            .client
            .request(http_method, url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json");

        if let Some(b) = body {
            let body_bytes = serde_json::to_vec(b).map_err(EpError::serde)?;
            builder = builder.body(body_bytes);
        }

        let resp = builder.send().await.map_err(EpError::request)?;

        let status = resp.status();
        let resp_bytes = resp.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&resp_bytes);
            return Err(EpError::request(format!("Azure request to {url} failed with status {status}: {body_text}")));
        }

        if resp_bytes.is_empty() {
            return Ok(Value::Null);
        }

        match serde_json::from_slice::<Value>(&resp_bytes) {
            Ok(v) => Ok(v),
            Err(_) => Ok(Value::String(String::from_utf8_lossy(&resp_bytes).into_owned())),
        }
    }

    async fn get_token(&self) -> Result<String, EpError> {
        // Check if current token is still valid (with 5 min buffer)
        {
            let state = self.token.read().await;
            if Instant::now() + Duration::from_secs(300) < state.expiry {
                return Ok(state.access_token.clone());
            }
        }

        // Token expired or near expiry — refresh
        let client_id = self.client_id.as_deref().ok_or_else(|| EpError::auth("cannot refresh Azure token: no client_id"))?;
        let client_secret = self.client_secret.as_deref().ok_or_else(|| EpError::auth("cannot refresh Azure token: no client_secret"))?;

        let new_state = acquire_token(&self.client, &self.auth_url, &self.tenant_id, client_id, client_secret, &self.base_url).await?;

        let token = new_state.access_token.clone();
        let mut state = self.token.write().await;
        *state = new_state;
        Ok(token)
    }
}

fn build_url(base: &str, path: &str, api_version: &str, extra_query: Option<&str>) -> String {
    let separator = if path.contains('?') { "&" } else { "?" };
    match extra_query {
        Some(q) => format!("{base}{path}{separator}api-version={api_version}&{q}"),
        None => format!("{base}{path}{separator}api-version={api_version}"),
    }
}

async fn acquire_token(
    client: &Client,
    auth_url: &str,
    tenant_id: &str,
    client_id: &str,
    client_secret: &str,
    resource_url: &str,
) -> Result<TokenState, EpError> {
    let token_url = format!("{auth_url}/{tenant_id}/oauth2/v2.0/token");
    let scope = format!("{resource_url}/.default");

    let resp = client
        .post(&token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", &scope),
        ])
        .send()
        .await
        .map_err(|e| EpError::auth(format!("failed to acquire Azure OAuth2 token: {e}")))?;

    let status = resp.status();
    let body: Value = resp.json().await.map_err(|e| EpError::auth(format!("failed to parse Azure token response: {e}")))?;

    if !status.is_success() {
        return Err(EpError::auth(format!("Azure OAuth2 token request failed with status {status}: {body}")));
    }

    let access_token = body["access_token"]
        .as_str()
        .ok_or_else(|| EpError::auth("Azure token response missing access_token field"))?
        .to_string();

    let expires_in = body["expires_in"].as_u64().unwrap_or(3600);

    Ok(TokenState {
        access_token,
        expiry: Instant::now() + Duration::from_secs(expires_in),
    })
}
