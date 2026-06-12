use crate::connection::GoogleWorkspaceConnection;
use error::EpError;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const TOKEN_INFO_URL: &str = "https://oauth2.googleapis.com/tokeninfo";

const GMAIL_BASE_URL: &str = "https://gmail.googleapis.com/gmail/v1";
const DRIVE_BASE_URL: &str = "https://www.googleapis.com/drive/v3";
const CALENDAR_BASE_URL: &str = "https://www.googleapis.com/calendar/v3";
const SHEETS_BASE_URL: &str = "https://sheets.googleapis.com/v4/spreadsheets";
const DOCS_BASE_URL: &str = "https://docs.googleapis.com/v1/documents";
const CHAT_BASE_URL: &str = "https://chat.googleapis.com/v1";
const TASKS_BASE_URL: &str = "https://tasks.googleapis.com/tasks/v1";
const MEET_BASE_URL: &str = "https://meet.googleapis.com/v2";
const PEOPLE_BASE_URL: &str = "https://people.googleapis.com/v1";

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    expires_in: Option<u64>,
    #[allow(dead_code)]
    token_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GoogleWorkspaceClient {
    client: Client,
    access_token: Arc<RwLock<String>>,
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl Default for GoogleWorkspaceClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            access_token: Arc::new(RwLock::new(String::new())),
            client_id: String::new(),
            client_secret: String::new(),
            refresh_token: String::new(),
        }
    }
}

impl GoogleWorkspaceClient {
    pub async fn new(conn: &GoogleWorkspaceConnection) -> Result<Self, EpError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Eve"));

        let client = Client::builder().default_headers(default_headers).build().map_err(EpError::connect)?;

        let gw_client = Self {
            client,
            access_token: Arc::new(RwLock::new(String::new())),
            client_id: conn.client_id.clone(),
            client_secret: conn.client_secret.clone(),
            refresh_token: conn.refresh_token.clone(),
        };

        gw_client.do_token_refresh().await?;

        Ok(gw_client)
    }

    async fn do_token_refresh(&self) -> Result<(), EpError> {
        let params = [
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.as_str()),
            ("refresh_token", self.refresh_token.as_str()),
            ("grant_type", "refresh_token"),
        ];

        let response = self.client.post(TOKEN_URL).form(&params).send().await.map_err(EpError::connect)?;

        let status = response.status();
        let body = response.bytes().await.map_err(EpError::connect)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&body);
            return Err(EpError::auth(format!("Google OAuth2 token refresh failed with status {status}: {body_text}")));
        }

        let token_response: TokenResponse =
            serde_json::from_slice(&body).map_err(|e| EpError::auth(format!("failed to parse token response: {e}")))?;

        let mut token = self.access_token.write().await;
        *token = token_response.access_token;

        Ok(())
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let token = self.access_token.read().await.clone();
        let response = self
            .client
            .post(TOKEN_INFO_URL)
            .header(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| EpError::auth("invalid token format"))?,
            )
            .send()
            .await
            .map_err(EpError::request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("Google Workspace health check failed with status: {}", response.status())))
        }
    }

    pub async fn request(
        &self,
        service: &str,
        method: &str,
        path: &str,
        body: Option<Value>,
        query_params: Option<Value>,
    ) -> Result<Value, EpError> {
        let base_url = service_base_url(service)?;
        let url = if path.is_empty() || path == "/" {
            base_url.to_string()
        } else {
            let path = path.trim_start_matches('/');
            format!("{base_url}/{path}")
        };

        let result = self.execute_request(method, &url, body.clone(), query_params.clone()).await;

        match result {
            Err(ref e) if is_auth_error(e) => {
                self.do_token_refresh().await?;
                self.execute_request(method, &url, body, query_params).await
            }
            other => other,
        }
    }

    async fn execute_request(&self, method: &str, url: &str, body: Option<Value>, query_params: Option<Value>) -> Result<Value, EpError> {
        let token = self.access_token.read().await.clone();

        let builder = match method.to_uppercase().as_str() {
            "GET" => self.client.get(url),
            "POST" => self.client.post(url),
            "PUT" => self.client.put(url),
            "PATCH" => self.client.patch(url),
            "DELETE" => self.client.delete(url),
            _ => return Err(EpError::request(format!("unsupported HTTP method: {method}"))),
        };

        let builder = builder.header(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| EpError::auth("invalid token format"))?,
        );

        let builder = if let Some(ref qp) = query_params {
            if let Some(obj) = qp.as_object() {
                let pairs: Vec<(String, String)> = obj
                    .iter()
                    .map(|(k, v)| {
                        let val = match v {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        (k.clone(), val)
                    })
                    .collect();
                builder.query(&pairs)
            } else {
                builder
            }
        } else {
            builder
        };

        let builder = if let Some(body) = body { builder.json(&body) } else { builder };

        let response = builder.send().await.map_err(EpError::request)?;
        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!(
                "Google Workspace {method} {url} failed with status {status}: {body_text}"
            )));
        }

        if response_bytes.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_slice(&response_bytes).map_err(|e| EpError::request(format!("invalid JSON in Google Workspace response: {e}")))
    }
}

fn service_base_url(service: &str) -> Result<&'static str, EpError> {
    match service.to_lowercase().as_str() {
        "gmail" => Ok(GMAIL_BASE_URL),
        "drive" => Ok(DRIVE_BASE_URL),
        "calendar" => Ok(CALENDAR_BASE_URL),
        "sheets" => Ok(SHEETS_BASE_URL),
        "docs" => Ok(DOCS_BASE_URL),
        "chat" => Ok(CHAT_BASE_URL),
        "tasks" => Ok(TASKS_BASE_URL),
        "meet" => Ok(MEET_BASE_URL),
        "people" => Ok(PEOPLE_BASE_URL),
        _ => Err(EpError::request(format!(
            "unsupported Google Workspace service: {service}. Supported: gmail, drive, calendar, sheets, docs, chat, tasks, meet, people"
        ))),
    }
}

fn is_auth_error(err: &EpError) -> bool {
    let msg = err.to_string();
    msg.contains("401") || msg.contains("Unauthorized")
}
