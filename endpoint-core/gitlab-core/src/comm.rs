use crate::connection::GitlabConnection;
use error::EpError;
use reqwest::Client;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::Value;

const DEFAULT_BASE_URL: &str = "https://gitlab.com";

#[derive(Debug, Clone)]
pub struct GitlabClient {
    client: Client,
    base_url: String,
    token: String,
}

impl Default for GitlabClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            token: String::new(),
        }
    }
}

impl GitlabClient {
    pub async fn new(conn: &GitlabConnection) -> Result<Self, EpError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            "PRIVATE-TOKEN",
            HeaderValue::from_str(&conn.token).map_err(|_| EpError::connect("invalid token format"))?,
        );
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        default_headers.insert("Accept", HeaderValue::from_static("application/json"));
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Eve"));

        let client = Client::builder().default_headers(default_headers).build().map_err(EpError::connect)?;

        let base_url = conn.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/').to_string();

        // Ensure the base_url includes /api/v4
        let base_url = if base_url.ends_with("/api/v4") {
            base_url
        } else {
            format!("{}/api/v4", base_url)
        };

        Ok(Self { client, base_url, token: conn.token.clone() })
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let response = self.client.get(format!("{}/version", self.base_url)).send().await.map_err(EpError::request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("GitLab health check failed with status: {}", response.status())))
        }
    }

    pub async fn request(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let builder = match method.to_uppercase().as_str() {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "PATCH" => self.client.patch(&url),
            "DELETE" => self.client.delete(&url),
            _ => return Err(EpError::request(format!("unsupported HTTP method: {method}"))),
        };

        let builder = if let Some(body) = body { builder.json(&body) } else { builder };

        let response = builder.send().await.map_err(EpError::request)?;
        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("GitLab {method} {path} failed with status {status}: {body_text}")));
        }

        if response_bytes.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_slice(&response_bytes).map_err(|e| EpError::request(format!("invalid JSON in GitLab response from {path}: {e}")))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}
