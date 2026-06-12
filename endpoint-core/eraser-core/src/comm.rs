use crate::connection::EraserConnection;
use error::EpError;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::Value;
use std::time::Duration;

const DEFAULT_BASE_URL: &str = "https://app.eraser.io";

#[derive(Debug, Clone)]
pub struct EraserClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl Default for EraserClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: String::new(),
        }
    }
}

impl EraserClient {
    pub async fn new(conn: &EraserConnection) -> Result<Self, EpError> {
        let mut default_headers = HeaderMap::new();
        let auth_value = if conn.api_key.starts_with("Bearer ") {
            conn.api_key.clone()
        } else {
            format!("Bearer {}", conn.api_key)
        };
        default_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).map_err(|_| EpError::connect("invalid API key format"))?,
        );
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client =
            Client::builder().default_headers(default_headers).timeout(Duration::from_secs(120)).build().map_err(EpError::connect)?;

        let base_url = conn.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/').to_string();

        Ok(Self { client, base_url, api_key: conn.api_key.clone() })
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        // Use a minimal render/prompt call as a health check.
        let body = serde_json::json!({
            "text": "health check",
            "diagramType": "sequence-diagram",
        });

        let response =
            self.client.post(format!("{}/api/render/prompt", self.base_url)).json(&body).send().await.map_err(EpError::request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("Eraser health check failed with status: {}", response.status())))
        }
    }

    /// Parse an HTTP response into a JSON value, returning an error on non-success status.
    async fn parse_json(response: reqwest::Response, method: &str, path: &str) -> Result<Value, EpError> {
        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Eraser {method} {path} failed with status {status}: {body_text}")));
        }

        // Some endpoints (e.g. DELETE) may return empty body on success.
        if response_bytes.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_slice(&response_bytes).map_err(|e| EpError::request(format!("invalid JSON in Eraser response from {path}: {e}")))
    }

    /// Execute a GET request against an Eraser API endpoint.
    pub async fn get(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.client.get(&url).send().await.map_err(EpError::request)?;
        Self::parse_json(response, "GET", path).await
    }

    /// Execute a POST request against an Eraser API endpoint.
    pub async fn post(&self, path: &str, body: Value) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.client.post(&url).json(&body).send().await.map_err(EpError::request)?;
        Self::parse_json(response, "POST", path).await
    }

    /// Execute a PUT request against an Eraser API endpoint.
    pub async fn put(&self, path: &str, body: Value) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.client.put(&url).json(&body).send().await.map_err(EpError::request)?;
        Self::parse_json(response, "PUT", path).await
    }

    /// Execute a DELETE request against an Eraser API endpoint.
    pub async fn delete(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.client.delete(&url).send().await.map_err(EpError::request)?;
        Self::parse_json(response, "DELETE", path).await
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}
