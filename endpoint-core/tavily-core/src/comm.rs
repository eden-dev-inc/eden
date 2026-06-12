use crate::connection::TavilyConnection;
use error::EpError;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::Value;

const DEFAULT_BASE_URL: &str = "https://api.tavily.com";

#[derive(Debug, Clone)]
pub struct TavilyClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl Default for TavilyClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: String::new(),
        }
    }
}

impl TavilyClient {
    pub async fn new(conn: &TavilyConnection) -> Result<Self, EpError> {
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

        let client = Client::builder().default_headers(default_headers).build().map_err(EpError::connect)?;

        let base_url = conn.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/').to_string();

        Ok(Self { client, base_url, api_key: conn.api_key.clone() })
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        // Tavily doesn't have a dedicated health endpoint; use a minimal search as a health check.
        let body = serde_json::json!({
            "query": "health check",
            "max_results": 1,
            "search_depth": "basic"
        });

        let response = self.client.post(format!("{}/search", self.base_url)).json(&body).send().await.map_err(EpError::request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("Tavily health check failed with status: {}", response.status())))
        }
    }

    /// Execute a POST request against a Tavily API endpoint.
    pub async fn post(&self, path: &str, body: Value) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.client.post(&url).json(&body).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Tavily POST {path} failed with status {status}: {body_text}")));
        }

        serde_json::from_slice(&response_bytes).map_err(|e| EpError::request(format!("invalid JSON in Tavily response from {path}: {e}")))
    }

    /// Execute a GET request against a Tavily API endpoint.
    pub async fn get(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.client.get(&url).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Tavily GET {path} failed with status {status}: {body_text}")));
        }

        serde_json::from_slice(&response_bytes).map_err(|e| EpError::request(format!("invalid JSON in Tavily response from {path}: {e}")))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}
