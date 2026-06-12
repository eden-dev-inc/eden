use crate::connection::SalesforceConnection;
use error::EpError;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::{Value, json};

const DEFAULT_API_VERSION: &str = "v60.0";
const TOKEN_PATH: &str = "/services/oauth2/token";

#[derive(Debug, Clone)]
pub struct SalesforceClient {
    client: Client,
    instance_url: String,
    api_version: String,
    access_token: String,
}

impl Default for SalesforceClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            instance_url: String::new(),
            api_version: DEFAULT_API_VERSION.to_string(),
            access_token: String::new(),
        }
    }
}

impl SalesforceClient {
    pub async fn new(conn: &SalesforceConnection) -> Result<Self, EpError> {
        let instance_url = conn.instance_url.trim_end_matches('/').to_string();
        let api_version = conn.api_version.as_deref().unwrap_or(DEFAULT_API_VERSION).to_string();

        let access_token = if let Some(token) = &conn.access_token {
            token.clone()
        } else {
            let username = conn
                .username
                .as_deref()
                .ok_or_else(|| EpError::connect("Salesforce connection requires either `access_token` or `username` + `password`"))?;
            let password = conn
                .password
                .as_deref()
                .ok_or_else(|| EpError::connect("Salesforce connection requires either `access_token` or `username` + `password`"))?;

            let token_url = format!("{}{}", instance_url, TOKEN_PATH);
            let http = Client::new();
            let params = [
                ("grant_type", "password"),
                ("client_id", &conn.client_id),
                ("client_secret", &conn.client_secret),
                ("username", username),
                ("password", password),
            ];

            let response = http.post(&token_url).form(&params).send().await.map_err(EpError::connect)?;

            let status = response.status();
            let body_bytes = response.bytes().await.map_err(EpError::connect)?;

            if !status.is_success() {
                let body_text = String::from_utf8_lossy(&body_bytes);
                return Err(EpError::connect(format!(
                    "Salesforce OAuth2 token request failed with status {status}: {body_text}"
                )));
            }

            let body: Value = serde_json::from_slice(&body_bytes)
                .map_err(|e| EpError::connect(format!("invalid JSON in Salesforce token response: {e}")))?;

            body["access_token"]
                .as_str()
                .ok_or_else(|| EpError::connect("Salesforce token response missing `access_token` field"))?
                .to_string()
        };

        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {access_token}"))
                .map_err(|_| EpError::connect("invalid Salesforce access token format"))?,
        );
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client = Client::builder().default_headers(default_headers).build().map_err(EpError::connect)?;

        Ok(Self { client, instance_url, api_version, access_token })
    }

    fn base_data_url(&self) -> String {
        format!("{}/services/data/{}", self.instance_url, self.api_version)
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let url = format!("{}/limits", self.base_data_url());
        let response = self.client.get(&url).send().await.map_err(EpError::request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("Salesforce health check failed with status: {}", response.status())))
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_data_url(), path);
        let response = self.client.get(&url).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Salesforce GET {path} failed with status {status}: {body_text}")));
        }

        if response_bytes.is_empty() {
            return Ok(json!({"success": true}));
        }

        serde_json::from_slice(&response_bytes)
            .map_err(|e| EpError::request(format!("invalid JSON in Salesforce response from {path}: {e}")))
    }

    pub async fn get_with_query(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_data_url(), path);
        let response = self.client.get(&url).query(query).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Salesforce GET {path} failed with status {status}: {body_text}")));
        }

        if response_bytes.is_empty() {
            return Ok(json!({"success": true}));
        }

        serde_json::from_slice(&response_bytes)
            .map_err(|e| EpError::request(format!("invalid JSON in Salesforce response from {path}: {e}")))
    }

    pub async fn post(&self, path: &str, body: Value) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_data_url(), path);
        let response = self.client.post(&url).json(&body).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Salesforce POST {path} failed with status {status}: {body_text}")));
        }

        if response_bytes.is_empty() {
            return Ok(json!({"success": true}));
        }

        serde_json::from_slice(&response_bytes)
            .map_err(|e| EpError::request(format!("invalid JSON in Salesforce response from {path}: {e}")))
    }

    pub async fn patch(&self, path: &str, body: Value) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_data_url(), path);
        let response = self.client.patch(&url).json(&body).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Salesforce PATCH {path} failed with status {status}: {body_text}")));
        }

        if response_bytes.is_empty() {
            return Ok(json!({"success": true}));
        }

        serde_json::from_slice(&response_bytes)
            .map_err(|e| EpError::request(format!("invalid JSON in Salesforce response from {path}: {e}")))
    }

    pub async fn delete(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_data_url(), path);
        let response = self.client.delete(&url).send().await.map_err(EpError::request)?;

        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("Salesforce DELETE {path} failed with status {status}: {body_text}")));
        }

        Ok(json!({"success": true}))
    }

    pub fn instance_url(&self) -> &str {
        &self.instance_url
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }
}
