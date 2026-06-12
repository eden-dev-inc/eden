use crate::connection::DatadogConnection;
use datadog_api_client::datadog::{APIKey, Configuration};
use error::EpError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

/// Wraps the official datadog-api-client `Configuration` for typed API calls
/// plus a raw reqwest client used by the `Custom` operation.
#[derive(Debug, Clone)]
pub struct DatadogClient {
    pub dd_config: Configuration,
    raw_client: reqwest::Client,
    site: String,
}

impl Default for DatadogClient {
    fn default() -> Self {
        Self {
            dd_config: Configuration::new(),
            raw_client: reqwest::Client::new(),
            site: "datadoghq.com".to_string(),
        }
    }
}

impl DatadogClient {
    pub async fn new(conn: &DatadogConnection) -> Result<Self, EpError> {
        // ── Official client configuration ──────────────────────────────────
        let mut dd_config = Configuration::new();
        dd_config.set_auth_key("apiKeyAuth", APIKey { key: conn.api_key.clone(), prefix: String::new() });
        if let Some(app_key) = &conn.application_key {
            dd_config.set_auth_key("appKeyAuth", APIKey { key: app_key.clone(), prefix: String::new() });
        }
        // When the site is a full URL (e.g. a wiremock mock server in tests,
        // or a custom on-prem proxy), use server_index=1 which has the
        // "{protocol}://{name}" template with no enum restriction.
        // Otherwise treat it as a standard Datadog site name.
        let site = if conn.site.starts_with("http://") || conn.site.starts_with("https://") {
            let (protocol, rest) = if conn.site.starts_with("https://") {
                ("https", conn.site.trim_start_matches("https://"))
            } else {
                ("http", conn.site.trim_start_matches("http://"))
            };
            let name = rest.trim_end_matches('/').to_string();
            dd_config.server_index = 1;
            dd_config.server_variables.insert("protocol".to_string(), protocol.to_string());
            dd_config.server_variables.insert("name".to_string(), name.clone());
            format!("{protocol}://{name}")
        } else {
            let s = conn.site.trim_end_matches('/').to_string();
            dd_config.server_variables.insert("site".to_string(), s.clone());
            format!("https://api.{s}")
        };

        // ── Raw reqwest client for the Custom operation ─────────────────────
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("dd-api-key"),
            HeaderValue::from_str(&conn.api_key).map_err(|_| EpError::connect("invalid DD-API-KEY header value"))?,
        );
        if let Some(app_key) = &conn.application_key {
            headers.insert(
                HeaderName::from_static("dd-application-key"),
                HeaderValue::from_str(app_key).map_err(|_| EpError::connect("invalid DD-APPLICATION-KEY header value"))?,
            );
        }
        headers.insert(HeaderName::from_static("content-type"), HeaderValue::from_static("application/json"));

        let raw_client = reqwest::Client::builder().default_headers(headers).build().map_err(EpError::connect)?;

        Ok(Self { dd_config, raw_client, site })
    }

    /// Validate API credentials using the Authentication endpoint.
    pub async fn health_check(&self) -> Result<(), EpError> {
        use datadog_api_client::datadogV1::api_authentication::AuthenticationAPI;
        AuthenticationAPI::with_config(self.dd_config.clone()).validate().await.map(|_| ()).map_err(EpError::request)
    }

    // ── Raw HTTP helpers used exclusively by the Custom operation ──────────

    fn base_url(&self) -> String {
        self.site.trim_end_matches('/').to_string()
    }

    fn full_url(&self, path: &str) -> String {
        let base = self.base_url();
        if path.starts_with('/') {
            format!("{base}{path}")
        } else {
            format!("{base}/{path}")
        }
    }

    async fn parse_json(resp: reqwest::Response, op: &str, url: &str) -> Result<Value, EpError> {
        let status = resp.status();
        let body = resp.bytes().await.map_err(EpError::request)?;
        if !status.is_success() {
            return Err(EpError::request(format!(
                "{op} request to {url} failed with status {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        serde_json::from_slice(&body).map_err(|e| EpError::request(format!("invalid JSON in {op} response from {url}: {e}")))
    }

    pub async fn get(&self, path: &str) -> Result<Value, EpError> {
        let url = self.full_url(path);
        let resp = self.raw_client.get(&url).send().await.map_err(EpError::request)?;
        Self::parse_json(resp, "GET", &url).await
    }

    pub async fn post(&self, path: &str, body: Option<Value>) -> Result<Value, EpError> {
        let url = self.full_url(path);
        let mut req = self.raw_client.post(&url);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.map_err(EpError::request)?;
        Self::parse_json(resp, "POST", &url).await
    }

    pub async fn put(&self, path: &str, body: Option<Value>) -> Result<Value, EpError> {
        let url = self.full_url(path);
        let mut req = self.raw_client.put(&url);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.map_err(EpError::request)?;
        Self::parse_json(resp, "PUT", &url).await
    }

    pub async fn delete(&self, path: &str) -> Result<Value, EpError> {
        let url = self.full_url(path);
        let resp = self.raw_client.delete(&url).send().await.map_err(EpError::request)?;
        Self::parse_json(resp, "DELETE", &url).await
    }
}
