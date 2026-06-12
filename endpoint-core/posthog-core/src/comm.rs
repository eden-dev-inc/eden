use crate::connection::PosthogConnection;
use error::{ConnectError, EpError};
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

const DEFAULT_BASE_URL: &str = "https://us.posthog.com";

#[derive(Debug, Clone)]
pub struct PosthogClient {
    client: Client,
    base_url: String,
}

impl Default for PosthogClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

impl PosthogClient {
    pub async fn new(conn: &PosthogConnection) -> Result<Self, EpError> {
        let mut default_headers = HeaderMap::new();
        let auth_value = format!("Bearer {}", conn.api_key);
        default_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).map_err(|_| EpError::connect("invalid API key format"))?,
        );
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        default_headers.insert("Accept", HeaderValue::from_static("application/json"));
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Eve"));

        let client = Client::builder().default_headers(default_headers).build().map_err(EpError::connect)?;

        let host = conn.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/');
        let base_url = format!("{}/api/projects/{}", host, conn.project_id);

        Ok(Self { client, base_url })
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let response = self.client.get(format!("{}/", self.base_url)).send().await.map_err(EpError::request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("PostHog health check failed with status: {}", response.status())))
        }
    }

    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        headers: Option<&HashMap<String, String>>,
    ) -> Result<Value, EpError> {
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
        let builder = apply_headers(builder, headers)?;

        let response = builder.send().await.map_err(EpError::request)?;
        let status = response.status();
        let response_bytes = response.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&response_bytes);
            return Err(EpError::request(format!("PostHog {method} {path} failed with status {status}: {body_text}")));
        }

        if response_bytes.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_slice(&response_bytes).map_err(|e| EpError::request(format!("invalid JSON in PostHog response from {path}: {e}")))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

fn apply_headers(
    mut builder: reqwest::RequestBuilder,
    headers: Option<&HashMap<String, String>>,
) -> Result<reqwest::RequestBuilder, EpError> {
    if let Some(headers) = headers {
        for (name, value) in headers {
            let header_name = HeaderName::from_str(name).map_err(|_| EpError::Connect(ConnectError::InvalidHeaderName))?;
            if header_name == AUTHORIZATION {
                return Err(EpError::request("custom PostHog requests cannot override the Authorization header"));
            }
            let header_value = HeaderValue::from_str(value).map_err(|_| EpError::Connect(ConnectError::InvalidHeaderValue))?;
            builder = builder.header(header_name, header_value);
        }
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::GET;
    use httpmock::MockServer;

    fn test_connection(server: &MockServer) -> PosthogConnection {
        PosthogConnection {
            api_key: "phx_test".to_string(),
            project_id: "123".to_string(),
            base_url: Some(format!("http://{}", server.address())),
        }
    }

    #[tokio::test]
    async fn request_applies_custom_headers_without_dropping_defaults() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/projects/123/events")
                .header("authorization", "Bearer phx_test")
                .header("accept", "application/json")
                .header("x-test-header", "enabled");
            then.status(200).header("content-type", "application/json").body(r#"{"ok":true}"#);
        });

        let client = PosthogClient::new(&test_connection(&server)).await.unwrap();
        let headers = HashMap::from([("x-test-header".to_string(), "enabled".to_string())]);

        let result = client.request("GET", "/events", None, Some(&headers)).await.unwrap();

        assert_eq!(result["ok"], true);
        mock.assert();
    }

    #[tokio::test]
    async fn request_rejects_invalid_header_names() {
        let client = PosthogClient::default();
        let headers = HashMap::from([("bad header".to_string(), "value".to_string())]);

        let err = client.request("GET", "/events", None, Some(&headers)).await.unwrap_err();

        assert!(matches!(err, EpError::Connect(ConnectError::InvalidHeaderName)));
    }

    #[tokio::test]
    async fn request_rejects_authorization_header_overrides() {
        let client = PosthogClient::default();
        let headers = HashMap::from([("authorization".to_string(), "Bearer user-supplied".to_string())]);

        let err = client.request("GET", "/events", None, Some(&headers)).await.unwrap_err();

        assert!(matches!(err, EpError::Request(_)));
        assert!(err.to_string().contains("cannot override the Authorization header"));
    }
}
