use crate::connection::PineconeConnection;
use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use futures::Future;
use hyper::header::CONTENT_TYPE;
use reqwest::{Client, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
pub enum Req {
    Upsert(String),
    Query(String),
    Fetch(Vec<String>, Option<String>),
    Update(String),
    Delete(String),
    List(Option<String>, Option<String>, Option<String>, Option<String>),
    DescribeIndexStats,
}

pub trait PineconeRequests {
    fn delete(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn describe_index_stats(&self) -> impl Future<Output = Result<Value, EpError>>;
    fn fetch(&self, ids: &[String], namespace: &Option<String>) -> impl Future<Output = Result<Value, EpError>>;
    fn list(
        &self,
        prefix: Option<String>,
        namespace: Option<String>,
        limit: Option<String>,
        pagination_token: Option<String>,
    ) -> impl Future<Output = Result<Value, EpError>>;
    fn query(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn update(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn upsert(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
}

#[derive(Debug, Clone, Default)]
pub struct PineconeClient {
    client: Client, // reqwest client
    url: String,    // base url
}

async fn parse_json_response(resp: reqwest::Response, operation: &str) -> Result<Value, EpError> {
    let status = resp.status();
    let body = resp.bytes().await.map_err(EpError::request)?;
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        return Err(EpError::request(format!("{operation} failed with status {status}: {body_text}")));
    }

    serde_json::from_slice(&body).map_err(|e| EpError::request(format!("invalid JSON in {operation} response: {e}")))
}

impl PineconeClient {
    pub async fn new(conn: &PineconeConnection) -> Result<Self, EpError> {
        let mut header = HeaderMap::new();
        header.insert(
            "Api-key",
            conn.token.parse().map_err(|e| EpError::request(format!("Invalid API key header: {}", e)))?,
        );
        header.insert(
            CONTENT_TYPE,
            "application/json".parse().map_err(|e| EpError::request(format!("Invalid content type header: {}", e)))?,
        );

        let client = Self {
            client: Client::builder().https_only(false).default_headers(header).build().map_err(EpError::connect)?,
            url: conn.url.to_owned(),
        };
        client.describe_index_stats().await?;
        Ok(client)
    }
}

impl PineconeRequests for PineconeClient {
    async fn delete(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/vectors/delete").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "delete").await
    }
    async fn describe_index_stats(&self) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/describe_index_stats").body("{}").send().await.map_err(EpError::request)?;

        parse_json_response(resp, "describe_index_stats").await
    }
    async fn fetch(&self, ids: &[String], namespace: &Option<String>) -> Result<Value, EpError> {
        let mut params = "?".to_string();

        for i in ids {
            params.push_str(&format!("ids={i}&"));
        }
        if let Some(namespace) = namespace {
            params.push_str(&format!("namespace={namespace}"));
        } else {
            params.remove(params.len() - 1); // removes last char either '?' or '&' (if namespace is None
        }

        let resp = self.client.get(self.url.to_string() + "/vectors/fetch" + &params).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "fetch").await
    }

    async fn list(
        &self,
        prefix: Option<String>,
        namespace: Option<String>,
        limit: Option<String>,
        pagination_token: Option<String>,
    ) -> Result<Value, EpError> {
        let mut params = "?".to_string();
        if let Some(prefix) = prefix {
            params.push_str(&format!("prefix={prefix}&"));
        }
        if let Some(namespace) = namespace {
            params.push_str(&format!("namespace={namespace}&"));
        }
        if let Some(limit) = limit {
            params.push_str(&format!("limit={limit}&"));
        }
        if let Some(pagination_token) = pagination_token {
            params.push_str(&format!("pagination_token={pagination_token}&"));
        }

        params.remove(params.len() - 1); // removes last char either '?' or '&'

        let resp = self.client.get(self.url.to_string() + "/vectors/list" + &params).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "list").await
    }
    async fn query(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/query").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "query").await
    }
    async fn update(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/vectors/update").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "update").await
    }
    async fn upsert(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/vectors/upsert").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "upsert").await
    }
}

#[cfg(test)]
mod tests {
    use super::{PineconeClient, PineconeRequests};
    use httpmock::prelude::*;
    use reqwest::Client;

    fn ensure_rustls_provider() {
        // Reqwest + rustls in test context may not have a default provider selected.
        // Install one explicitly so HTTPS mock-server tests are deterministic in CI.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    fn test_client() -> Client {
        ensure_rustls_provider();
        Client::builder().danger_accept_invalid_certs(true).build().expect("test client")
    }

    #[tokio::test]
    async fn upsert_rejects_non_2xx_with_error_body() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/vectors/upsert");
                then.status(400).header("content-type", "application/json").body(r#"{"error":"bad vectors"}"#);
            })
            .await;

        let client = PineconeClient { client: test_client(), url: server.base_url() };

        let result = client.upsert(r#"{"vectors":[]}"#.to_string()).await;
        assert!(result.is_err(), "non-2xx should return Err");

        let error = result.expect_err("expected Err").to_string();
        assert!(error.contains("400"), "error should include status code: {error}");
        assert!(error.contains("bad vectors"), "error should include response body: {error}");
    }

    #[tokio::test]
    async fn upsert_parses_success_json() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/vectors/upsert");
                then.status(200).header("content-type", "application/json").body(r#"{"upsertedCount":2}"#);
            })
            .await;

        let client = PineconeClient { client: test_client(), url: server.base_url() };

        let result = client
            .upsert(r#"{"vectors":[{"id":"1","values":[0.1,0.2]}]}"#.to_string())
            .await
            .expect("successful response should parse");

        assert_eq!(result["upsertedCount"], 2);
    }

    #[tokio::test]
    async fn upsert_rejects_malformed_utf8_even_on_2xx() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/vectors/upsert");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(vec![b'{', b'"', b'v', b'a', b'l', b'u', b'e', b'"', b':', b'"', 0xff, b'"', b'}']);
            })
            .await;

        let client = PineconeClient { client: test_client(), url: server.base_url() };
        let result = client.upsert(r#"{"vectors":[]}"#.to_string()).await;
        assert!(result.is_err(), "malformed utf-8 in 2xx JSON should return Err");
        let error = result.expect_err("expected Err").to_string();
        assert!(error.contains("invalid JSON"), "error should indicate parse failure: {error}");
    }
}
