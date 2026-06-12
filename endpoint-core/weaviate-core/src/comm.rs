use crate::connection::WeaviateConnection;
use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use futures::Future;
use hyper::header::CONTENT_TYPE;
use reqwest::{Client, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
pub enum Req {
    GraphQL(String),
    CreateObject(String),
    GetObject(String, String),
    ListObjects(Option<String>, Option<u32>),
    UpdateObject(String, String, String),
    DeleteObject(String, String),
    BatchObjects(String),
    GetSchema,
    HealthCheck,
}

pub trait WeaviateRequests {
    fn graphql(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn create_object(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn get_object(&self, class: &str, id: &str) -> impl Future<Output = Result<Value, EpError>>;
    fn list_objects(&self, class: Option<&str>, limit: Option<u32>) -> impl Future<Output = Result<Value, EpError>>;
    fn update_object(&self, class: &str, id: &str, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn delete_object(&self, class: &str, id: &str) -> impl Future<Output = Result<Value, EpError>>;
    fn batch_objects(&self, body: String) -> impl Future<Output = Result<Value, EpError>>;
    fn get_schema(&self) -> impl Future<Output = Result<Value, EpError>>;
    fn health_check(&self) -> impl Future<Output = Result<Value, EpError>>;
}

#[derive(Debug, Clone, Default)]
pub struct WeaviateClient {
    client: Client,
    url: String,
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

async fn parse_optional_json_response(resp: reqwest::Response, operation: &str) -> Result<Value, EpError> {
    let status = resp.status();
    let body = resp.bytes().await.map_err(EpError::request)?;
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        return Err(EpError::request(format!("{operation} failed with status {status}: {body_text}")));
    }

    if body.is_empty() {
        Ok(Value::Object(serde_json::Map::new()))
    } else {
        serde_json::from_slice(&body).map_err(|e| EpError::request(format!("invalid JSON in {operation} response: {e}")))
    }
}

impl WeaviateClient {
    pub async fn new(conn: &WeaviateConnection) -> Result<Self, EpError> {
        let mut header = HeaderMap::new();
        if !conn.token.is_empty() {
            header.insert(
                "Authorization",
                format!("Bearer {}", conn.token).parse().map_err(|e| EpError::request(format!("Invalid authorization header: {}", e)))?,
            );
        }
        header.insert(
            CONTENT_TYPE,
            "application/json".parse().map_err(|e| EpError::request(format!("Invalid content type header: {}", e)))?,
        );

        let client = Self {
            client: Client::builder().https_only(false).default_headers(header).build().map_err(EpError::connect)?,
            url: conn.url.to_owned(),
        };
        client.health_check().await?;
        Ok(client)
    }
}

impl WeaviateRequests for WeaviateClient {
    async fn graphql(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/v1/graphql").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "graphql").await
    }

    async fn create_object(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/v1/objects").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "create_object").await
    }

    async fn get_object(&self, class: &str, id: &str) -> Result<Value, EpError> {
        let resp = self.client.get(format!("{}/v1/objects/{}/{}", self.url, class, id)).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "get_object").await
    }

    async fn list_objects(&self, class: Option<&str>, limit: Option<u32>) -> Result<Value, EpError> {
        let mut params = vec![];
        if let Some(class) = class {
            params.push(format!("class={class}"));
        }
        if let Some(limit) = limit {
            params.push(format!("limit={limit}"));
        }

        let query_string = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params.join("&"))
        };

        let resp = self.client.get(format!("{}/v1/objects{}", self.url, query_string)).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "list_objects").await
    }

    async fn update_object(&self, class: &str, id: &str, body: String) -> Result<Value, EpError> {
        let resp = self
            .client
            .patch(format!("{}/v1/objects/{}/{}", self.url, class, id))
            .body(body)
            .send()
            .await
            .map_err(EpError::request)?;

        parse_optional_json_response(resp, "update").await
    }

    async fn delete_object(&self, class: &str, id: &str) -> Result<Value, EpError> {
        let resp = self.client.delete(format!("{}/v1/objects/{}/{}", self.url, class, id)).send().await.map_err(EpError::request)?;

        parse_optional_json_response(resp, "delete").await
    }

    async fn batch_objects(&self, body: String) -> Result<Value, EpError> {
        let resp = self.client.post(self.url.to_string() + "/v1/batch/objects").body(body).send().await.map_err(EpError::request)?;

        parse_json_response(resp, "batch_objects").await
    }

    async fn get_schema(&self) -> Result<Value, EpError> {
        let resp = self.client.get(self.url.to_string() + "/v1/schema").send().await.map_err(EpError::request)?;

        parse_json_response(resp, "get_schema").await
    }

    async fn health_check(&self) -> Result<Value, EpError> {
        let resp = self.client.get(self.url.to_string() + "/v1/.well-known/ready").send().await.map_err(EpError::request)?;

        parse_optional_json_response(resp, "health check").await
    }
}

#[cfg(test)]
mod tests {
    use super::{WeaviateClient, WeaviateRequests};
    use httpmock::prelude::*;
    use reqwest::Client;

    fn test_client() -> Client {
        Client::builder().danger_accept_invalid_certs(true).build().expect("test client")
    }

    #[tokio::test]
    async fn create_object_rejects_non_2xx_with_error_body() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/objects");
                then.status(422).header("content-type", "application/json").body(r#"{"error":"invalid payload"}"#);
            })
            .await;

        let client = WeaviateClient { client: test_client(), url: server.base_url() };

        let result = client.create_object(r#"{"foo":"bar"}"#.to_string()).await;
        assert!(result.is_err(), "non-2xx should return Err");

        let error = result.expect_err("expected Err").to_string();
        assert!(error.contains("422"), "error should include status code: {error}");
        assert!(error.contains("invalid payload"), "error should include response body: {error}");
    }

    #[tokio::test]
    async fn create_object_parses_success_json() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/objects");
                then.status(200).header("content-type", "application/json").body(r#"{"id":"abc123","class":"Test"}"#);
            })
            .await;

        let client = WeaviateClient { client: test_client(), url: server.base_url() };

        let result = client
            .create_object(r#"{"class":"Test","properties":{"name":"v"}}"#.to_string())
            .await
            .expect("successful response should parse");

        assert_eq!(result["id"], "abc123");
        assert_eq!(result["class"], "Test");
    }

    #[tokio::test]
    async fn create_object_rejects_malformed_utf8_even_on_2xx() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/objects");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(vec![b'{', b'"', b'v', b'a', b'l', b'u', b'e', b'"', b':', b'"', 0xff, b'"', b'}']);
            })
            .await;

        let client = WeaviateClient { client: test_client(), url: server.base_url() };
        let result = client.create_object(r#"{"class":"Test"}"#.to_string()).await;
        assert!(result.is_err(), "malformed utf-8 in 2xx JSON should return Err");
        let error = result.expect_err("expected Err").to_string();
        assert!(error.contains("invalid JSON"), "error should indicate parse failure: {error}");
    }
}
