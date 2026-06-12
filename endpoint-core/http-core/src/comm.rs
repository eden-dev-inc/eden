use crate::connection::HttpConnection;
use error::{ConnectError, EpError};
use futures::Future;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
//
// #[derive(Debug, Clone, BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
// pub struct HttpRequest {
//     pub method: String,
//     pub body: Option<String>,
//     pub headers: Option<HashMap<String, String>>,
// }
//
// #[cfg(test)]
// mod tests {
//     use serde_json::json;
//
//     use crate::http::comm::HttpRequest;
//
//     #[test]
//     fn json_output() {
//         let req = HttpRequest {
//         method: "post".to_string(),
//         body: Some(serde_json::to_string(&json!({
//           "q": "The Great Pyramid of Giza (also known as the Pyramid of Khufu or the Pyramid of Cheops) is the oldest and largest of the three pyramids in the Giza pyramid complex.",
//           "source": "en",
//           "target": "es",
//           "format": "text"
//         })).unwrap_or_default()),
//         headers: None,
//     };
//
//         print!("{}", serde_json::to_string(&req).unwrap_or_default())
//     }
// }
//
// impl HttpRequest {
//     pub async fn read(self, client: &HttpClient) -> Result<Value, EpError> {
//         match self.method.to_uppercase().as_str() {
//             "GET" => client.get(self.body, self.headers).await,
//             _ => Err(EpError::request(
//                 "request does not have propper permissions",
//             )),
//         }
//     }
//     pub async fn write(self, client: &HttpClient) -> Result<Value, EpError> {
//         match self.method.to_uppercase().as_str() {
//             "DELETE" => client.delete(self.body, self.headers).await,
//             "GET" => client.get(self.body, self.headers).await,
//             "POST" => client.post(self.body, self.headers).await,
//             "PUT" => client.put(self.body, self.headers).await,
//             _ => Err(EpError::request(&format!(
//                 "Unsupported HTTP method {}",
//                 self.method
//             ))),
//         }
//     }
// }

pub trait HttpRequests {
    fn delete(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
    fn get(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
    fn post(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
    fn put(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
}

#[derive(Debug, Clone, Default)]
pub struct HttpClient {
    client: Client, // reqwest client
    url: String,    // base url
}

async fn parse_json_response(resp: reqwest::Response, operation: &str, url: &str) -> Result<Value, EpError> {
    let status = resp.status();
    let body = resp.bytes().await.map_err(EpError::request)?;
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        return Err(EpError::request(format!("{operation} request to {url} failed with status {status}: {body_text}")));
    }

    serde_json::from_slice(&body).map_err(|e| EpError::request(format!("invalid JSON in {operation} response from {url}: {e}")))
}

impl HttpClient {
    pub async fn new(conn: &HttpConnection) -> Result<Self, EpError> {
        let header_map = collect_headers(&conn.headers)?;

        // build the client
        let builder = reqwest::Client::builder().default_headers(header_map);

        // Allow self-signed certificates in test environments
        // This is needed for httpmock-based tests that generate self-signed certificates
        let builder = if cfg!(debug_assertions) {
            builder.danger_accept_invalid_certs(true)
        } else {
            builder
        };

        let client = Self {
            client: builder.build().map_err(EpError::connect)?,
            url: conn.url.trim_end_matches('/').to_string(),
        };

        Ok(client)
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let health_url = format!("{}/health", self.url);
        let response = self.client.get(&health_url).send().await.map_err(EpError::request)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("health check failed with status: {}", response.status())))
        }
    }
}

fn collect_headers(headers: &Option<HashMap<String, String>>) -> Result<HeaderMap, EpError> {
    let mut header_map = HeaderMap::new();

    // create the header map for the client
    if let Some(headers) = headers {
        for (key, value) in headers {
            let header_name = HeaderName::from_str(key).map_err(|_| EpError::Connect(ConnectError::InvalidHeaderName))?;
            let header_value = HeaderValue::from_str(value).map_err(|_| EpError::Connect(ConnectError::InvalidHeaderValue))?;
            header_map.insert(header_name, header_value);
        }
    }
    Ok(header_map)
}

// fn load_rustls_config(
//     cert_pem: &str,
//     key_pem: &str,
// ) -> Result<ServerConfig, Box<dyn std::error::Error>> {
//     // Parse the certificate and private key
//     let cert_chain = certs(&mut Cursor::new(cert_pem))?
//         .into_iter()
//         .map(Certificate)
//         .collect();
//     let mut keys: Vec<PrivateKey> = pkcs8_private_keys(&mut Cursor::new(key_pem))?
//         .into_iter()
//         .map(PrivateKey)
//         .collect();

//     // Build the TLS configuration
//     let config = ServerConfig::builder()
//         .with_safe_defaults()
//         .with_no_client_auth()
//         .with_single_cert(cert_chain, keys.remove(0))?;

//     Ok(config)
// }

impl HttpRequests for HttpClient {
    async fn delete(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> Result<Value, EpError> {
        let client = self.client.delete(self.url.to_string());

        let header_map = collect_headers(&headers)?;
        let client = if !header_map.is_empty() {
            client.headers(header_map)
        } else {
            client
        };

        let client = if let Some(body) = body { client.body(body) } else { client };

        let resp = client.send().await.map_err(EpError::request)?;
        parse_json_response(resp, "DELETE", &self.url).await
    }
    async fn get(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> Result<Value, EpError> {
        let client = self.client.get(self.url.to_string());

        let header_map = collect_headers(&headers)?;
        let client = if !header_map.is_empty() {
            client.headers(header_map)
        } else {
            client
        };

        let client = if let Some(body) = body { client.body(body) } else { client };

        let resp = client.send().await.map_err(EpError::request)?;
        parse_json_response(resp, "GET", &self.url).await
    }
    async fn post(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> Result<Value, EpError> {
        let client = self.client.post(self.url.to_string());

        let header_map = collect_headers(&headers)?;
        let client = if !header_map.is_empty() {
            client.headers(header_map)
        } else {
            client
        };

        let client = if let Some(body) = body { client.body(body) } else { client };

        let resp = client.send().await.map_err(EpError::request)?;
        parse_json_response(resp, "POST", &self.url).await
    }
    async fn put(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> Result<Value, EpError> {
        let client = self.client.put(self.url.to_string());

        let header_map = collect_headers(&headers)?;
        let client = if !header_map.is_empty() {
            client.headers(header_map)
        } else {
            client
        };

        let client = if let Some(body) = body { client.body(body) } else { client };

        let resp = client.send().await.map_err(EpError::request)?;
        parse_json_response(resp, "PUT", &self.url).await
    }
}

#[cfg(test)]
mod tests {
    use super::{HttpClient, HttpRequests};
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
    async fn get_rejects_non_2xx_with_error_body() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/target");
                then.status(404).header("content-type", "application/json").body(r#"{"error":"not found"}"#);
            })
            .await;

        let client = HttpClient { client: test_client(), url: server.url("/target") };

        let result = client.get(None, None).await;
        assert!(result.is_err(), "non-2xx should return Err");

        let error = result.expect_err("expected Err").to_string();
        assert!(error.contains("404"), "error should include status code: {error}");
        assert!(error.contains("not found"), "error should include response body: {error}");
    }

    #[tokio::test]
    async fn get_parses_success_json() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/target");
                then.status(200).header("content-type", "application/json").body(r#"{"ok":true}"#);
            })
            .await;

        let client = HttpClient { client: test_client(), url: server.url("/target") };

        let result = client.get(None, None).await.expect("successful response should parse");
        assert_eq!(result["ok"], true);
    }

    #[tokio::test]
    async fn get_rejects_malformed_utf8_even_on_2xx() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/target");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(vec![b'{', b'"', b'v', b'a', b'l', b'u', b'e', b'"', b':', b'"', 0xff, b'"', b'}']);
            })
            .await;

        let client = HttpClient { client: test_client(), url: server.url("/target") };
        let result = client.get(None, None).await;
        assert!(result.is_err(), "malformed utf-8 in 2xx JSON should return Err");
        let error = result.expect_err("expected Err").to_string();
        assert!(error.contains("invalid JSON"), "error should indicate parse failure: {error}");
    }
}
