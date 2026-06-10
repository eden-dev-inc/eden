use crate::connection::AwsConnection;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SignatureLocation, SigningSettings, sign};
use aws_sigv4::sign::v4;
use error::EpError;
use reqwest::Client;
use serde_json::Value;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct AwsClient {
    client: Client,
    region: String,
    credentials: Credentials,
    endpoint_url: Option<String>,
}

fn service_endpoint(service: &str, region: &str, endpoint_url: Option<&str>) -> Result<String, EpError> {
    if let Some(url) = endpoint_url {
        if !url.starts_with("https://") && !url.starts_with("http://localhost") && !url.starts_with("http://127.0.0.1") {
            return Err(EpError::connect(format!("custom AWS endpoint URL must use HTTPS (got: {url})")));
        }
        Ok(url.trim_end_matches('/').to_string())
    } else {
        Ok(format!("https://{service}.{region}.amazonaws.com"))
    }
}

impl AwsClient {
    pub async fn new(conn: &AwsConnection) -> Result<Self, EpError> {
        let region = conn.region.trim();
        if region.is_empty() {
            return Err(EpError::connect("AWS connection region cannot be empty"));
        }

        let credentials = resolve_credentials(conn).await?;

        let client = Client::builder().build().map_err(EpError::connect)?;

        Ok(Self {
            client,
            region: region.to_string(),
            credentials,
            endpoint_url: conn.endpoint_url.clone(),
        })
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        // Use STS GetCallerIdentity as a lightweight health check
        let url = service_endpoint("sts", &self.region, self.endpoint_url.as_deref())?;
        let body = "Action=GetCallerIdentity&Version=2011-06-15";

        let signed_request =
            self.sign_request("POST", &url, "sts", Some(body.as_bytes()), Some("application/x-www-form-urlencoded")).await?;

        let resp = signed_request.send(&self.client).await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(EpError::request(format!("AWS health check failed with status: {}", resp.status())))
        }
    }

    /// Execute a form-encoded AWS API request (used by STS, IAM, and other query-based services).
    /// Returns the raw response text (typically XML).
    pub async fn execute_form(&self, service: &str, form_body: &str) -> Result<String, EpError> {
        let url = service_endpoint(service, &self.region, self.endpoint_url.as_deref())?;

        let signed =
            self.sign_request("POST", &url, service, Some(form_body.as_bytes()), Some("application/x-www-form-urlencoded")).await?;

        let resp = signed.send(&self.client).await?;

        let status = resp.status();
        let resp_text = resp.text().await.map_err(EpError::request)?;

        if !status.is_success() {
            return Err(EpError::request(format!("AWS {service} form request failed with status {status}: {resp_text}")));
        }

        Ok(resp_text)
    }

    pub async fn execute(
        &self,
        service: &str,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<&Value>,
        content_type: Option<&str>,
    ) -> Result<Value, EpError> {
        let base = service_endpoint(service, &self.region, self.endpoint_url.as_deref())?;
        let url = if let Some(q) = query {
            format!("{base}{path}?{q}")
        } else {
            format!("{base}{path}")
        };

        let body_bytes = body.map(|b| serde_json::to_vec(b).map_err(EpError::serde)).transpose()?;

        let ct = content_type.unwrap_or("application/json");

        let signed = self.sign_request(method, &url, service, body_bytes.as_deref(), Some(ct)).await?;

        let resp = signed.send(&self.client).await?;

        let status = resp.status();
        let resp_bytes = resp.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&resp_bytes);
            return Err(EpError::request(format!("AWS {service} request to {url} failed with status {status}: {body_text}")));
        }

        if resp_bytes.is_empty() {
            return Ok(Value::Null);
        }

        // Return raw text as a JSON string for non-JSON responses (e.g. S3/REST-XML)
        match serde_json::from_slice::<Value>(&resp_bytes) {
            Ok(v) => Ok(v),
            Err(_) => Ok(Value::String(String::from_utf8_lossy(&resp_bytes).into_owned())),
        }
    }

    /// Execute a JSON Target AWS API request (DynamoDB, Lambda, Kinesis, Step Functions, etc.).
    /// Sets the `X-Amz-Target` header and `application/x-amz-json-{ct_version}` content type.
    pub async fn execute_json_target(&self, service: &str, target: &str, body: Option<&Value>, ct_version: &str) -> Result<Value, EpError> {
        let url = service_endpoint(service, &self.region, self.endpoint_url.as_deref())?;
        let body_bytes = body.map(|b| serde_json::to_vec(b).map_err(EpError::serde)).transpose()?;
        let content_type = format!("application/x-amz-json-{ct_version}");
        let extra_headers = [("x-amz-target", target)];

        let signed = self.sign_request_inner("POST", &url, service, body_bytes.as_deref(), Some(&content_type), &extra_headers).await?;

        let resp = signed.send(&self.client).await?;

        let status = resp.status();
        let resp_bytes = resp.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&resp_bytes);
            return Err(EpError::request(format!(
                "AWS {service} JSON target request failed with status {status}: {body_text}"
            )));
        }

        if resp_bytes.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_slice(&resp_bytes)
            .map_err(|e| EpError::request(format!("invalid JSON in AWS {service} JSON target response: {e}")))
    }

    async fn sign_request(
        &self,
        method: &str,
        url: &str,
        service: &str,
        body: Option<&[u8]>,
        content_type: Option<&str>,
    ) -> Result<SignedRequest, EpError> {
        self.sign_request_inner(method, url, service, body, content_type, &[]).await
    }

    async fn sign_request_inner<'a>(
        &'a self,
        method: &'a str,
        url: &'a str,
        service: &'a str,
        body: Option<&'a [u8]>,
        content_type: Option<&'a str>,
        extra_headers: &[(&'a str, &'a str)],
    ) -> Result<SignedRequest, EpError> {
        let mut signing_settings = SigningSettings::default();
        signing_settings.signature_location = SignatureLocation::Headers;

        let identity = self.credentials.clone().into();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(service)
            .time(SystemTime::now())
            .settings(signing_settings)
            .build()
            .map_err(|e| EpError::auth(format!("failed to build AWS signing params: {e}")))?;

        let signable_body = match body {
            Some(b) => SignableBody::Bytes(b),
            None => SignableBody::empty(),
        };

        let signable_request = SignableRequest::new(method, url, extra_headers.iter().copied(), signable_body)
            .map_err(|e| EpError::auth(format!("failed to create signable request: {e}")))?;

        let (signing_instructions, _signature) = sign(signable_request, &signing_params.into())
            .map_err(|e| EpError::auth(format!("failed to sign AWS request: {e}")))?
            .into_parts();

        let mut headers: Vec<(String, String)> =
            signing_instructions.headers().map(|(name, value)| (name.to_string(), value.to_string())).collect();

        // Extra headers are included in the canonical request (signed) but not emitted by
        // signing_instructions since they are "pre-existing" headers.  Add them explicitly so
        // they are sent on the wire.
        for (name, value) in extra_headers {
            headers.push((name.to_string(), value.to_string()));
        }

        Ok(SignedRequest {
            method: method.to_string(),
            url: url.to_string(),
            headers,
            body: body.map(|b| b.to_vec()),
            content_type: content_type.map(|s| s.to_string()),
        })
    }
}

async fn resolve_credentials(conn: &AwsConnection) -> Result<Credentials, EpError> {
    let access_key = conn.access_key_id.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let secret_key = conn.secret_access_key.as_deref().map(str::trim).filter(|s| !s.is_empty());

    if access_key.is_some() ^ secret_key.is_some() {
        return Err(EpError::connect(
            "both `access_key_id` and `secret_access_key` must be provided together for AWS connections",
        ));
    }

    if let (Some(ak), Some(sk)) = (access_key, secret_key) {
        let session = conn.session_token.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(|s| s.to_string());
        return Ok(Credentials::new(ak, sk, session, None, "eden-aws"));
    }

    // Fall back to default credential chain (env vars, instance profile, etc.)
    let config = aws_config::defaults(BehaviorVersion::latest()).region(aws_config::Region::new(conn.region.clone())).load().await;

    config
        .credentials_provider()
        .ok_or_else(|| EpError::connect("no AWS credentials found in default chain"))?
        .provide_credentials()
        .await
        .map(|c| Credentials::new(c.access_key_id(), c.secret_access_key(), c.session_token().map(|s| s.to_string()), None, "eden-aws"))
        .map_err(|e| EpError::connect(format!("failed to resolve AWS credentials: {e}")))
}

#[derive(Debug)]
struct SignedRequest {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
    content_type: Option<String>,
}

impl SignedRequest {
    async fn send(self, client: &Client) -> Result<reqwest::Response, EpError> {
        let method: reqwest::Method =
            self.method.parse().map_err(|_| EpError::request(format!("invalid HTTP method '{}'", self.method)))?;
        let mut builder = client.request(method, &self.url);

        for (name, value) in &self.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        if let Some(ct) = &self.content_type {
            builder = builder.header("content-type", ct.as_str());
        }

        if let Some(body) = self.body {
            builder = builder.body(body);
        }

        builder.send().await.map_err(EpError::request)
    }
}
