use crate::connection::{S3Connection, S3Provider};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use error::EpError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "format", rename_all = "snake_case", content = "value")]
pub enum S3ObjectBody {
    #[default]
    Empty,
    Json(Value),
    Text(String),
    Base64(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct S3PutObjectRequest {
    pub bucket: Option<String>,
    pub key: String,
    #[serde(default)]
    pub body: S3ObjectBody,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3PutObjectResponse {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3GetObjectResponse {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i64>,
    pub last_modified: Option<String>,
    pub metadata: HashMap<String, String>,
    pub payload: S3ObjectBody,
    #[serde(skip)]
    pub raw_body: Bytes,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3HeadObjectResponse {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i64>,
    pub last_modified: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3ListObjectEntry {
    pub key: String,
    pub etag: Option<String>,
    pub size: Option<i64>,
    pub last_modified: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3ListObjectsResponse {
    pub provider: S3Provider,
    pub bucket: String,
    pub prefix: Option<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub objects: Vec<S3ListObjectEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3ListBucketEntry {
    pub name: String,
    pub creation_date: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3ListBucketsResponse {
    pub provider: S3Provider,
    pub buckets: Vec<S3ListBucketEntry>,
}

#[derive(Debug, Clone)]
pub struct S3Client {
    client: Client,
    provider: S3Provider,
    default_bucket: Option<String>,
    region: String,
}

impl S3Client {
    pub async fn new(connection: &S3Connection) -> Result<Self, EpError> {
        let region = normalize_required_string(&connection.region, "s3 connection region cannot be empty")?;
        let mut config_loader = aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region.clone()));

        let access_key_id = normalize_optional_string(connection.access_key_id.as_deref());
        let secret_access_key = normalize_optional_string(connection.secret_access_key.as_deref());

        if access_key_id.is_some() ^ secret_access_key.is_some() {
            return Err(EpError::connect(
                "both `access_key_id` and `secret_access_key` must be provided together for s3 connections",
            ));
        }

        if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
            let credentials = Credentials::new(
                access_key_id,
                secret_access_key,
                normalize_optional_string(connection.session_token.as_deref()),
                None,
                "eden-s3",
            );
            config_loader = config_loader.credentials_provider(credentials);
        }

        let shared_config = config_loader.load().await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config);
        if let Some(endpoint_url) = normalize_optional_string(connection.endpoint_url.as_deref()) {
            builder = builder.endpoint_url(endpoint_url);
        }

        if connection.force_path_style.unwrap_or(connection.provider.default_force_path_style()) {
            builder = builder.force_path_style(true);
        }

        Ok(Self {
            client: Client::from_conf(builder.build()),
            provider: connection.provider,
            default_bucket: normalize_optional_string(connection.default_bucket.as_deref()),
            region,
        })
    }

    pub fn provider(&self) -> S3Provider {
        self.provider
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        self.client.list_buckets().send().await.map_err(|e| EpError::request(format!("s3 health check failed: {e}")))?;
        Ok(())
    }

    pub fn resolve_bucket(&self, bucket: Option<&str>) -> Result<String, EpError> {
        normalize_optional_string(bucket)
            .or_else(|| self.default_bucket.clone())
            .ok_or_else(|| EpError::request("s3 operation requires `bucket` in request or `default_bucket` in connection"))
    }

    pub async fn put_object(&self, request: &S3PutObjectRequest) -> Result<S3PutObjectResponse, EpError> {
        let bucket = self.resolve_bucket(request.bucket.as_deref())?;
        let mut operation = self
            .client
            .put_object()
            .bucket(bucket.clone())
            .key(request.key.clone())
            .body(ByteStream::from(body_to_bytes(&request.body)?));

        if let Some(content_type) = normalize_optional_string(request.content_type.as_deref()) {
            operation = operation.content_type(content_type);
        }

        if let Some(metadata) = request.metadata.as_ref() {
            for (key, value) in metadata {
                operation = operation.metadata(key, value);
            }
        }

        let response = operation
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to put s3 object `{}` in bucket `{bucket}`: {e}", request.key)))?;

        Ok(S3PutObjectResponse {
            provider: self.provider,
            bucket,
            key: request.key.clone(),
            etag: response.e_tag().map(ToOwned::to_owned),
            version_id: response.version_id().map(ToOwned::to_owned),
        })
    }

    pub async fn get_object(&self, bucket: Option<&str>, key: &str) -> Result<S3GetObjectResponse, EpError> {
        let bucket = self.resolve_bucket(bucket)?;
        let response = self
            .client
            .get_object()
            .bucket(bucket.clone())
            .key(key)
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to get s3 object `{key}` from bucket `{bucket}`: {e}")))?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| EpError::request(format!("failed to read s3 object `{key}` body: {e}")))?
            .into_bytes();

        Ok(S3GetObjectResponse {
            provider: self.provider,
            bucket,
            key: key.to_string(),
            etag: response.e_tag.map(|value| value.to_string()),
            content_type: response.content_type.map(|value| value.to_string()),
            content_length: response.content_length,
            last_modified: response.last_modified.map(|value| value.to_string()),
            metadata: response.metadata.unwrap_or_default(),
            payload: normalize_payload(body.as_ref()),
            raw_body: body,
        })
    }

    pub async fn head_object(&self, bucket: Option<&str>, key: &str) -> Result<S3HeadObjectResponse, EpError> {
        let bucket = self.resolve_bucket(bucket)?;
        let response = self
            .client
            .head_object()
            .bucket(bucket.clone())
            .key(key)
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to head s3 object `{key}` from bucket `{bucket}`: {e}")))?;

        Ok(S3HeadObjectResponse {
            provider: self.provider,
            bucket,
            key: key.to_string(),
            etag: response.e_tag.map(|value| value.to_string()),
            content_type: response.content_type.map(|value| value.to_string()),
            content_length: response.content_length,
            last_modified: response.last_modified.map(|value| value.to_string()),
            metadata: response.metadata.unwrap_or_default(),
        })
    }

    pub async fn delete_object(&self, bucket: Option<&str>, key: &str) -> Result<(), EpError> {
        let bucket = self.resolve_bucket(bucket)?;
        self.client
            .delete_object()
            .bucket(bucket.clone())
            .key(key)
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to delete s3 object `{key}` from bucket `{bucket}`: {e}")))?;
        Ok(())
    }

    pub async fn list_objects(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: Option<i32>,
    ) -> Result<S3ListObjectsResponse, EpError> {
        let bucket = self.resolve_bucket(bucket)?;
        let mut operation = self.client.list_objects_v2().bucket(bucket.clone());

        if let Some(prefix) = normalize_optional_string(prefix) {
            operation = operation.prefix(prefix);
        }

        if let Some(token) = normalize_optional_string(continuation_token) {
            operation = operation.continuation_token(token);
        }

        if let Some(max_keys) = max_keys {
            operation = operation.max_keys(max_keys);
        }

        let response =
            operation.send().await.map_err(|e| EpError::request(format!("failed to list s3 objects for bucket `{bucket}`: {e}")))?;

        let objects = response
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|object| S3ListObjectEntry {
                key: object.key.unwrap_or_default(),
                etag: object.e_tag,
                size: object.size,
                last_modified: object.last_modified.map(|value| value.to_string()),
                storage_class: object.storage_class.map(|value| value.as_str().to_string()),
            })
            .collect();

        Ok(S3ListObjectsResponse {
            provider: self.provider,
            bucket,
            prefix: normalize_optional_string(prefix),
            is_truncated: response.is_truncated.unwrap_or(false),
            next_continuation_token: response.next_continuation_token,
            objects,
        })
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<(), EpError> {
        self.client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to create s3 bucket `{bucket}`: {e}")))?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), EpError> {
        self.client
            .delete_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to delete s3 bucket `{bucket}`: {e}")))?;
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<S3ListBucketsResponse, EpError> {
        let response = self.client.list_buckets().send().await.map_err(|e| EpError::request(format!("failed to list s3 buckets: {e}")))?;

        let buckets = response
            .buckets
            .unwrap_or_default()
            .into_iter()
            .map(|bucket| S3ListBucketEntry {
                name: bucket.name.unwrap_or_default(),
                creation_date: bucket.creation_date.map(|value| value.to_string()),
            })
            .collect();

        Ok(S3ListBucketsResponse { provider: self.provider, buckets })
    }
}

pub fn normalize_payload(bytes: &[u8]) -> S3ObjectBody {
    if bytes.is_empty() {
        return S3ObjectBody::Empty;
    }

    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return S3ObjectBody::Json(value);
    }

    if let Ok(value) = String::from_utf8(bytes.to_vec()) {
        return S3ObjectBody::Text(value);
    }

    S3ObjectBody::Base64(BASE64.encode(bytes))
}

fn body_to_bytes(body: &S3ObjectBody) -> Result<Vec<u8>, EpError> {
    match body {
        S3ObjectBody::Empty => Ok(Vec::new()),
        S3ObjectBody::Json(value) => serde_json::to_vec(value).map_err(EpError::serde),
        S3ObjectBody::Text(value) => Ok(value.as_bytes().to_vec()),
        S3ObjectBody::Base64(value) => BASE64.decode(value).map_err(|e| EpError::request(format!("invalid base64 s3 request body: {e}"))),
    }
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|candidate| !candidate.is_empty()).map(ToOwned::to_owned)
}

fn normalize_required_string(value: &str, message: &str) -> Result<String, EpError> {
    normalize_optional_string(Some(value)).ok_or_else(|| EpError::connect(message.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_payload_prefers_json() {
        let payload = normalize_payload(br#"{"ok":true}"#);
        assert!(matches!(payload, S3ObjectBody::Json(_)));
    }

    #[test]
    fn normalize_payload_falls_back_to_text() {
        let payload = normalize_payload(b"hello");
        assert_eq!(serde_json::to_value(payload).expect("serialize payload")["format"], "text");
    }

    #[test]
    fn provider_defaults_path_style_for_localstack() {
        assert!(S3Provider::Localstack.default_force_path_style());
        assert!(!S3Provider::AwsS3.default_force_path_style());
    }
}

#[cfg(all(test, feature = "infra-tests"))]
mod infra_tests {
    use super::*;
    use testcontainers_modules::testcontainers::core::ContainerPort;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{GenericImage, ImageExt};

    #[tokio::test]
    async fn localstack_supports_bucket_and_object_crud() {
        let container = GenericImage::new("localstack/localstack", "4.10.0")
            .with_exposed_port(ContainerPort::Tcp(4566))
            .with_env_var("SERVICES", "s3")
            .with_env_var("AWS_DEFAULT_REGION", "us-east-1")
            .start()
            .await
            .expect("start localstack");

        let port = container.get_host_port_ipv4(ContainerPort::Tcp(4566)).await.expect("resolve localstack port");

        let client = S3Client::new(&S3Connection {
            provider: S3Provider::Localstack,
            region: "us-east-1".to_string(),
            endpoint_url: Some(format!("http://127.0.0.1:{port}")),
            access_key_id: Some("test".to_string()),
            secret_access_key: Some("test".to_string()),
            session_token: None,
            force_path_style: Some(true),
            default_bucket: None,
        })
        .await
        .expect("build s3 client");

        let bucket = format!("eden-s3-core-test-{}", std::process::id());
        wait_for_bucket_create(&client, &bucket).await;

        client
            .put_object(&S3PutObjectRequest {
                bucket: Some(bucket.clone()),
                key: "hello.json".to_string(),
                body: S3ObjectBody::Json(serde_json::json!({"hello":"world"})),
                content_type: Some("application/json".to_string()),
                metadata: Some(HashMap::from([(String::from("env"), String::from("test"))])),
            })
            .await
            .expect("put object");

        let head = client.head_object(Some(bucket.as_str()), "hello.json").await.expect("head object");
        assert_eq!(head.content_type.as_deref(), Some("application/json"));
        assert_eq!(head.metadata.get("env").map(String::as_str), Some("test"));

        let get = client.get_object(Some(bucket.as_str()), "hello.json").await.expect("get object");
        assert!(matches!(get.payload, S3ObjectBody::Json(_)));

        let list = client.list_objects(Some(bucket.as_str()), None, None, None).await.expect("list objects");
        assert_eq!(list.objects.len(), 1);
        assert_eq!(list.objects[0].key, "hello.json");

        let buckets = client.list_buckets().await.expect("list buckets");
        assert!(buckets.buckets.iter().any(|entry| entry.name == bucket));

        client.delete_object(Some(bucket.as_str()), "hello.json").await.expect("delete object");
        client.delete_bucket(&bucket).await.expect("delete bucket");
    }

    async fn wait_for_bucket_create(client: &S3Client, bucket: &str) {
        for _ in 0..20 {
            match client.create_bucket(bucket).await {
                Ok(()) => return,
                Err(_) => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
            }
        }

        client.create_bucket(bucket).await.expect("create bucket");
    }
}
