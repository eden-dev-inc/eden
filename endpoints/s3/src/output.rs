use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use s3_core::{
    S3GetObjectResponse, S3HeadObjectResponse, S3ListBucketsResponse, S3ListObjectsResponse, S3ObjectBody, S3Provider, S3PutObjectResponse,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, utoipa::ToSchema, schemars::JsonSchema)]
#[serde(tag = "format", rename_all = "snake_case", content = "value")]
pub enum S3PayloadSchema {
    #[default]
    Empty,
    Json(Value),
    Text(String),
    Base64(String),
}

impl From<S3ObjectBody> for S3PayloadSchema {
    fn from(value: S3ObjectBody) -> Self {
        match value {
            S3ObjectBody::Empty => Self::Empty,
            S3ObjectBody::Json(value) => Self::Json(value),
            S3ObjectBody::Text(value) => Self::Text(value),
            S3ObjectBody::Base64(value) => Self::Base64(value),
        }
    }
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3PutObjectOutput {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3GetObjectOutput {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i64>,
    pub last_modified: Option<String>,
    pub metadata: HashMap<String, String>,
    pub payload: S3PayloadSchema,
    #[serde(skip)]
    pub raw_body: bytes::Bytes,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3HeadObjectOutput {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i64>,
    pub last_modified: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3ListObjectOutput {
    pub key: String,
    pub etag: Option<String>,
    pub size: Option<i64>,
    pub last_modified: Option<String>,
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3ListObjectsOutput {
    pub provider: S3Provider,
    pub bucket: String,
    pub prefix: Option<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub objects: Vec<S3ListObjectOutput>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3BucketOutput {
    pub name: String,
    pub creation_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3ListBucketsOutput {
    pub provider: S3Provider,
    pub buckets: Vec<S3BucketOutput>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3DeleteObjectOutput {
    pub provider: S3Provider,
    pub bucket: String,
    pub key: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct S3BucketMutationOutput {
    pub provider: S3Provider,
    pub bucket: String,
    pub success: bool,
}

macro_rules! impl_json_output {
    ($ty:ty) => {
        impl ToOutput for $ty {
            fn to_output(self) -> EndpointOutput<Self> {
                EndpointOutput::new(EpKind::S3, EndpointResponse::Response(self))
            }

            fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
                Err(EpError::Protocol(ProtocolError::NotImplemented))
            }

            fn try_serde_serialize(&self) -> ResultEP<Value> {
                serde_json::to_value(self).map_err(EpError::serde)
            }

            fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
                let json = serde_json::to_string(self).map_err(EpError::serde)?;
                borsh::to_vec(&json).map_err(EpError::serde)
            }
        }
    };
}

impl_json_output!(S3PutObjectOutput);
impl_json_output!(S3HeadObjectOutput);
impl_json_output!(S3ListObjectsOutput);
impl_json_output!(S3ListBucketsOutput);
impl_json_output!(S3DeleteObjectOutput);
impl_json_output!(S3BucketMutationOutput);

impl ToOutput for S3GetObjectOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::S3, EndpointResponse::Response(self))
    }

    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Ok(self.raw_body)
    }

    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self).map_err(EpError::serde)
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        let json = serde_json::to_string(self).map_err(EpError::serde)?;
        borsh::to_vec(&json).map_err(EpError::serde)
    }
}

impl From<S3PutObjectResponse> for S3PutObjectOutput {
    fn from(value: S3PutObjectResponse) -> Self {
        Self {
            provider: value.provider,
            bucket: value.bucket,
            key: value.key,
            etag: value.etag,
            version_id: value.version_id,
        }
    }
}

impl From<S3GetObjectResponse> for S3GetObjectOutput {
    fn from(value: S3GetObjectResponse) -> Self {
        Self {
            provider: value.provider,
            bucket: value.bucket,
            key: value.key,
            etag: value.etag,
            content_type: value.content_type,
            content_length: value.content_length,
            last_modified: value.last_modified,
            metadata: value.metadata,
            payload: value.payload.into(),
            raw_body: value.raw_body,
        }
    }
}

impl From<S3HeadObjectResponse> for S3HeadObjectOutput {
    fn from(value: S3HeadObjectResponse) -> Self {
        Self {
            provider: value.provider,
            bucket: value.bucket,
            key: value.key,
            etag: value.etag,
            content_type: value.content_type,
            content_length: value.content_length,
            last_modified: value.last_modified,
            metadata: value.metadata,
        }
    }
}

impl From<S3ListObjectsResponse> for S3ListObjectsOutput {
    fn from(value: S3ListObjectsResponse) -> Self {
        Self {
            provider: value.provider,
            bucket: value.bucket,
            prefix: value.prefix,
            is_truncated: value.is_truncated,
            next_continuation_token: value.next_continuation_token,
            objects: value
                .objects
                .into_iter()
                .map(|object| S3ListObjectOutput {
                    key: object.key,
                    etag: object.etag,
                    size: object.size,
                    last_modified: object.last_modified,
                    storage_class: object.storage_class,
                })
                .collect(),
        }
    }
}

impl From<S3ListBucketsResponse> for S3ListBucketsOutput {
    fn from(value: S3ListBucketsResponse) -> Self {
        Self {
            provider: value.provider,
            buckets: value
                .buckets
                .into_iter()
                .map(|bucket| S3BucketOutput { name: bucket.name, creation_date: bucket.creation_date })
                .collect(),
        }
    }
}
