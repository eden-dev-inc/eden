use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum S3Provider {
    #[default]
    AwsS3,
    Localstack,
    Rustfs,
    GenericS3,
}

impl S3Provider {
    pub const fn default_force_path_style(self) -> bool {
        match self {
            Self::AwsS3 => false,
            Self::Localstack | Self::Rustfs | Self::GenericS3 => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct S3Connection {
    #[serde(default)]
    pub provider: S3Provider,
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_path_style: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_bucket: Option<String>,
}

impl Default for S3Connection {
    fn default() -> Self {
        Self {
            provider: S3Provider::AwsS3,
            region: "us-east-1".to_string(),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            force_path_style: None,
            default_bucket: None,
        }
    }
}

impl_connection!(S3Connection, EpKind::S3);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect and bucket settings.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct S3Target {
    #[serde(default)]
    pub provider: S3Provider,
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_path_style: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_bucket: Option<String>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct S3Credentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}

impl S3Connection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &S3Target, creds: &S3Credentials) -> Self {
        Self {
            provider: target.provider,
            region: target.region.clone(),
            endpoint_url: target.endpoint_url.clone(),
            access_key_id: creds.access_key_id.clone(),
            secret_access_key: creds.secret_access_key.clone(),
            session_token: creds.session_token.clone(),
            force_path_style: target.force_path_style,
            default_bucket: target.default_bucket.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(S3Target, S3Credentials)> {
        Ok((
            S3Target {
                provider: self.provider,
                region: self.region.clone(),
                endpoint_url: self.endpoint_url.clone(),
                force_path_style: self.force_path_style,
                default_bucket: self.default_bucket.clone(),
            },
            S3Credentials {
                access_key_id: self.access_key_id.clone(),
                secret_access_key: self.secret_access_key.clone(),
                session_token: self.session_token.clone(),
            },
        ))
    }
}
