use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Function provider type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum FunctionProvider {
    #[default]
    AwsLambda,
}

/// Connection settings for function providers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct FunctionConnection {
    #[serde(default)]
    pub provider: FunctionProvider,
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
    pub default_function_name: Option<String>,
}

impl Default for FunctionConnection {
    fn default() -> Self {
        Self {
            provider: FunctionProvider::AwsLambda,
            region: "us-east-1".to_string(),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            default_function_name: None,
        }
    }
}

impl_connection!(FunctionConnection, EpKind::Function);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect and function settings.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct FunctionTarget {
    #[serde(default)]
    pub provider: FunctionProvider,
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_function_name: Option<String>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct FunctionCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}

impl FunctionConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &FunctionTarget, creds: &FunctionCredentials) -> Self {
        Self {
            provider: target.provider,
            region: target.region.clone(),
            endpoint_url: target.endpoint_url.clone(),
            access_key_id: creds.access_key_id.clone(),
            secret_access_key: creds.secret_access_key.clone(),
            session_token: creds.session_token.clone(),
            default_function_name: target.default_function_name.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(FunctionTarget, FunctionCredentials)> {
        Ok((
            FunctionTarget {
                provider: self.provider,
                region: self.region.clone(),
                endpoint_url: self.endpoint_url.clone(),
                default_function_name: self.default_function_name.clone(),
            },
            FunctionCredentials {
                access_key_id: self.access_key_id.clone(),
                secret_access_key: self.secret_access_key.clone(),
                session_token: self.session_token.clone(),
            },
        ))
    }
}
