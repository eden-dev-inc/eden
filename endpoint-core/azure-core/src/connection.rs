use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct AzureConnection {
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
}

impl_connection!(AzureConnection, EpKind::Azure);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct AzureTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct AzureCredentials {
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
}

impl AzureConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &AzureTarget, creds: &AzureCredentials) -> Self {
        Self {
            tenant_id: creds.tenant_id.clone(),
            client_id: creds.client_id.clone(),
            client_secret: creds.client_secret.clone(),
            subscription_id: target.subscription_id.clone(),
            access_token: creds.access_token.clone(),
            endpoint_url: target.endpoint_url.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(AzureTarget, AzureCredentials)> {
        Ok((
            AzureTarget {
                subscription_id: self.subscription_id.clone(),
                endpoint_url: self.endpoint_url.clone(),
            },
            AzureCredentials {
                tenant_id: self.tenant_id.clone(),
                client_id: self.client_id.clone(),
                client_secret: self.client_secret.clone(),
                access_token: self.access_token.clone(),
            },
        ))
    }
}
