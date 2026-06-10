use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct DatadogConnection {
    pub site: String,
    pub api_key: String,
    pub application_key: Option<String>,
}

impl_connection!(DatadogConnection, EpKind::Datadog);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct DatadogTarget {
    pub site: String,
}

/// Connection credentials — API keys.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct DatadogCredentials {
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_key: Option<String>,
}

impl DatadogConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &DatadogTarget, creds: &DatadogCredentials) -> Self {
        Self {
            site: target.site.clone(),
            api_key: creds.api_key.clone(),
            application_key: creds.application_key.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(DatadogTarget, DatadogCredentials)> {
        Ok((
            DatadogTarget { site: self.site.clone() },
            DatadogCredentials {
                api_key: self.api_key.clone(),
                application_key: self.application_key.clone(),
            },
        ))
    }
}
