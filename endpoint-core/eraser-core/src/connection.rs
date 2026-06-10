use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct EraserConnection {
    /// The Eraser API token.
    pub api_key: String,
    /// Optional base URL override. Defaults to `https://app.eraser.io`.
    #[serde(default)]
    pub base_url: Option<String>,
}

impl_connection!(EraserConnection, EpKind::Eraser);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct EraserTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

/// Connection credentials — API key.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct EraserCredentials {
    pub api_key: String,
}

impl EraserConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &EraserTarget, creds: &EraserCredentials) -> Self {
        Self {
            api_key: creds.api_key.clone(),
            base_url: target.base_url.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(EraserTarget, EraserCredentials)> {
        Ok((
            EraserTarget { base_url: self.base_url.clone() },
            EraserCredentials { api_key: self.api_key.clone() },
        ))
    }
}
