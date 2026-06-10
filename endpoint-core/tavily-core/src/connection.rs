use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct TavilyConnection {
    /// The Tavily API key (prefixed with `tvly-`).
    pub api_key: String,
    /// Optional base URL override. Defaults to `https://api.tavily.com`.
    #[serde(default)]
    pub base_url: Option<String>,
}

impl_connection!(TavilyConnection, EpKind::Tavily);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct TavilyTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

/// Connection credentials — API key.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct TavilyCredentials {
    pub api_key: String,
}

impl TavilyConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &TavilyTarget, creds: &TavilyCredentials) -> Self {
        Self {
            api_key: creds.api_key.clone(),
            base_url: target.base_url.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(TavilyTarget, TavilyCredentials)> {
        Ok((
            TavilyTarget { base_url: self.base_url.clone() },
            TavilyCredentials { api_key: self.api_key.clone() },
        ))
    }
}
