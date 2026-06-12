use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct WeaviateConnection {
    pub url: String,   // base URL e.g. "http://localhost:8080"
    pub token: String, // API key / auth token
}

impl_connection!(WeaviateConnection, EpKind::Weaviate);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct WeaviateTarget {
    pub url: String,
}

/// Connection credentials — auth token.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct WeaviateCredentials {
    pub token: String,
}

impl WeaviateConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &WeaviateTarget, creds: &WeaviateCredentials) -> Self {
        Self { url: target.url.clone(), token: creds.token.clone() }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(WeaviateTarget, WeaviateCredentials)> {
        Ok((WeaviateTarget { url: self.url.clone() }, WeaviateCredentials { token: self.token.clone() }))
    }
}
