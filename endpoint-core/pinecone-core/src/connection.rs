use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PineconeConnection {
    pub url: String,   // db url
    pub token: String, //auth api-key
}

impl_connection!(PineconeConnection, EpKind::Pinecone);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PineconeTarget {
    pub url: String,
}

/// Connection credentials — auth token.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PineconeCredentials {
    pub token: String,
}

impl PineconeConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &PineconeTarget, creds: &PineconeCredentials) -> Self {
        Self { url: target.url.clone(), token: creds.token.clone() }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(PineconeTarget, PineconeCredentials)> {
        Ok((PineconeTarget { url: self.url.clone() }, PineconeCredentials { token: self.token.clone() }))
    }
}
