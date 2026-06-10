use crate::auth::MongoAuth;
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MongoConnection {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<MongoAuth>,
}

impl_connection!(MongoConnection, EpKind::Mongo);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MongoTarget {
    pub url: String,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MongoCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<MongoAuth>,
}

impl MongoConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &MongoTarget, creds: &MongoCredentials) -> Self {
        Self { url: target.url.clone(), auth: creds.auth.clone() }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(MongoTarget, MongoCredentials)> {
        Ok((MongoTarget { url: self.url.clone() }, MongoCredentials { auth: self.auth.clone() }))
    }
}
