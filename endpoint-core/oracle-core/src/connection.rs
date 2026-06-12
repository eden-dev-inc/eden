use crate::auth::OracleAuth;
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct OracleConnection {
    pub url: String,
    pub auth: OracleAuth,
}

impl_connection!(OracleConnection, EpKind::Oracle);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct OracleTarget {
    pub url: String,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct OracleCredentials {
    pub auth: OracleAuth,
}

impl OracleConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &OracleTarget, creds: &OracleCredentials) -> Self {
        Self { url: target.url.clone(), auth: creds.auth.clone() }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(OracleTarget, OracleCredentials)> {
        Ok((OracleTarget { url: self.url.clone() }, OracleCredentials { auth: self.auth.clone() }))
    }
}
