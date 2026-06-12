use crate::auth::MssqlAuth;
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MssqlConnection {
    pub url: String,
    pub auth: MssqlAuth,
}

impl_connection!(MssqlConnection, EpKind::Mssql);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MssqlTarget {
    pub url: String,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MssqlCredentials {
    pub auth: MssqlAuth,
}

impl MssqlConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &MssqlTarget, creds: &MssqlCredentials) -> Self {
        Self { url: target.url.clone(), auth: creds.auth.clone() }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(MssqlTarget, MssqlCredentials)> {
        Ok((MssqlTarget { url: self.url.clone() }, MssqlCredentials { auth: self.auth.clone() }))
    }
}
