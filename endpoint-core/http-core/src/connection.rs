use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct HttpConnection {
    pub url: String, // http url
    pub headers: Option<std::collections::HashMap<String, String>>,
    // pub body: Option<String>,
    // pub tls: TlsConfig, // tls certificate
}

#[allow(dead_code)] // Placeholder for future TLS support
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
struct TlsConfig {
    pub cert_pem: String,
    pub key_pem: String,
}

impl_connection!(HttpConnection, EpKind::Http);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct HttpTarget {
    pub url: String,
}

/// Connection credentials — auth headers.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct HttpCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<std::collections::HashMap<String, String>>,
}

impl HttpConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &HttpTarget, creds: &HttpCredentials) -> Self {
        Self { url: target.url.clone(), headers: creds.headers.clone() }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(HttpTarget, HttpCredentials)> {
        Ok((HttpTarget { url: self.url.clone() }, HttpCredentials { headers: self.headers.clone() }))
    }
}
