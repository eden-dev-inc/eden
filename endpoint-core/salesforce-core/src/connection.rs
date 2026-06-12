use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SalesforceConnection {
    /// Salesforce instance URL (e.g., `https://yourorg.salesforce.com`).
    pub instance_url: String,
    /// OAuth2 Connected App consumer key.
    pub client_id: String,
    /// OAuth2 Connected App consumer secret.
    pub client_secret: String,
    /// Salesforce username for the OAuth2 password flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Salesforce password concatenated with the security token for the OAuth2 password flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Pre-obtained OAuth2 access token (skips the token request when provided).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    /// Salesforce REST API version (e.g., `v60.0`). Defaults to `v60.0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

impl_connection!(SalesforceConnection, EpKind::Salesforce);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SalesforceTarget {
    /// Salesforce instance URL (e.g., `https://yourorg.salesforce.com`).
    pub instance_url: String,
    /// Salesforce REST API version (e.g., `v60.0`). Defaults to `v60.0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SalesforceCredentials {
    /// OAuth2 Connected App consumer key.
    pub client_id: String,
    /// OAuth2 Connected App consumer secret.
    pub client_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
}

impl SalesforceConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &SalesforceTarget, creds: &SalesforceCredentials) -> Self {
        Self {
            instance_url: target.instance_url.clone(),
            client_id: creds.client_id.clone(),
            client_secret: creds.client_secret.clone(),
            username: creds.username.clone(),
            password: creds.password.clone(),
            access_token: creds.access_token.clone(),
            api_version: target.api_version.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(SalesforceTarget, SalesforceCredentials)> {
        Ok((
            SalesforceTarget {
                instance_url: self.instance_url.clone(),
                api_version: self.api_version.clone(),
            },
            SalesforceCredentials {
                client_id: self.client_id.clone(),
                client_secret: self.client_secret.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                access_token: self.access_token.clone(),
            },
        ))
    }
}
