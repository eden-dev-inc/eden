use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Connection configuration for Snowflake.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SnowflakeConnection {
    /// Account identifier (e.g., "xy12345.us-east-1")
    pub account: String,

    /// Username for authentication
    pub user: String,

    /// RSA private key (PEM format) for JWT authentication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,

    /// OAuth token (alternative to key-pair authentication)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_token: Option<String>,

    /// Default warehouse to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,

    /// Default database to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// Default schema to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Default role to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Query timeout in seconds (default: 0 = no timeout)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

impl_connection!(SnowflakeConnection, EpKind::Snowflake);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SnowflakeTarget {
    /// Account identifier (e.g., "xy12345.us-east-1")
    pub account: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SnowflakeCredentials {
    pub user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_token: Option<String>,
}

impl SnowflakeConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &SnowflakeTarget, creds: &SnowflakeCredentials) -> Self {
        Self {
            account: target.account.clone(),
            user: creds.user.clone(),
            private_key: creds.private_key.clone(),
            oauth_token: creds.oauth_token.clone(),
            warehouse: target.warehouse.clone(),
            database: target.database.clone(),
            schema: target.schema.clone(),
            role: target.role.clone(),
            timeout: target.timeout,
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(SnowflakeTarget, SnowflakeCredentials)> {
        Ok((
            SnowflakeTarget {
                account: self.account.clone(),
                warehouse: self.warehouse.clone(),
                database: self.database.clone(),
                schema: self.schema.clone(),
                role: self.role.clone(),
                timeout: self.timeout,
            },
            SnowflakeCredentials {
                user: self.user.clone(),
                private_key: self.private_key.clone(),
                oauth_token: self.oauth_token.clone(),
            },
        ))
    }
}
