use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PostgresConnection {
    pub url: String,
    pub sslmode: Option<SslMode>,
}

impl_connection!(PostgresConnection, EpKind::Postgres);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect (host, port, database, TLS).
/// Shared across all privilege tiers for a single endpoint.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PostgresTarget {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub sslmode: Option<SslMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_name: Option<String>,
}

fn default_pg_port() -> u16 {
    5432
}

/// Connection credentials — WHO to authenticate as.
/// One set per privilege tier (read, write, admin, system).
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PostgresCredentials {
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl PostgresConnection {
    /// Compose a connection URL from a target and credentials.
    pub fn from_target_and_credentials(target: &PostgresTarget, creds: &PostgresCredentials) -> Self {
        let password_part = creds.password.as_ref().map(|p| format!(":{}", percent_encode(p))).unwrap_or_default();

        let db_part = target.database.as_deref().unwrap_or(&creds.username);

        let mut url = format!(
            "postgresql://{}{}@{}:{}/{}",
            percent_encode(&creds.username),
            password_part,
            &target.host,
            target.port,
            percent_encode(db_part),
        );

        // Append query params
        let mut params = Vec::new();
        if let Some(ref app_name) = target.application_name {
            params.push(format!("application_name={}", percent_encode(app_name)));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        Self { url, sslmode: target.sslmode.clone() }
    }

    /// Split a connection URL into target and credentials.
    pub fn split(&self) -> ResultEP<(PostgresTarget, PostgresCredentials)> {
        let parsed = crate::url::PostgresConnectionParsed::from_connection(self)?;
        Ok((
            PostgresTarget {
                host: parsed.host,
                port: parsed.port,
                database: Some(parsed.database),
                sslmode: Some(parsed.sslmode),
                application_name: parsed.application_name,
            },
            PostgresCredentials { username: parsed.user, password: parsed.password },
        ))
    }
}

/// Minimal percent-encoding for URL components (username, password, database).
fn percent_encode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            _ => {
                result.push_str(&format!("%{b:02X}"));
            }
        }
    }
    result
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    Disable,
    #[default]
    Prefer,
    Require,
}

impl From<SslMode> for tokio_postgres::config::SslMode {
    fn from(ssl_mode: SslMode) -> Self {
        match ssl_mode {
            SslMode::Disable => Self::Disable,
            SslMode::Prefer => Self::Prefer,
            SslMode::Require => Self::Require,
        }
    }
}
