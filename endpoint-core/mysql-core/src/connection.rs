use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MysqlConnection {
    pub url: String,
}

impl_connection!(MysqlConnection, EpKind::Mysql);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect (host, port, database).
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MysqlTarget {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MysqlCredentials {
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl MysqlConnection {
    /// Compose a connection URL from a target and credentials.
    pub fn from_target_and_credentials(target: &MysqlTarget, creds: &MysqlCredentials) -> Self {
        let password_part = creds.password.as_ref().map(|p| format!(":{p}")).unwrap_or_default();

        let port_part = target.port.unwrap_or(3306);

        let db_part = target.database.as_deref().unwrap_or("");

        let url = format!("mysql://{}{}@{}:{}/{}", creds.username, password_part, target.host, port_part, db_part,);

        Self { url }
    }

    /// Split a connection URL into target and credentials.
    pub fn split(&self) -> ResultEP<(MysqlTarget, MysqlCredentials)> {
        // Parse: mysql://user:pass@host:port/db
        let without_scheme = self.url.strip_prefix("mysql://").unwrap_or(&self.url);

        let (userinfo, hostinfo) = match without_scheme.split_once('@') {
            Some((u, h)) => (u, h),
            None => ("", without_scheme),
        };

        let (username, password) = match userinfo.split_once(':') {
            Some((u, p)) => (u.to_string(), Some(p.to_string())),
            None => (userinfo.to_string(), None),
        };

        let (host_port, database) = match hostinfo.split_once('/') {
            Some((hp, db)) => {
                let db = if db.is_empty() { None } else { Some(db.to_string()) };
                (hp, db)
            }
            None => (hostinfo, None),
        };

        let (host, port) = match host_port.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u16>().ok()),
            None => (host_port.to_string(), None),
        };

        Ok((MysqlTarget { host, port, database }, MysqlCredentials { username, password }))
    }
}
