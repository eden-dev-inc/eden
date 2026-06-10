use crate::MysqlAsync;
use crate::connection::{MysqlConnection, MysqlCredentials, MysqlTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::ResultEP;
use error::{ConnectError, EpError};
use format::endpoint::EpKind;
use function_name::named;
use mysql_async::{Opts, Pool};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "MysqlConfig")]
pub struct MysqlConfig {
    pub target: MysqlTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<MysqlCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<MysqlCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<MysqlCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<MysqlCredentials>,
}

impl_ep_config_target_auth!(MysqlConfig, MysqlConnection, MysqlTarget, MysqlCredentials, EpKind::Mysql);

impl fmt::Display for MysqlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "target: {:?}, read: {:?}, write: {:?}, admin: {:?}, system: {:?}",
            self.target, self.read_credentials, self.write_credentials, self.admin_credentials, self.system_credentials
        )
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible deserialization
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct MysqlConfigRaw {
    // New format
    #[serde(default)]
    target: Option<MysqlTarget>,
    #[serde(default)]
    read_credentials: Option<MysqlCredentials>,
    #[serde(default)]
    write_credentials: Option<MysqlCredentials>,
    #[serde(default)]
    admin_credentials: Option<MysqlCredentials>,
    #[serde(default)]
    system_credentials: Option<MysqlCredentials>,

    // Legacy format
    #[serde(default)]
    read_conn: Option<MysqlConnection>,
    #[serde(default)]
    write_conn: Option<MysqlConnection>,
    #[serde(default)]
    admin_conn: Option<MysqlConnection>,
    #[serde(default)]
    system_conn: Option<MysqlConnection>,
}

impl<'de> Deserialize<'de> for MysqlConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = MysqlConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(MysqlConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<MysqlConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(MysqlConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(MysqlConfig::default())
        }
    }
}

/// r2d2 M7Sql returning errors
impl RWPool<MysqlAsync> for MysqlConfig {
    #[named]
    async fn conn_async(&self, connection: Box<dyn EpConnection>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<MysqlAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<MysqlConnection>() {
            Some(mongo_config) => mongo_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let url = Opts::from_url(&connection.url).map_err(EpError::connect)?;

        Ok(Pool::new(url))
    }
}
