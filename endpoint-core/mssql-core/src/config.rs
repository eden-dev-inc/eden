use crate::MssqlAsync;
use crate::comm::MssqlClient;
use crate::connection::{MssqlConnection, MssqlCredentials, MssqlTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::ResultEP;
use error::{ConnectError, EpError};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Clone, Default, BorshDeserialize, BorshSerialize, Serialize, ToSchema)]
#[schema(title = "MssqlConfig")]
pub struct MssqlConfig {
    pub database: String,
    pub target: MssqlTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<MssqlCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<MssqlCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<MssqlCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<MssqlCredentials>,
}

impl MssqlConfig {
    #[allow(dead_code)]
    fn new() -> MssqlConfig {
        MssqlConfig::default()
    }
}

impl_ep_config_target_auth!(MssqlConfig, MssqlConnection, MssqlTarget, MssqlCredentials, EpKind::Mssql);

impl fmt::Display for MssqlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "database: {:?}, target: {:?}, read: {:?}, write: {:?}, admin: {:?}, system: {:?}",
            self.database, self.target, self.read_credentials, self.write_credentials, self.admin_credentials, self.system_credentials
        )
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible deserialization
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct MssqlConfigRaw {
    #[serde(default)]
    target: Option<MssqlTarget>,
    #[serde(default)]
    read_credentials: Option<MssqlCredentials>,
    #[serde(default)]
    write_credentials: Option<MssqlCredentials>,
    #[serde(default)]
    admin_credentials: Option<MssqlCredentials>,
    #[serde(default)]
    system_credentials: Option<MssqlCredentials>,

    #[serde(default)]
    read_conn: Option<MssqlConnection>,
    #[serde(default)]
    write_conn: Option<MssqlConnection>,
    #[serde(default)]
    admin_conn: Option<MssqlConnection>,
    #[serde(default)]
    system_conn: Option<MssqlConnection>,

    // Extra fields
    #[serde(default)]
    database: String,
}

impl<'de> Deserialize<'de> for MssqlConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = MssqlConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(MssqlConfig {
                database: raw.database,
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<MssqlConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(MssqlConfig {
                database: raw.database,
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(MssqlConfig { database: raw.database, ..Default::default() })
        }
    }
}

impl RWPool<MssqlAsync> for MssqlConfig {
    #[named]
    async fn conn_async(&self, connection: Box<dyn EpConnection>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<MssqlAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<MssqlConnection>() {
            Some(config) => config,
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(MssqlClient::new(connection).await?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}
