use crate::SnowflakeAsync;
use crate::client::{SnowflakeClient, SnowflakeClientConfig};
use crate::connection::{SnowflakeConnection, SnowflakeCredentials, SnowflakeTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

/// Configuration for Snowflake endpoint with read/write connection support.
#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "SnowflakeConfig")]
pub struct SnowflakeConfig {
    /// Target configuration
    pub target: SnowflakeTarget,
    /// Read credentials
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<SnowflakeCredentials>,
    /// Write credentials
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<SnowflakeCredentials>,
    /// Admin credentials
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<SnowflakeCredentials>,
    /// System credentials
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<SnowflakeCredentials>,
}

impl_ep_config_target_auth!(SnowflakeConfig, SnowflakeConnection, SnowflakeTarget, SnowflakeCredentials, EpKind::Snowflake);

impl fmt::Display for SnowflakeConfig {
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
struct SnowflakeConfigRaw {
    #[serde(default)]
    target: Option<SnowflakeTarget>,
    #[serde(default)]
    read_credentials: Option<SnowflakeCredentials>,
    #[serde(default)]
    write_credentials: Option<SnowflakeCredentials>,
    #[serde(default)]
    admin_credentials: Option<SnowflakeCredentials>,
    #[serde(default)]
    system_credentials: Option<SnowflakeCredentials>,

    #[serde(default)]
    read_conn: Option<SnowflakeConnection>,
    #[serde(default)]
    write_conn: Option<SnowflakeConnection>,
    #[serde(default)]
    admin_conn: Option<SnowflakeConnection>,
    #[serde(default)]
    system_conn: Option<SnowflakeConnection>,
}

impl<'de> Deserialize<'de> for SnowflakeConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = SnowflakeConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(SnowflakeConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<SnowflakeConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(SnowflakeConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(SnowflakeConfig::default())
        }
    }
}

impl RWPool<SnowflakeAsync> for SnowflakeConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<SnowflakeAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<SnowflakeConnection>() {
            Some(snowflake_conn) => snowflake_conn.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        // Create a pool of clients (fewer than ClickHouse since Snowflake uses HTTP)
        for _ in 0..2 {
            let client = create_snowflake_client(&connection)?;
            clients.push(Arc::new(client));
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}

/// Create a Snowflake client from connection configuration.
fn create_snowflake_client(conn: &SnowflakeConnection) -> Result<SnowflakeClient, EpError> {
    let config = SnowflakeClientConfig {
        account: conn.account.clone(),
        user: conn.user.clone(),
        private_key_pem: conn.private_key.clone(),
        oauth_token: conn.oauth_token.clone(),
        warehouse: conn.warehouse.clone(),
        database: conn.database.clone(),
        schema: conn.schema.clone(),
        role: conn.role.clone(),
        timeout: conn.timeout.unwrap_or(0),
        host: None,
    };

    SnowflakeClient::new(config)
}
