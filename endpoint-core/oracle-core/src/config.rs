use crate::OracleAsync;
use crate::connection::{OracleConnection, OracleCredentials, OracleTarget};

use bb8::Pool as Bb8_Pool;
use bb8_oracle::OracleConnectionManager;
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "OracleConfig")]
pub struct OracleConfig {
    pub target: OracleTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<OracleCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<OracleCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<OracleCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<OracleCredentials>,
}

impl_ep_config_target_auth!(OracleConfig, OracleConnection, OracleTarget, OracleCredentials, EpKind::Oracle);

impl fmt::Display for OracleConfig {
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
struct OracleConfigRaw {
    #[serde(default)]
    target: Option<OracleTarget>,
    #[serde(default)]
    read_credentials: Option<OracleCredentials>,
    #[serde(default)]
    write_credentials: Option<OracleCredentials>,
    #[serde(default)]
    admin_credentials: Option<OracleCredentials>,
    #[serde(default)]
    system_credentials: Option<OracleCredentials>,

    #[serde(default)]
    read_conn: Option<OracleConnection>,
    #[serde(default)]
    write_conn: Option<OracleConnection>,
    #[serde(default)]
    admin_conn: Option<OracleConnection>,
    #[serde(default)]
    system_conn: Option<OracleConnection>,
}

impl<'de> Deserialize<'de> for OracleConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = OracleConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(OracleConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<OracleConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(OracleConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(OracleConfig::default())
        }
    }
}

impl RWPool<OracleAsync> for OracleConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Bb8_Pool<OracleConnectionManager>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<OracleConnection>() {
            Some(oracle_conn) => oracle_conn.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut connector = oracle::Connector::new(connection.auth.username.clone(), connection.auth.password.clone(), connection.url);

        if let Some(privelege) = connection.auth.get_privelege() {
            connector.privilege(privelege);
        }

        let manager = OracleConnectionManager::from_connector(connector);

        Bb8_Pool::builder().min_idle(2).max_size(8).build(manager).await.map_err(EpError::connect)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::OracleConfig;

    #[test]
    fn serde_json() {
        let _conn = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=g48357ea2b86899_rcp2bndnbbimkb8j_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))".to_string();

        let config = OracleConfig {
            target: Default::default(),
            read_credentials: None,
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
        };

        println!("{}", serde_json::to_string(&config).unwrap_or_default())
    }
}
