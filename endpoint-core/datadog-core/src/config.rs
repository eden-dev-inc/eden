#[cfg(feature = "sdk")]
use crate::DatadogAsync;
use crate::connection::{DatadogConnection, DatadogCredentials, DatadogTarget};

#[cfg(feature = "sdk")]
use super::comm::DatadogClient;
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "sdk")]
use deadpool::unmanaged::Pool;
#[cfg(feature = "sdk")]
use ep_core::ep::RWPool;
use ep_core::ep::{EpConfig, EpConnection};
use ep_core::impl_ep_config_target_auth;
#[cfg(feature = "sdk")]
use error::ConnectError;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
#[cfg(feature = "sdk")]
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
#[cfg(feature = "sdk")]
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "DatadogConfig")]
pub struct DatadogConfig {
    pub target: DatadogTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<DatadogCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<DatadogCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<DatadogCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<DatadogCredentials>,
}

impl_ep_config_target_auth!(DatadogConfig, DatadogConnection, DatadogTarget, DatadogCredentials, EpKind::Datadog);

impl fmt::Display for DatadogConfig {
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
struct DatadogConfigRaw {
    #[serde(default)]
    target: Option<DatadogTarget>,
    #[serde(default)]
    read_credentials: Option<DatadogCredentials>,
    #[serde(default)]
    write_credentials: Option<DatadogCredentials>,
    #[serde(default)]
    admin_credentials: Option<DatadogCredentials>,
    #[serde(default)]
    system_credentials: Option<DatadogCredentials>,

    #[serde(default)]
    read_conn: Option<DatadogConnection>,
    #[serde(default)]
    write_conn: Option<DatadogConnection>,
    #[serde(default)]
    admin_conn: Option<DatadogConnection>,
    #[serde(default)]
    system_conn: Option<DatadogConnection>,
}

impl<'de> Deserialize<'de> for DatadogConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = DatadogConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(DatadogConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<DatadogConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(DatadogConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(DatadogConfig::default())
        }
    }
}

#[cfg(feature = "sdk")]
impl RWPool<DatadogAsync> for DatadogConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Pool<DatadogClient>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<DatadogConnection>() {
            Some(dd_config) => dd_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(DatadogClient::new(&connection).await?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}
