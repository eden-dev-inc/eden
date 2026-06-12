use crate::SalesforceAsync;
use crate::connection::{SalesforceConnection, SalesforceCredentials, SalesforceTarget};

use super::comm::SalesforceClient;
use borsh::{BorshDeserialize, BorshSerialize};
use deadpool::unmanaged::Pool;
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
#[schema(title = "SalesforceConfig")]
pub struct SalesforceConfig {
    pub target: SalesforceTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<SalesforceCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<SalesforceCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<SalesforceCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<SalesforceCredentials>,
}

impl_ep_config_target_auth!(SalesforceConfig, SalesforceConnection, SalesforceTarget, SalesforceCredentials, EpKind::Salesforce);

impl fmt::Display for SalesforceConfig {
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
struct SalesforceConfigRaw {
    #[serde(default)]
    target: Option<SalesforceTarget>,
    #[serde(default)]
    read_credentials: Option<SalesforceCredentials>,
    #[serde(default)]
    write_credentials: Option<SalesforceCredentials>,
    #[serde(default)]
    admin_credentials: Option<SalesforceCredentials>,
    #[serde(default)]
    system_credentials: Option<SalesforceCredentials>,

    #[serde(default)]
    read_conn: Option<SalesforceConnection>,
    #[serde(default)]
    write_conn: Option<SalesforceConnection>,
    #[serde(default)]
    admin_conn: Option<SalesforceConnection>,
    #[serde(default)]
    system_conn: Option<SalesforceConnection>,
}

impl<'de> Deserialize<'de> for SalesforceConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = SalesforceConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(SalesforceConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<SalesforceConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(SalesforceConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(SalesforceConfig::default())
        }
    }
}

impl RWPool<SalesforceAsync> for SalesforceConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Pool<SalesforceClient>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));
        let connection = match connection.as_any().downcast_ref::<SalesforceConnection>() {
            Some(config) => config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };
        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(SalesforceClient::new(&connection).await?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}
