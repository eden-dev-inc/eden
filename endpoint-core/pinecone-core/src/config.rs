use crate::PineconeAsync;
use crate::comm::PineconeClient;
use crate::connection::{PineconeConnection, PineconeCredentials, PineconeTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use deadpool::unmanaged::Pool;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "PineconeConfig")]
pub struct PineconeConfig {
    pub target: PineconeTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<PineconeCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<PineconeCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<PineconeCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<PineconeCredentials>,
}

impl_ep_config_target_auth!(PineconeConfig, PineconeConnection, PineconeTarget, PineconeCredentials, EpKind::Pinecone);

impl fmt::Display for PineconeConfig {
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
struct PineconeConfigRaw {
    #[serde(default)]
    target: Option<PineconeTarget>,
    #[serde(default)]
    read_credentials: Option<PineconeCredentials>,
    #[serde(default)]
    write_credentials: Option<PineconeCredentials>,
    #[serde(default)]
    admin_credentials: Option<PineconeCredentials>,
    #[serde(default)]
    system_credentials: Option<PineconeCredentials>,

    #[serde(default)]
    read_conn: Option<PineconeConnection>,
    #[serde(default)]
    write_conn: Option<PineconeConnection>,
    #[serde(default)]
    admin_conn: Option<PineconeConnection>,
    #[serde(default)]
    system_conn: Option<PineconeConnection>,
}

impl<'de> Deserialize<'de> for PineconeConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = PineconeConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(PineconeConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<PineconeConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(PineconeConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(PineconeConfig::default())
        }
    }
}

impl RWPool<PineconeAsync> for PineconeConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Pool<PineconeClient>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("pinecone.{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<PineconeConnection>() {
            Some(mongo_config) => mongo_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(PineconeClient::new(&connection).await?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}
