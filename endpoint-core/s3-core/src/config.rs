use crate::S3Async;
use crate::comm::S3Client;
use crate::connection::{S3Connection, S3Credentials, S3Target};

use borsh::{BorshDeserialize, BorshSerialize};
use deadpool::unmanaged::Pool;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "S3Config")]
pub struct S3Config {
    pub target: S3Target,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<S3Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<S3Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<S3Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<S3Credentials>,
}

impl_ep_config_target_auth!(S3Config, S3Connection, S3Target, S3Credentials, EpKind::S3);

impl fmt::Display for S3Config {
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
struct S3ConfigRaw {
    #[serde(default)]
    target: Option<S3Target>,
    #[serde(default)]
    read_credentials: Option<S3Credentials>,
    #[serde(default)]
    write_credentials: Option<S3Credentials>,
    #[serde(default)]
    admin_credentials: Option<S3Credentials>,
    #[serde(default)]
    system_credentials: Option<S3Credentials>,

    #[serde(default)]
    read_conn: Option<S3Connection>,
    #[serde(default)]
    write_conn: Option<S3Connection>,
    #[serde(default)]
    admin_conn: Option<S3Connection>,
    #[serde(default)]
    system_conn: Option<S3Connection>,
}

impl<'de> Deserialize<'de> for S3Config {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = S3ConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(S3Config {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<S3Connection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(S3Config {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(S3Config::default())
        }
    }
}

impl RWPool<S3Async> for S3Config {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Pool<S3Client>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<S3Connection>() {
            Some(s3_connection) => s3_connection.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        for _ in 0..2 {
            clients.push(S3Client::new(&connection).await?);
        }

        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}
