use crate::CassandraAsync;
use crate::connection::{CassandraConnection, CassandraCredentials, CassandraTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::ResultEP;
use error::{ConnectError, EpError};
use format::endpoint::EpKind;
use function_name::named;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::host_filter::DcHostFilter;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "CassandraConfig")]
pub struct CassandraConfig {
    pub target: CassandraTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<CassandraCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<CassandraCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<CassandraCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<CassandraCredentials>,
}

impl CassandraConfig {
    #[allow(dead_code)]
    fn new() -> CassandraConfig {
        CassandraConfig::default()
    }
}

impl_ep_config_target_auth!(CassandraConfig, CassandraConnection, CassandraTarget, CassandraCredentials, EpKind::Cassandra);

impl fmt::Display for CassandraConfig {
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
struct CassandraConfigRaw {
    #[serde(default)]
    target: Option<CassandraTarget>,
    #[serde(default)]
    read_credentials: Option<CassandraCredentials>,
    #[serde(default)]
    write_credentials: Option<CassandraCredentials>,
    #[serde(default)]
    admin_credentials: Option<CassandraCredentials>,
    #[serde(default)]
    system_credentials: Option<CassandraCredentials>,

    #[serde(default)]
    read_conn: Option<CassandraConnection>,
    #[serde(default)]
    write_conn: Option<CassandraConnection>,
    #[serde(default)]
    admin_conn: Option<CassandraConnection>,
    #[serde(default)]
    system_conn: Option<CassandraConnection>,
}

impl<'de> Deserialize<'de> for CassandraConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = CassandraConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(CassandraConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<CassandraConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(CassandraConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(CassandraConfig::default())
        }
    }
}

impl RWPool<CassandraAsync> for CassandraConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<CassandraAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<CassandraConnection>() {
            Some(mongo_config) => mongo_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(session_from_connection(connection.clone()).await.map_err(EpError::connect)?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}

async fn session_from_connection(connection: CassandraConnection) -> ResultEP<Session> {
    let session = SessionBuilder::new()
        .known_nodes(connection.known_nodes)
        // .known_nodes_addr(
        //     connection
        //         .known_nodes_addr
        //         .iter()
        //         .map(|n| n.into())
        //         .collect::<Vec<SocketAddr>>(),
        // )
        .compression(connection.compression.map(Into::into));

    let session = match connection.user {
        Some(user) => session.user(user.username, user.password),
        None => session,
    };

    let session = match connection.auto_await_schema_agreement {
        Some(auto_await_schema_agreement) => session.auto_await_schema_agreement(auto_await_schema_agreement),
        None => session,
    };

    let session = match connection.cluster_metadata_refresh_interval {
        Some(cluster_metadata_refresh_interval) => {
            session.cluster_metadata_refresh_interval(Duration::from_millis(cluster_metadata_refresh_interval))
        }
        None => session,
    };

    let session = match connection.keepalive_interval {
        Some(keepalive_interval) => session.keepalive_interval(Duration::from_millis(keepalive_interval)),
        None => session,
    };

    let session = match connection.keepalive_timeout {
        Some(keepalive_timeout) => session.keepalive_timeout(Duration::from_millis(keepalive_timeout)),
        None => session,
    };

    let session = match connection.schema_agreement_interval {
        Some(schema_agreement_interval) => session.schema_agreement_interval(Duration::from_millis(schema_agreement_interval)),
        None => session,
    };

    let session = match connection.schema_agreement_timeout {
        Some(schema_agreement_timeout) => session.schema_agreement_timeout(Duration::from_millis(schema_agreement_timeout)),
        None => session,
    };

    // let mut session = match connection.keyspaces_to_fetch {
    //     Some(keyspaces_to_fetch) => session.keyspaces_to_fetch(keyspaces_to_fetch),
    //     None => session,
    // };

    let session = match connection.tcp_keepalive_interval {
        Some(tcp_keepalive_interval) => session.tcp_keepalive_interval(Duration::from_millis(tcp_keepalive_interval)),
        None => session,
    };

    let session = match connection.tracing_info_fetch_interval {
        Some(tracing_info_fetch_interval) => session.tracing_info_fetch_interval(Duration::from_millis(tracing_info_fetch_interval)),
        None => session,
    };

    let session = match connection.tracing_info_fetch_attempts {
        Some(tracing_info_fetch_attempts) => session.tracing_info_fetch_attempts(tracing_info_fetch_attempts.0),
        None => session,
    };

    let session = match connection.tracing_info_fetch_consistency {
        Some(tracing_info_fetch_consistency) => session.tracing_info_fetch_consistency(tracing_info_fetch_consistency.into()),
        None => session,
    };

    let session = match connection.timeout {
        Some(timeout) => session.connection_timeout(Duration::from_millis(timeout)),
        None => session,
    };

    // let mut session = match connection.pool_size {
    //     Some(pool_size) => session.pool_size(pool_size),
    //     None => session,
    // };

    let session = match connection.use_keyspace {
        Some(keyspace) => session.use_keyspace(keyspace.keyspace_name, keyspace.case_sensitive),
        None => session,
    };

    let session = match connection.write_coalescing {
        Some(write_coalescing) => session.write_coalescing(write_coalescing),
        None => session,
    };

    let session = match connection.write_coalescing {
        Some(write_coalescing) => session.write_coalescing(write_coalescing),
        None => session,
    };

    let session = match connection.host_filter {
        Some(host_filter) => session.host_filter(Arc::new(DcHostFilter::new(host_filter))),
        None => session,
    };

    session.build().await.map_err(EpError::connect)
}
