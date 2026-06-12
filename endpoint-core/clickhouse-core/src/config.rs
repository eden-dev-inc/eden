use crate::ClickhouseAsync;
use crate::connection::{ClickhouseConnection, ClickhouseCredentials, ClickhouseTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use hyper::HeaderMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "ClickhouseConfig")]
pub struct ClickhouseConfig {
    pub target: ClickhouseTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<ClickhouseCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<ClickhouseCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<ClickhouseCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<ClickhouseCredentials>,
}

impl_ep_config_target_auth!(ClickhouseConfig, ClickhouseConnection, ClickhouseTarget, ClickhouseCredentials, EpKind::Clickhouse);

impl fmt::Display for ClickhouseConfig {
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
struct ClickhouseConfigRaw {
    #[serde(default)]
    target: Option<ClickhouseTarget>,
    #[serde(default)]
    read_credentials: Option<ClickhouseCredentials>,
    #[serde(default)]
    write_credentials: Option<ClickhouseCredentials>,
    #[serde(default)]
    admin_credentials: Option<ClickhouseCredentials>,
    #[serde(default)]
    system_credentials: Option<ClickhouseCredentials>,

    #[serde(default)]
    read_conn: Option<ClickhouseConnection>,
    #[serde(default)]
    write_conn: Option<ClickhouseConnection>,
    #[serde(default)]
    admin_conn: Option<ClickhouseConnection>,
    #[serde(default)]
    system_conn: Option<ClickhouseConnection>,
}

impl<'de> Deserialize<'de> for ClickhouseConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = ClickhouseConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(ClickhouseConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<ClickhouseConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(ClickhouseConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
            })
        } else {
            Ok(ClickhouseConfig::default())
        }
    }
}

impl RWPool<ClickhouseAsync> for ClickhouseConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<ClickhouseAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<ClickhouseConnection>() {
            Some(clickhouse_config) => clickhouse_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(ClickhouseClient::build(&connection).await?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}

#[derive(Clone, Default)]
pub struct ClickhouseClient {
    // client: Client, // reqwest client
    client: clickhouse::Client,
    url: String, // base url
}

impl Debug for ClickhouseClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "clichouse_client: {}", self.url)
    }
}

impl ClickhouseClient {
    pub async fn build(conn: &ClickhouseConnection) -> Result<clickhouse::Client, EpError> {
        let _header = HeaderMap::new();

        let mut client = clickhouse::Client::default().with_url(conn.url.to_string());

        if let Some(database) = &conn.database {
            client = client.with_database(database);
        }

        if let Some(user) = &conn.user {
            client = client.with_user(user.to_string());
        }

        if let Some(password) = &conn.password {
            client = client.with_password(password);
        }

        if let Some(compression) = &conn.compression {
            client = client.with_compression(clickhouse::Compression::from(compression.clone()));
        }

        if let Some(options) = &conn.options {
            for option in options {
                client = client.with_option(&option.name, &option.value);
            }
        }

        if let Some(product_info) = &conn.product_info {
            client = client.with_product_info(&product_info.name, &product_info.version);
        }

        // ensure that all data responses are with JSON (ClickHouse format names are case-sensitive)
        client = client.with_header("X-ClickHouse-Format", "JSON");
        client = client.with_header("Accept", "application/json");

        Ok(client)
    }

    pub async fn query(&self, body: &str) -> Result<Vec<String>, EpError> {
        self.client.query(body).fetch_all::<String>().await.map_err(EpError::request)
    }
}
