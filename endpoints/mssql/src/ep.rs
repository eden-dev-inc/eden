use crate::api::lib::MssqlApi;
use crate::metadata::MssqlMetadata;
use crate::request::MssqlRequest;
use dashmap::DashMap;
use eden_logger_internal::LogContext;
use endpoint_types::ep::{EP, EpTransaction};
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::EpPool;
use ep_core::ep::EpRouter;
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, impl_endpoint};
use error::{EpError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_name::named;
use mssql_core::config::MssqlConfig;
use mssql_core::{MssqlAsync, MssqlTx};
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;
use telemetry::TelemetryWrapper;
use tiberius::{ExecuteResult, Row};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Mssql => Mssql,  MssqlAsync);

ep_core::impl_endpoint_lifecycle_spec!(MssqlEp, MssqlAsync, MssqlConfig, MssqlRequest, MssqlMetadata, MssqlApi, MssqlTx);

impl EP<MssqlAsync, MssqlConfig, MssqlRequest, MssqlMetadata, MssqlApi, MssqlTx> for MssqlEp {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::default()
    }
    #[named]
    async fn transaction(
        &self,
        _endpoint_cache_uuid: &EndpointCacheUuid,
        _outpound: UnboundedSender<Result<(), EpError>>,
        _inbound: Receiver<bool>,
        _transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        Err(EpError::Transaction(TransactionError::NotImplemented))
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let mut conn = pool.get().await.map_err(EpError::connect)?;

        conn.simple_query("SELECT 1;").await
    }
    fn kind() -> EpKind {
        EpKind::Mssql
    }

    async fn process_wire_protocol(
        &self,
        _endpoint_cache_uuid: &EndpointCacheUuid,
        _receiver: &mut UnboundedReceiver<Vec<u8>>,
        _sender: UnboundedSender<Vec<u8>>,
        _settings: EdenSettings,
        _interlay_cache_uuid: InterlayCacheUuid,
        _interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        _telemetry_wrapper: &mut TelemetryWrapper,
        _ctx: LogContext,
    ) {
        //DO_NOTHING
    }
}

#[allow(dead_code)]
fn parse_rows(rows: Vec<Row>) -> String {
    "[".to_string()
        + rows
            .iter()
            .map(parse_row) //format!("{}", serde_json::to_string(&row).unwrap_or_default()))
            .collect::<Vec<String>>()
            .join(",")
            .as_str()
        + "]"
}

#[allow(dead_code)]
fn parse_row(row: &Row) -> String {
    let mut s = vec![];
    for col in row.columns() {
        let val = col.name();
        s.push(format!("\"{}\":\"{}\"", col.name(), val));
    }
    "{".to_string() + &s.join(",") + "}"
}

#[allow(dead_code)]
fn parse_writes(res: ExecuteResult) -> String {
    "[".to_string()
        + res
            .into_iter()
            .map(|r| format!("\"{r}\"")) //format!("{}", serde_json::to_string(&row).unwrap_or_default()))
            .collect::<Vec<String>>()
            .join(",")
            .as_str()
        + "]"
}
