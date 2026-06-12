//https://www.oracle.com/cloud/free/

use crate::api::lib::OracleApi;
use crate::metadata::OracleMetadata;
use crate::request::OracleRequest;
use crate::{EP, EpTransaction};
use dashmap::DashMap;
use eden_logger_internal::LogContext;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, impl_endpoint};
use error::{EpError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_name::named;
use oracle_client::{ColumnInfo, ResultSet, Row};
use oracle_core::config::OracleConfig;
use oracle_core::{OracleAsync, OracleTx};
use serde_json::Value;
use std::sync::Arc;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Oracle => Oracle,  OracleAsync);

ep_core::impl_endpoint_lifecycle_spec!(OracleEp, OracleAsync, OracleConfig, OracleRequest, OracleMetadata, OracleApi, OracleTx);

impl EP<OracleAsync, OracleConfig, OracleRequest, OracleMetadata, OracleApi, OracleTx> for OracleEp {
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
        let _span = telemetry_wrapper.client_tracer(format!("oracle.{}.{}", Self::kind(), function_name!()));

        Err(EpError::Transaction(TransactionError::NotImplemented))
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = &mut pool.get().await.map_err(EpError::connect)?;

        match conn.ping() {
            Ok(_) => Ok(()),
            Err(err) => Err(EpError::request(err)),
        }
    }
    fn kind() -> EpKind {
        EpKind::Oracle
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
fn parse_rows(results: ResultSet<Row>) -> Result<String, EpError> {
    let columns = results.column_info().to_vec();

    let mut output = vec![];
    for r in results {
        let row = r.map_err(EpError::request)?;
        output.push(parse_row(row, columns.clone())?);
    }

    Ok("[".to_string() + output.join(",").as_str() + "]")

    // "[".to_string()
    //     + rows.into_iter()
    //         .map(pg_parse_row) //format!("{}", serde_json::to_string(&row).unwrap_or_default()))
    //         .collect::<Vec<String>>()
    //         .join(",")
    //         .as_str()
    //     + "]"
}

#[allow(dead_code)]
fn parse_row(row: Row, columns: Vec<ColumnInfo>) -> Result<String, EpError> {
    let mut s = vec![];
    for (index, col) in columns.iter().enumerate() {
        let val = column_value(&row, index)?;
        s.push(format!("\"{}\":\"{}\"", col.name(), val));
    }
    Ok("{".to_string() + &s.join(",") + "}")
}

#[allow(dead_code)]
fn column_value(row: &Row, index: usize) -> Result<String, EpError> {
    row.get(index).map_err(EpError::request)
    // row.get::<&col.type_id(), String>(col.name())
    // match *col.type_id() {
    //     Type::TEXT => Some(row.get::<&str, String>(col.name())),
    //     Type::INT4 => Some(format!("{}", row.get::<&str, i32>(col.name()))),
    //     _ => Some(col.type_().to_string()),
    // }
}
