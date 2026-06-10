use crate::api::lib::MysqlApi;
use crate::api::lib::query::QueryInput;
use crate::metadata::MysqlMetadata;
use crate::request::MysqlRequest;
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
use mysql_core::{MysqlAsync, MysqlTx, config::MysqlConfig};
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Mysql => Mysql, MysqlAsync);

ep_core::impl_endpoint_lifecycle_spec!(MysqlEp, MysqlAsync, MysqlConfig, MysqlRequest, MysqlMetadata, MysqlApi, MysqlTx);

impl EP<MysqlAsync, MysqlConfig, MysqlRequest, MysqlMetadata, MysqlApi, MysqlTx> for MysqlEp {
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

        QueryInput::new("SELECT 1;".to_string()).run_query(pool.clone()).await.map(|_| ())
    }
    fn kind() -> EpKind {
        EpKind::Mysql
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
