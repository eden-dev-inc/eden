use crate::api::lib::FunctionApi;
use crate::metadata::FunctionMetadata;
use crate::request::FunctionRequest;
use crate::{EP, EpTransaction, Transaction};
use dashmap::DashMap;
use deadpool::unmanaged::Pool;
use eden_logger_internal::LogContext;
use ep_core::GetPool;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::impl_endpoint;
use ep_core::settings::EdenSettings;
use error::{EpError, RequestError, ResultEP, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_core::comm::FunctionClient;
use function_core::config::FunctionConfig;
use function_name::named;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

pub type FunctionAsync = Pool<FunctionClient>;
pub type FunctionTx = Pool<FunctionClient>;

impl_endpoint!(EpKind::Function => Function,  FunctionAsync);

ep_core::impl_endpoint_lifecycle_spec!(
    FunctionEp,
    FunctionAsync,
    FunctionConfig,
    FunctionRequest,
    FunctionMetadata,
    FunctionApi,
    FunctionTx
);

impl EP<FunctionAsync, FunctionConfig, FunctionRequest, FunctionMetadata, FunctionApi, FunctionTx> for FunctionEp {
    fn new() -> Self {
        Self::default()
    }

    #[named]
    async fn transaction(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let tx = match transaction.as_any().downcast_ref::<Transaction<FunctionRequest>>() {
            Some(tx) => tx.to_owned(),
            None => return Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        };

        span.add_simple_event("processing transaction");
        span.add_simple_event("preparing transaction");

        outbound.send(Ok(())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;

        span.add_simple_event("waiting for inbound");

        let confirmation = tokio::spawn(async { inbound.await.map_err(EpError::transaction) }).await.map_err(EpError::transaction)??;

        if !confirmation {
            span.add_simple_event("rolling back transaction");
            return Err(EpError::Transaction(TransactionError::Rollback));
        }

        span.add_simple_event("commiting transaction");

        let mut results = vec![];
        for req in &tx.0 {
            results.push(self.write(endpoint_cache_uuid, req, settings, telemetry_wrapper).await)
        }

        if results.len() == 1 {
            results.pop().unwrap_or(Err(EpError::Request(RequestError::FailedToUnwrapResponse)))
        } else {
            serde_json::to_value(&results).map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::transaction(e)
            })
        }
    }

    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().write_conn_async(endpoint_cache_uuid).await?;
        let client = pool.get().await.map_err(EpError::connect)?;

        client.health_check().await?;

        Ok(())
    }

    fn kind() -> EpKind {
        EpKind::Function
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
        // DO_NOTHING
    }
}
