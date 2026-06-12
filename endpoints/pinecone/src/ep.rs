use crate::api::lib::PineconeApi;
use crate::metadata::PineconeMetadata;
use crate::request::PineconeRequest;
use crate::{EP, EpTransaction, Transaction};
use dashmap::DashMap;
use eden_logger_internal::LogContext;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, impl_endpoint};
use error::{EpError, RequestError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_name::named;
use pinecone_core::comm::PineconeRequests;
use pinecone_core::config::PineconeConfig;
use pinecone_core::{PineconeAsync, PineconeTx};
use serde_json::Value;
use std::sync::Arc;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Pinecone => Pinecone,  PineconeAsync);

ep_core::impl_endpoint_lifecycle_spec!(
    PineconeEp,
    PineconeAsync,
    PineconeConfig,
    PineconeRequest,
    PineconeMetadata,
    PineconeApi,
    PineconeTx
);

impl EP<PineconeAsync, PineconeConfig, PineconeRequest, PineconeMetadata, PineconeApi, PineconeTx> for PineconeEp {
    fn new() -> Self {
        Self::default()
    }
    #[named]
    async fn transaction(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        outpound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("pinecone.{}.{}", Self::kind(), function_name!()));

        let tx = match transaction.as_any().downcast_ref::<Transaction<PineconeRequest>>() {
            Some(tx) => tx.to_owned(),
            None => return Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        };

        span.add_simple_event("processing transaction");

        outpound.send(Ok(())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;

        span.add_simple_event("waiting for inbound");

        let confirmation = tokio::spawn(async { inbound.await.map_err(EpError::transaction) }).await.map_err(EpError::transaction)??;

        if !confirmation {
            span.add_simple_event("rolling back transaction");
            return Err(EpError::Transaction(TransactionError::Rollback));
        }

        span.add_simple_event("commiting transaction");

        let mut results = vec![];
        for req in &tx.0 {
            self.write(endpoint_cache_uuid, req, settings, telemetry_wrapper).await?;
        }

        if results.len() == 1 {
            results.pop().unwrap_or(Err(EpError::Request(RequestError::FailedToUnwrapResponse)))
        } else {
            serde_json::to_value(&results).map_err(EpError::transaction)
        }
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = &mut pool.get().await.map_err(EpError::connect)?;

        match conn.describe_index_stats().await {
            Ok(_) => Ok(()),
            Err(err) => Err(EpError::request(err)),
        }
    }
    fn kind() -> EpKind {
        EpKind::Pinecone
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
