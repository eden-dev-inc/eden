use crate::api::lib::AzureApi;
use crate::metadata::AzureMetadata;
use crate::request::AzureRequest;
use crate::{EP, EpTransaction, Transaction};
use azure_core::comm::AzureClient;
use azure_core::config::AzureConfig;
use dashmap::DashMap;
use deadpool::unmanaged::Pool;
use eden_logger_internal::LogContext;
use ep_core::GetPool;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::impl_endpoint;
use ep_core::settings::EdenSettings;
use error::{EpError, RequestError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanStatus;

use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

pub type AzureAsync = Pool<AzureClient>;
pub type AzureTx = Pool<AzureClient>;

impl_endpoint!(EpKind::Azure => Azure, AzureAsync);

ep_core::impl_endpoint_lifecycle_spec!(AzureEp, AzureAsync, AzureConfig, AzureRequest, AzureMetadata, AzureApi, AzureTx);

impl EP<AzureAsync, AzureConfig, AzureRequest, AzureMetadata, AzureApi, AzureTx> for AzureEp {
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

        let tx = match transaction.as_any().downcast_ref::<Transaction<AzureRequest>>() {
            Some(tx) => tx.to_owned(),
            None => return Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        };

        span.add_event("processing transaction", vec![]);

        span.add_event("preparing transaction", vec![]);

        outbound.send(Ok(())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;

        span.add_event("waiting for inbound", vec![]);

        let confirmation = tokio::spawn(async { inbound.await.map_err(EpError::transaction) }).await.map_err(EpError::transaction)??;

        if !confirmation {
            span.add_event("rolling back transaction", vec![]);
            return Err(EpError::Transaction(TransactionError::Rollback));
        }

        span.add_event("committing transaction", vec![]);

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
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let client = pool.get().await.map_err(EpError::connect)?;

        client.health_check().await?;

        Ok(())
    }
    fn kind() -> EpKind {
        EpKind::Azure
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
