use std::sync::Arc;

use super::request::SnowflakeRequest;
use crate::api::lib::SnowflakeApi;
use crate::metadata::SnowflakeMetadata;
use crate::{EP, EpTransaction, Transaction};
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
use serde_json::Value;
use snowflake_core::config::SnowflakeConfig;
use snowflake_core::{SnowflakeAsync, SnowflakeTx};
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Snowflake => Snowflake, SnowflakeAsync);

ep_core::impl_endpoint_lifecycle_spec!(
    SnowflakeEp,
    SnowflakeAsync,
    SnowflakeConfig,
    SnowflakeRequest,
    SnowflakeMetadata,
    SnowflakeApi,
    SnowflakeTx
);

impl EP<SnowflakeAsync, SnowflakeConfig, SnowflakeRequest, SnowflakeMetadata, SnowflakeApi, SnowflakeTx> for SnowflakeEp {
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

        let tx = transaction
            .as_any()
            .downcast_ref::<Transaction<SnowflakeRequest>>()
            .ok_or(EpError::Transaction(TransactionError::FailedToDowncast))?;

        span.add_simple_event("processing transaction");

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
            results.push(match self.write(endpoint_cache_uuid, req, settings, telemetry_wrapper).await {
                Ok(res) => res,
                Err(e) => serde_json::from_str(&e.to_string()).map_err(EpError::transaction)?,
            })
        }
        serde_json::to_value(&results).map_err(EpError::transaction)
    }

    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = pool.get().await.map_err(EpError::connect)?;

        // Execute a simple SELECT 1 query to verify connection
        match conn.execute("SELECT 1").await {
            Ok(_) => Ok(()),
            Err(err) => Err(EpError::request(err.to_string())),
        }
    }

    fn kind() -> EpKind {
        EpKind::Snowflake
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
        // Snowflake uses HTTP REST API, not a binary wire protocol
        // This is intentionally left empty
    }
}
