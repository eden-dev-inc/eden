use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use eden_logger_internal::LogContext;
use ep_core::GetPool;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::EpRouter;
use ep_core::settings::EdenSettings;
use error::{EpError, ResultEP, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use postgres::ep::PostgresEp;
use postgres::request::PostgresRequest;
use postgres_core::{PostgresAsync, PostgresConfig, PostgresTx};
use serde_json::Value;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

use endpoint_types::{EP, EpTransaction, Operation, RunRequest, Transaction};

use crate::api::control_plane::RdsApi;
use crate::request::RdsRequest;
use postgres::metadata::PostgresMetadata;

#[derive(Debug, Clone, Default)]
pub struct RdsEp(pub PostgresEp);

impl GetPool<PostgresAsync> for RdsEp {
    fn pool(&self) -> &ep_core::ep::EpPool<PostgresAsync> {
        self.0.pool()
    }

    fn mut_pool(&mut self) -> &mut ep_core::ep::EpPool<PostgresAsync> {
        self.0.mut_pool()
    }
}

impl EpRouter for RdsEp {
    fn as_router(self: Box<Self>) -> Box<dyn EpRouter> {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

ep_core::impl_endpoint_lifecycle_spec!(RdsEp, PostgresAsync, PostgresConfig, RdsRequest, PostgresMetadata, RdsApi, PostgresTx);

impl EP<PostgresAsync, PostgresConfig, RdsRequest, PostgresMetadata, RdsApi, PostgresTx> for RdsEp {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::default()
    }

    fn validate_operation(&self, op: &dyn Operation<PostgresAsync, RdsApi, PostgresTx>) -> ResultEP<()> {
        match op.kind() {
            RdsApi::Postgres => {
                // All postgres data-plane operations are allowed through RDS
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn transaction(
        &self,
        _endpoint_cache_uuid: &EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let tx = match transaction.as_any().downcast_ref::<Transaction<RdsRequest>>() {
            Some(tx) => tx,
            None => {
                let err = EpError::transaction("failed to downcast rds transaction");
                outbound.send(Err(err.clone())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
                return Err(err);
            }
        };

        let mut postgres_requests = Vec::with_capacity(tx.0.len());
        for req in &tx.0 {
            if let Err(err) = self.validate_operation(req.operation()) {
                outbound.send(Err(err.clone())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
                return Err(err);
            }

            match req.as_postgres_request() {
                Some(pg_req) => postgres_requests.push(pg_req),
                None => {
                    let err = EpError::transaction("control-plane operations are not supported in rds transactions");
                    outbound.send(Err(err.clone())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
                    return Err(err);
                }
            }
        }

        let pg_tx = Transaction::<PostgresRequest>(postgres_requests);
        self.0.transaction(_endpoint_cache_uuid, outbound, inbound, &pg_tx, _settings, telemetry_wrapper).await
    }

    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        self.0.health_check(endpoint_cache_uuid, telemetry_wrapper).await
    }

    fn kind() -> EpKind {
        EpKind::Rds
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
        // Wire protocol not supported for RDS — use data-plane operations instead.
    }
}
