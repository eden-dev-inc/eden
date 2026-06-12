use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use eden_logger_internal::LogContext;
use ep_core::GetPool;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::EpRouter;
use ep_core::settings::EdenSettings;
use ep_redis::ep::RedisEp;
use ep_redis::metadata::RedisMetadata;
use ep_redis::redis_like::process_wire_protocol_with_bytes;
use ep_redis::request::RedisRequest;
use error::{EpError, ResultEP, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use redis_core::config::RedisConfig;
use redis_core::{RedisAsync, RedisTx};
use serde_json::Value;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

use endpoint_types::{EP, EpTransaction, Operation, RunRequest, Transaction};

use crate::api::control_plane::ElasticacheApi;
use crate::policy;
use crate::protocol::ElasticacheBytes;
use crate::request::{ElasticacheRedisOperation, ElasticacheRequest};

#[derive(Debug, Clone, Default)]
pub struct ElasticacheEp(pub RedisEp);

impl GetPool<RedisAsync> for ElasticacheEp {
    fn pool(&self) -> &ep_core::ep::EpPool<RedisAsync> {
        self.0.pool()
    }

    fn mut_pool(&mut self) -> &mut ep_core::ep::EpPool<RedisAsync> {
        self.0.mut_pool()
    }
}

impl EpRouter for ElasticacheEp {
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

ep_core::impl_endpoint_lifecycle_spec!(ElasticacheEp, RedisAsync, RedisConfig, ElasticacheRequest, RedisMetadata, ElasticacheApi, RedisTx);

impl EP<RedisAsync, RedisConfig, ElasticacheRequest, RedisMetadata, ElasticacheApi, RedisTx> for ElasticacheEp {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::default()
    }

    fn validate_operation(&self, op: &dyn Operation<RedisAsync, ElasticacheApi, RedisTx>) -> ResultEP<()> {
        match op.kind() {
            ElasticacheApi::Redis => {
                if let Some(redis_op) = op.as_any().downcast_ref::<ElasticacheRedisOperation>() {
                    return policy::ensure_api_allowed(&redis_op.inner().kind());
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn transaction(
        &self,
        _endpoint_cache_uuid: &EndpointCacheUuid,
        outpound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let tx = match transaction.as_any().downcast_ref::<Transaction<ElasticacheRequest>>() {
            Some(tx) => tx,
            None => {
                let err = EpError::transaction("failed to downcast elasticache transaction");
                outpound.send(Err(err.clone())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
                return Err(err);
            }
        };

        let mut redis_requests = Vec::with_capacity(tx.0.len());
        for req in &tx.0 {
            if let Err(err) = self.validate_operation(req.operation()) {
                outpound.send(Err(err.clone())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
                return Err(err);
            }

            match req.as_redis_request() {
                Some(redis_req) => redis_requests.push(redis_req),
                None => {
                    let err = EpError::transaction("control-plane operations are not supported in elasticache transactions");
                    outpound.send(Err(err.clone())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
                    return Err(err);
                }
            }
        }

        let redis_tx = Transaction::<RedisRequest>(redis_requests);
        self.0.transaction(_endpoint_cache_uuid, outpound, inbound, &redis_tx, _settings, telemetry_wrapper).await
    }

    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        self.0.health_check(endpoint_cache_uuid, telemetry_wrapper).await
    }

    fn kind() -> EpKind {
        EpKind::Elasticache
    }

    async fn process_wire_protocol(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        receiver: &mut UnboundedReceiver<Vec<u8>>,
        sender: UnboundedSender<Vec<u8>>,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: &mut TelemetryWrapper,
        ctx: LogContext,
    ) {
        process_wire_protocol_with_bytes::<Self, ElasticacheRequest, ElasticacheApi, ElasticacheBytes>(
            self,
            endpoint_cache_uuid,
            receiver,
            sender,
            settings,
            interlay_cache_uuid,
            interlay_endpoints,
            telemetry_wrapper,
            ctx,
            "Elasticache",
        )
        .await;
    }
}
