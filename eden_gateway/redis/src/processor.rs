//! Redis-specific wire protocol processor.
//!
//! This module contains the Redis implementation of the `DatabaseProtocolProcessor` trait,
//! handling RESP protocol parsing, command classification, and gateway routing.

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use database::cache::CacheFunctions;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::InterlayUuid;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::request::DEFAULT_MAX_TIMEOUT;
use eden_core::telemetry::metrics::ProxyBatchRecord;
use eden_core::telemetry::{FastSpanAttribute, MetricEvent, RecordMetric, TelemetryWrapper};
use eden_gateway_core::response::{GatewayResponsePolicySpec, GatewayResponseProfile};
use eden_gateway_core::traits::{BytesQueueSender, DatabaseProtocolProcessor, ProxyRequestChunk};
use eden_logger_internal::{LogAudience, LogContext, log_error, log_info, log_trace, log_warn};
use endpoint_schema::endpoint::EndpointSchema;
use endpoints::endpoint::EP;
use endpoints::endpoint::ep_redis::ep::RedisEp;
use ep_core::ReqType;
use ep_core::database::schema::interlay::{InterlaySignal, InterlayState};
use ep_core::database::schema::routing::{HashAlgorithm, HashConfig, HashTagDelimiter, RoutingResolver};
use ep_core::settings::EdenSettings;
use redis_core::{RedisClient, RedisConfig, RedisConnection, RedisCredentials, RedisTarget};
#[cfg(test)]
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;

pub mod cluster;
mod comparison;
mod dispatch;
mod metrics;
mod pinning;
mod request;
mod routing;
mod runtime;
mod streaming;
mod wire;

use cluster::{ClusterProcessor, ClusterSupport};
pub(crate) use comparison::RedisResponseComparison;
pub(crate) use dispatch::{CommandDispatchPath, PreDispatchHandling, RedisDispatch};
pub(crate) use metrics::RedisPipelineMetrics;
use pinning::{PinAction, PinnedTransactionTracker};
pub(crate) use request::RedisRequestMetadata;
pub(crate) use routing::RoutingRuntime;
use routing::RoutingState;
pub use streaming::RedisStreamingProcessor;
pub(crate) use wire::RedisWire;

use super::replication::REPLICATION_MANAGERS;
use super::response::{RedisGatewayResponseProfile, RedisResponseErrorScanner};
use super::{RedisIngressBatch, policy_enforcement_mode, policy_override_from_guard};
#[cfg(test)]
use endpoints::endpoint::ep_redis::api::key::RedisKey;
use endpoints::endpoint::ep_redis::api::{RedisApi, RedisJsonValue};
use endpoints::endpoint::ep_redis::protocol::RedisCommandArgs;
use endpoints::endpoint::ep_redis::protocol::RedisProtocol;
use endpoints::endpoint::ep_redis::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use endpoints::endpoint::ep_redis::protocol::encoder::EncoderRespFrame;
use endpoints::endpoint::protocol::EpProtocol;
use ep_core::pool::PinnedGuard;
use once_cell::sync::Lazy;

static WIRE_CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);
const MAX_REQUEST_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const MAX_RESPONSE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const MISSING_UPSTREAM_RESPONSE_MESSAGE: &str = "proxy handler produced no client response";

#[derive(Debug)]
pub(super) struct RedisWireBatch {
    bytes: Bytes,
    received_at: Instant,
    queue_wait_us: u64,
}

impl RedisWireBatch {
    pub(super) fn from_proxy_chunk(chunk: ProxyRequestChunk) -> Self {
        let received_at = chunk.received_at();
        let queue_wait_us = chunk.queue_wait_us();
        Self { bytes: chunk.into_bytes(), received_at, queue_wait_us }
    }

    pub(super) fn from_ingress_batch(batch: RedisIngressBatch) -> Self {
        let received_at = batch.received_at();
        let queue_wait_us = batch.queue_wait_us();
        Self {
            bytes: batch.batch_bytes().clone(),
            received_at,
            queue_wait_us,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(super) fn received_at(&self) -> Instant {
        self.received_at
    }

    pub(super) fn queue_wait_us(&self) -> u64 {
        self.queue_wait_us
    }

    pub(super) fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

#[derive(Debug)]
pub(super) struct CommandMeta {
    parsed: RedisCommandArgs,
    command_bytes: Bytes,
    policy_override: Option<Bytes>,
    abort_after_response: bool,
}

const UNSUPPORTED_PUBSUB_MESSAGE: &str = "pub/sub commands are not supported through Eden proxy";
const UNSUPPORTED_AUTH_MESSAGE: &str = "AUTH is not supported through Eden proxy";
const UNSUPPORTED_SELECT_MESSAGE: &str = "SELECT is not supported through Eden proxy";

/// Redis-specific implementation of the wire protocol processor.
#[derive(Clone)]
pub struct RedisProtocolProcessor {
    ep: RedisEp,
    database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
}

impl RedisProtocolProcessor {
    /// Creates a new Redis protocol processor with the given endpoint.
    pub fn new(ep: RedisEp, database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>) -> Self {
        Self { ep, database_manager }
    }
}

impl GatewayResponseProfile for RedisProtocolProcessor {
    type Observer = RedisResponseErrorScanner;

    fn response_policy_spec(&self) -> GatewayResponsePolicySpec {
        GatewayResponseProfile::response_policy_spec(&RedisGatewayResponseProfile)
    }
}

impl DatabaseProtocolProcessor for RedisProtocolProcessor {
    fn process(
        &self,
        receiver: UnboundedReceiver<ProxyRequestChunk>,
        sender: BytesQueueSender,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: std::net::SocketAddr,
        listener_id: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let ep = self.ep.clone();
        let database_manager = self.database_manager.clone();

        Box::pin(async move {
            let (bytes_tx, bytes_rx) = tokio::sync::mpsc::unbounded_channel::<RedisWireBatch>();
            let forward_task = eden_gateway_core::runtime::spawn_on_current_runtime(async move {
                RedisStreamingProcessor::forward_proxy_chunks_as_batches(receiver, bytes_tx).await;
            });

            RedisStreamingProcessor::run_bytes(
                ep,
                bytes_rx,
                database_manager,
                sender,
                settings,
                interlay_cache_uuid,
                interlay_endpoints,
                telemetry_wrapper,
                ctx,
                client_addr,
                listener_id,
            )
            .await;

            let _ = forward_task.await;
        })
    }
}

#[cfg(test)]
mod tests;
