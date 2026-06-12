use crate::api::lib::RedisApi;
use crate::metadata::RedisMetadata;
use crate::protocol::RedisBytes;
use crate::redis_like::RedisLikeEp;
use crate::request::RedisRequest;
use dashmap::DashMap;
use deadpool::managed::Object;
use eden_logger_internal::LogContext;
use endpoint_types::request::EpWireRequest;
use endpoint_types::{EP, EpTransaction};
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, ReqType, impl_endpoint};
use error::EpError;
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use format::{CacheUuid, OrganizationUuid};
use function_name::named;
use redis::{ConnectionInfo, RedisResult};
pub(crate) use redis_core::RedisAsync;
use redis_core::RedisClient;
pub(crate) use redis_core::RedisTx;
use redis_core::config::{MultiKeyExecution, RedisConfig};
use serde_json::Value;
use std::sync::Arc;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use telemetry::metric_event::{MetricEvent, RecordMetric};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Redis => Redis, RedisAsync);

pub struct RedisConnectionManager {
    connection_info: ConnectionInfo,
}

pub type RedisPinnedConnection = Object<redis_core::RedisConnectionManager>;

pub enum RedisMultiplexedResponseTarget {
    Shared(Arc<dyn redis_core::multiplex::DispatchResponseSink>),
    Static(&'static dyn redis_core::multiplex::DispatchResponseSink),
    StaticDiscard(&'static dyn redis_core::multiplex::DispatchResponseSink),
}

pub struct RedisMultiplexedDispatchWithPermit<'a> {
    pub endpoint_cache_uuid: &'a EndpointCacheUuid,
    pub bytes: RedisBytes,
    pub req_type: ReqType,
    pub command_count: usize,
    pub response_target: RedisMultiplexedResponseTarget,
    pub request_received_at: std::time::Instant,
    pub completion_permit: tokio::sync::OwnedSemaphorePermit,
}

impl RedisConnectionManager {
    pub fn new(connection_info: &ConnectionInfo) -> Self {
        Self { connection_info: connection_info.clone() }
    }

    pub fn get_connection(&self) -> RedisResult<redis::Connection> {
        redis::Client::open(self.connection_info.clone())?.get_connection()
    }
}
//
// impl r2d2::ManageConnection for RedisConnectionManager {
//     type Connection = redis::Connection;
//     type Error = RedisError;
//
//     fn connect(&self) -> Result<Self::Connection, Self::Error> {
//         self.get_connection()
//     }
//
//     fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
//         if conn.check_connection() {
//             Ok(())
//         } else {
//             Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
//         }
//     }
//
//     fn has_broken(&self, conn: &mut Self::Connection) -> bool {
//         !conn.is_open()
//     }
// }
//
// impl bb8::ManageConnection for RedisConnectionManager {
//     type Connection = MultiplexedConnection;
//     type Error = RedisError;
//
//     async fn connect(&self) -> Result<Self::Connection, Self::Error> {
//         redis::Client::open(self.connection_info.clone())?
//             .get_multiplexed_async_connection()
//             .await
//     }
//
//     async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
//         let pong: String = conn.ping().await?;;
//         match pong.as_str() {
//             "PONG" => Ok(()),
//             _ => Err((ErrorKind::ResponseError, "ping request").into()),
//         }
//     }
//
//     fn has_broken(&self, _: &mut Self::Connection) -> bool {
//         false
//     }
// }

ep_core::impl_endpoint_lifecycle_spec!(RedisEp, RedisAsync, RedisConfig, RedisRequest, RedisMetadata, RedisApi, RedisTx);

impl EP<RedisAsync, RedisConfig, RedisRequest, RedisMetadata, RedisApi, RedisTx> for RedisEp {
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
        outpound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));
        self.transaction_impl(_endpoint_cache_uuid, outpound, inbound, transaction, _settings, telemetry_wrapper, &mut span)
            .await
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));
        self.health_check_impl(endpoint_cache_uuid, telemetry_wrapper, &mut span).await
    }
    fn kind() -> EpKind {
        EpKind::Redis
    }

    /// Process Redis wire protocol requests in a continuous loop.
    ///
    /// This implementation:
    /// 1. Buffers incoming bytes from the client
    /// 2. Validates and parses RESP (Redis Serialization Protocol) frames
    /// 3. Routes each command through `tcp_read_bytes()` which applies migration logic
    /// 4. Sends responses back to the client
    ///
    /// Migration routing is handled transparently by `tcp_read_bytes()` based on
    /// the `TrafficRouting` configuration for this endpoint.
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
        self.process_wire_protocol_impl(
            endpoint_cache_uuid,
            receiver,
            sender,
            settings,
            interlay_cache_uuid,
            interlay_endpoints,
            telemetry_wrapper,
            ctx,
        )
        .await;
    }
}

impl RedisLikeEp for RedisEp {
    type Request = RedisRequest;
    type WireBytes = RedisBytes;

    const WIRE_LABEL: &'static str = "Redis";
}

impl RedisEp {
    /// Legacy processor fallback dispatch path. New Redis proxy
    /// connections should enter the direct lane-pool path in
    /// `eden_gateway`; this remains for interlay states whose migration,
    /// routing, policy, or audit behavior still requires the full
    /// processor.
    pub async fn multiplexed_raw_bytes_with_req_type(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<bytes::Bytes, EpError> {
        let endpoint_uuid = endpoint_cache_uuid.uuid();

        let mut span = telemetry_wrapper.client_tracer(Self::kind().span_raw_bytes());
        span.add_event("running multiplexed redis tcp_read_bytes", vec![FastSpanAttribute::new("endpoint", endpoint_uuid)]);

        let client = {
            let mut pool_span = telemetry_wrapper.start_client_span(Self::kind().span_pool_acquire());
            pool_span.add_simple_event("selecting multiplexed redis backend");

            match req_type {
                ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
                ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
            }
        };

        if settings.test() {
            Ok(bytes::Bytes::from_static(b"PASSED"))
        } else if matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            let timeout_duration = settings.max_timeout_duration();
            let network_result = {
                let mut network_span = telemetry_wrapper.start_client_span(Self::kind().span_send_raw_bytes());
                network_span.add_simple_event("sending bytes through redis multiplexer");

                match tokio::time::timeout(timeout_duration, client.send_raw_bytes_multiplexed(bytes.into_bytes())).await {
                    Ok(result) => {
                        match &result {
                            Ok(_) => network_span.add_simple_event("received multiplexed response from endpoint"),
                            Err(error) => {
                                network_span.add_event("error from endpoint", vec![FastSpanAttribute::new("error", error.to_string())])
                            }
                        }
                        result
                    }
                    Err(_) => Err(EpError::timeout(format!("Operation timed out after {} ms", timeout_duration.as_millis()))),
                }
            };

            if let Ok((_, network_latency_us)) = &network_result
                && let Some(org_uuid) = endpoint_cache_uuid.org()
            {
                let mut endpoint_uuid_buf = [0u8; 36];
                let endpoint_id = endpoint_uuid.as_hyphenated().encode_lower(&mut endpoint_uuid_buf);
                let org_uuid_label = org_uuid.eden_uuid::<OrganizationUuid>().to_string();
                telemetry_wrapper.record(MetricEvent::NetworkLatency {
                    org_uuid: org_uuid_label.as_str(),
                    endpoint_uuid: endpoint_id,
                    endpoint_kind: Self::kind().as_str(),
                    duration_us: *network_latency_us,
                });
            }

            network_result.map(|(response, _)| response)
        } else {
            let timeout_duration = settings.max_timeout_duration();
            let network_result = {
                let mut network_span = telemetry_wrapper.start_client_span(Self::kind().span_send_raw_bytes());
                network_span.add_simple_event("sending bytes through redis multi-key policy path");

                match tokio::time::timeout(timeout_duration, bytes.send_raw_bytes(client)).await {
                    Ok(result) => {
                        match &result {
                            Ok(_) => network_span.add_simple_event("received policy-aware response from endpoint"),
                            Err(error) => {
                                network_span.add_event("error from endpoint", vec![FastSpanAttribute::new("error", error.to_string())])
                            }
                        }
                        result
                    }
                    Err(_) => Err(EpError::timeout(format!("Operation timed out after {} ms", timeout_duration.as_millis()))),
                }
            };

            if let Ok((_, network_latency_us)) = &network_result
                && let Some(org_uuid) = endpoint_cache_uuid.org()
            {
                let mut endpoint_uuid_buf = [0u8; 36];
                let endpoint_id = endpoint_uuid.as_hyphenated().encode_lower(&mut endpoint_uuid_buf);
                let org_uuid_label = org_uuid.eden_uuid::<OrganizationUuid>().to_string();
                telemetry_wrapper.record(MetricEvent::NetworkLatency {
                    org_uuid: org_uuid_label.as_str(),
                    endpoint_uuid: endpoint_id,
                    endpoint_kind: Self::kind().as_str(),
                    duration_us: *network_latency_us,
                });
            }

            network_result.map(|(response, _)| response)
        }
    }

    /// Legacy processor fallback sink-path equivalent of
    /// `multiplexed_raw_bytes_with_req_type`: instead of awaiting the
    /// response, registers the sink as the destination so the
    /// multiplexer's worker reader delivers directly. Returns when the
    /// request has been enqueued to a worker.
    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_multiplexed_raw_bytes_with_req_type_to_sink(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        sink: std::sync::Arc<dyn redis_core::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
        settings: EdenSettings,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let client = match req_type {
            ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
            ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
        };
        if matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            return client.dispatch_raw_bytes_multiplexed_to_sink(bytes.into_bytes(), sink, request_received_at).await;
        }

        let command_count = RedisClient::count_pipeline_commands(bytes.bytes())?;
        let timeout_duration = settings.max_timeout_duration();
        let result = match tokio::time::timeout(timeout_duration, bytes.send_raw_bytes(client)).await {
            Ok(result) => result,
            Err(_) => Err(EpError::timeout(format!("Operation timed out after {} ms", timeout_duration.as_millis()))),
        };

        match result {
            Ok((response, network_latency_us)) => {
                sink.deliver(Ok(response), command_count, request_received_at, network_latency_us);
                Ok(())
            }
            Err(error) => {
                sink.deliver(Err(error.clone()), command_count, request_received_at, 0);
                Err(error)
            }
        }
    }

    /// Best-effort sink dispatch for side paths such as mirroring. Returns
    /// once the command has been accepted by a direct multiplexer worker and
    /// never waits for mirror queue capacity. The sink is called only for
    /// accepted commands so the worker can drain the response.
    pub async fn try_dispatch_multiplexed_raw_bytes_with_req_type_to_sink(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        sink: std::sync::Arc<dyn redis_core::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
    ) -> Result<(), EpError> {
        let command_count = RedisClient::count_pipeline_commands(bytes.bytes())?;
        self.try_dispatch_multiplexed_raw_bytes_with_req_type_to_sink_with_command_count(
            endpoint_cache_uuid,
            bytes,
            req_type,
            command_count,
            sink,
            request_received_at,
        )
        .await
    }

    /// Best-effort sink dispatch when the caller already parsed the command count.
    pub async fn try_dispatch_multiplexed_raw_bytes_with_req_type_to_sink_with_command_count(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        command_count: usize,
        sink: std::sync::Arc<dyn redis_core::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
    ) -> Result<(), EpError> {
        let client = match req_type {
            ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
            ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
        };
        if matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            return client.try_dispatch_raw_bytes_multiplexed_to_sink_with_command_count(
                bytes.into_bytes(),
                command_count,
                sink,
                request_received_at,
            );
        }

        Err(EpError::request("redis mirror fast path requires native multiplexing"))
    }

    /// Best-effort sink dispatch with a permit released on response drain/failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_dispatch_multiplexed_raw_bytes_with_req_type_to_sink_with_command_count_and_completion_permit(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        command_count: usize,
        sink: std::sync::Arc<dyn redis_core::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
        completion_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), EpError> {
        let client = match req_type {
            ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
            ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
        };
        if matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            return client.try_dispatch_raw_bytes_multiplexed_to_sink_with_command_count_and_completion_permit(
                bytes.into_bytes(),
                command_count,
                sink,
                request_received_at,
                completion_permit,
            );
        }

        Err(EpError::request("redis mirror fast path requires native multiplexing"))
    }

    /// Best-effort mirror dispatch to a process-lifetime sink. The caller
    /// must already know the command count so the mirror path does not rescan
    /// RESP bytes.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_dispatch_multiplexed_raw_bytes_with_req_type_to_static_sink_with_command_count_and_completion_permit(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        command_count: usize,
        sink: &'static dyn redis_core::multiplex::DispatchResponseSink,
        request_received_at: std::time::Instant,
        completion_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), EpError> {
        let client = match req_type {
            ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
            ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
        };
        if matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            return client.try_dispatch_raw_bytes_multiplexed_to_static_sink_with_command_count_and_completion_permit(
                bytes.into_bytes(),
                command_count,
                sink,
                request_received_at,
                completion_permit,
            );
        }

        Err(EpError::request("redis mirror fast path requires native multiplexing"))
    }

    /// Best-effort mirror dispatch to a process-lifetime sink that only needs
    /// drain/latency/error accounting. Successful mirror responses are read and
    /// discarded by the worker instead of being materialized into `Bytes`.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_dispatch_multiplexed_raw_bytes_with_req_type_to_static_discard_sink_with_command_count_and_completion_permit(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: RedisBytes,
        req_type: ReqType,
        command_count: usize,
        sink: &'static dyn redis_core::multiplex::DispatchResponseSink,
        request_received_at: std::time::Instant,
        completion_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), EpError> {
        let client = match req_type {
            ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
            ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
        };
        if matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            return client.try_dispatch_raw_bytes_multiplexed_to_static_discard_sink_with_command_count_and_completion_permit(
                bytes.into_bytes(),
                command_count,
                sink,
                request_received_at,
                completion_permit,
            );
        }

        Err(EpError::request("redis mirror fast path requires native multiplexing"))
    }

    /// Best-effort sink dispatch with a permit released on response
    /// drain/failure. The caller must already know the command count so the
    /// mirror path does not rescan RESP bytes.
    pub async fn try_dispatch_multiplexed_raw_bytes_with_completion_permit(
        &self,
        dispatch: RedisMultiplexedDispatchWithPermit<'_>,
    ) -> Result<(), EpError> {
        let RedisMultiplexedDispatchWithPermit {
            endpoint_cache_uuid,
            bytes,
            req_type,
            command_count,
            response_target,
            request_received_at,
            completion_permit,
        } = dispatch;

        let client = match req_type {
            ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
            ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
        };

        if !matches!(client.multi_key_execution(), MultiKeyExecution::Native) {
            return Err(EpError::request("redis mirror fast path requires native multiplexing"));
        }

        match response_target {
            RedisMultiplexedResponseTarget::Shared(sink) => client
                .try_dispatch_raw_bytes_multiplexed_to_sink_with_command_count_and_completion_permit(
                    bytes.into_bytes(),
                    command_count,
                    sink,
                    request_received_at,
                    completion_permit,
                ),
            RedisMultiplexedResponseTarget::Static(sink) => client
                .try_dispatch_raw_bytes_multiplexed_to_static_sink_with_command_count_and_completion_permit(
                    bytes.into_bytes(),
                    command_count,
                    sink,
                    request_received_at,
                    completion_permit,
                ),
            RedisMultiplexedResponseTarget::StaticDiscard(sink) => client
                .try_dispatch_raw_bytes_multiplexed_to_static_discard_sink_with_command_count_and_completion_permit(
                    bytes.into_bytes(),
                    command_count,
                    sink,
                    request_received_at,
                    completion_permit,
                ),
        }
    }

    /// Get a pinned write connection so multiple Redis commands share state (e.g., WATCH/MULTI/EXEC).
    pub async fn pinned_write_connection(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<RedisPinnedConnection, EpError> {
        let pool = self.pool().write_conn_async(endpoint_cache_uuid).await?;

        pool.get().await.map_err(EpError::parse_redis_error)
    }
}
