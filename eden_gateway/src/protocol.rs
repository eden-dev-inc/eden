use super::*;
use eden_logger_internal::LogContextEdenExt;

// EchoProtocol implements ProtocolRW that provides Reader and Writer streams
// This protocol reads the Reader stream and writes to the writer stream
#[derive(Default)]
pub struct ProxyProtocol {}

impl ProtocolRW for ProxyProtocol {
    type Reader = ReadHalf<DuplexStream>;
    type Writer = WriteHalf<DuplexStream>;

    #[named]
    fn split(
        &self,
        engine_service: Arc<MyEngineService>,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>, //TODO, make sure this is tracked
        organization_cache_uuid: OrganizationCacheUuid,
        eden_settings: EdenSettings,
        mut telemetry_wrapper: TelemetryWrapper,
        client_addr: SocketAddr,
    ) -> (Self::Reader, Self::Writer) {
        // create an in-memory duplex pair and echo whatever comes in.
        // `a` is the side used by the proxy. `b` is used by our task.
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (a_r, a_w) = tokio::io::split(a);
        let (b_r, b_w) = tokio::io::split(b);

        let organization_uuid = organization_cache_uuid.eden_uuid::<eden_core::format::OrganizationUuid>();
        telemetry_wrapper.set_org_uuid(organization_uuid.clone());
        let engine_service_clone = engine_service.clone();
        let telemetry_wrapper_clone = telemetry_wrapper.clone();
        let bridge_metrics = telemetry_wrapper.metrics().clone();
        let bridge_metric_labels = ProxyBridgeMetricLabels::from_interlay_state(&interlay_cache_uuid, &interlay_endpoints);
        // Create channels to the protocol processor and return responses from it.
        // Create base context once - will use with_fresh_span() in loop iterations
        let base_ctx = ctx_with_trace!()
            .with_feature("gateway")
            .with_organization_uuid(organization_uuid.to_string())
            .with_additional("interlay_cache_uuid", interlay_cache_uuid.to_string());
        eden_gateway_core::runtime::spawn_on_current_runtime(async move {
            let organization_cache_uuid_clone = organization_cache_uuid.clone();

            // Use unbounded channels to avoid head-of-line blocking
            // The serial command processing architecture requires non-blocking sends
            let (sender, receiver) = unbounded_channel::<ProxyRequestChunk>();
            let (response_sender_raw, response_receiver) = unbounded_channel::<QueuedBytes>();
            let config = gateway_runtime_config();
            let response_sender =
                BytesQueueSender::new(response_sender_raw, config.max_bridge_pending_messages, config.max_bridge_pending_bytes);
            let processor = processor::GatewayProcessor::spawn(
                receiver,
                database_manager,
                response_sender.clone(),
                engine_service_clone,
                interlay_cache_uuid,
                interlay_endpoints,
                organization_cache_uuid_clone.clone(),
                eden_settings,
                telemetry_wrapper_clone,
                client_addr,
                String::new(),
            );

            let bridge_result = run_proxy_bridge_loop(
                base_ctx.clone(),
                b_r,
                b_w,
                ProxyBridgeQueues {
                    sender,
                    response_sender,
                    response_receiver,
                    observer: BridgeQueueObserver::disabled(),
                },
                ProxyBridgeTelemetry::new(bridge_metrics, bridge_metric_labels),
            )
            .await;
            _ = join!(processor);
            if let Err(_error) = bridge_result {
                log_debug!(
                    base_ctx.clone(),
                    "Gateway bridge closed with error",
                    audience = LogAudience::Internal,
                    error = _error.to_string()
                );
            }
            log_debug!(base_ctx, "Connection closed", audience = LogAudience::Internal);
        });
        (a_r, a_w)
    }
}

// ProtocolRW implements the transform of the stream from Reader to Writer.
// It is the key implementation of the protocol as it e.g. decodes the commands,
// sent to the database that are arriving in Reader stream, executes appropriate
// APIs and writes the results into the Writer stream.
pub trait ProtocolRW: Send + Sync + 'static {
    type Reader: AsyncRead + Unpin + Send + 'static;
    type Writer: AsyncWrite + Unpin + Send + 'static;

    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    fn split(
        &self,
        engine_service: Arc<MyEngineService>,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        organization_cache_uuid: OrganizationCacheUuid,
        eden_settings: EdenSettings,
        telemetry_wrapper: TelemetryWrapper,
        client_addr: SocketAddr,
    ) -> (Self::Reader, Self::Writer);
}
