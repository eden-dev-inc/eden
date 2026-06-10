//! Redis unified processor runtime.

use super::*;
use crate::response::RedisGatewayResponseProfile;
use eden_core::telemetry::metrics::ProxyMirrorSeries;
use eden_gateway_core::response::{GatewayMirrorResponseMode, GatewayResponseProfile};
use eden_logger_internal::log_debug;
use std::cell::RefCell;
use std::collections::HashMap;

const SLOW_GATEWAY_OP_LOG_THRESHOLD_US: u64 = 10_000;

struct RedisMirrorDiscardSink {
    mirror_series: ProxyMirrorSeries,
}

impl redis_core::multiplex::DispatchResponseSink for RedisMirrorDiscardSink {
    fn deliver(
        &self,
        response: Result<Bytes, EpError>,
        _command_count: usize,
        _request_received_at: std::time::Instant,
        network_latency_us: u64,
    ) {
        self.mirror_series.record_latency(network_latency_us);

        if response.is_err() {
            self.mirror_series.record_upstream_error();
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct RedisMirrorSeriesKey {
    metrics_id: usize,
    organization_uuid: Arc<str>,
    interlay_id: Arc<str>,
    primary_endpoint_uuid: Arc<str>,
    mirror_endpoint_uuid: Arc<str>,
    endpoint_kind: &'static str,
    req_type_label: &'static str,
}

struct RedisMirrorDispatchTarget {
    sink: RedisMirrorDiscardSink,
}

impl RedisMirrorDispatchTarget {
    #[inline]
    fn record_request(&self) {
        self.sink.mirror_series.record_request();
    }

    #[inline]
    fn sink(&'static self) -> &'static dyn redis_core::multiplex::DispatchResponseSink {
        &self.sink
    }
}

thread_local! {
    static REDIS_MIRROR_TARGETS: RefCell<HashMap<RedisMirrorSeriesKey, &'static RedisMirrorDispatchTarget>> = RefCell::default();
}

pub(super) struct RedisUnifiedProcessor;

impl RedisUnifiedProcessor {
    fn req_type_label(req_type: &ReqType) -> &'static str {
        match req_type {
            ReqType::Read => "read",
            ReqType::Write => "write",
        }
    }

    fn sample_allows_mirror(sample_ratio: f64) -> bool {
        if sample_ratio >= 1.0 {
            return true;
        }
        sample_ratio > 0.0 && rand::random::<f64>() <= sample_ratio
    }

    #[allow(clippy::too_many_arguments)]
    fn record_mirror_skip(
        telemetry_wrapper: &TelemetryWrapper,
        organization_uuid: &str,
        interlay_id: &str,
        primary_endpoint_uuid: &str,
        mirror_endpoint_uuid: &str,
        endpoint_kind: &str,
        req_type: &ReqType,
        reason: &str,
    ) {
        let req_type = Self::req_type_label(req_type);
        let labels = [
            ("org_uuid", organization_uuid),
            ("interlay_uuid", interlay_id),
            ("primary_endpoint_uuid", primary_endpoint_uuid),
            ("mirror_endpoint_uuid", mirror_endpoint_uuid),
            ("endpoint_kind", endpoint_kind),
            ("req_type", req_type),
            ("reason", reason),
        ];
        telemetry_wrapper.metrics().proxy().record_mirror_skip(&labels);
    }

    #[allow(clippy::too_many_arguments)]
    fn mirror_dispatch_target(
        telemetry_wrapper: &TelemetryWrapper,
        organization_uuid: &Arc<str>,
        interlay_id: &Arc<str>,
        primary_endpoint_uuid: &Arc<str>,
        mirror_endpoint_uuid: &Arc<str>,
        endpoint_kind: &'static str,
        req_type_label: &'static str,
    ) -> &'static RedisMirrorDispatchTarget {
        let key = RedisMirrorSeriesKey {
            metrics_id: Arc::as_ptr(telemetry_wrapper.metrics()) as usize,
            organization_uuid: organization_uuid.clone(),
            interlay_id: interlay_id.clone(),
            primary_endpoint_uuid: primary_endpoint_uuid.clone(),
            mirror_endpoint_uuid: mirror_endpoint_uuid.clone(),
            endpoint_kind,
            req_type_label,
        };

        REDIS_MIRROR_TARGETS.with(|targets| {
            let mut targets = targets.borrow_mut();
            if let Some(existing) = targets.get(&key) {
                return *existing;
            }

            let labels = [
                ("org_uuid", organization_uuid.as_ref()),
                ("interlay_uuid", interlay_id.as_ref()),
                ("primary_endpoint_uuid", primary_endpoint_uuid.as_ref()),
                ("mirror_endpoint_uuid", mirror_endpoint_uuid.as_ref()),
                ("endpoint_kind", endpoint_kind),
                ("req_type", req_type_label),
            ];
            let upstream_error_labels = [
                ("org_uuid", organization_uuid.as_ref()),
                ("interlay_uuid", interlay_id.as_ref()),
                ("primary_endpoint_uuid", primary_endpoint_uuid.as_ref()),
                ("mirror_endpoint_uuid", mirror_endpoint_uuid.as_ref()),
                ("endpoint_kind", endpoint_kind),
                ("req_type", req_type_label),
                ("reason", "upstream_error"),
            ];
            let series = telemetry_wrapper.metrics().proxy().mirror_series(&labels, &upstream_error_labels);
            let target = Box::leak(Box::new(RedisMirrorDispatchTarget { sink: RedisMirrorDiscardSink { mirror_series: series } }));
            targets.insert(key, target);
            target
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Best-effort mirror fanout.
    ///
    /// Mirroring is intentionally separate from Redis replication:
    /// replication remains the PSYNC/AOF-style `ReplicationManager` path,
    /// while mirroring only tries to enqueue the command to configured mirror
    /// endpoints and does not wait for or compare mirror command success.
    async fn mirror_redis_command(
        ep: &RedisEp,
        state: &InterlayState,
        organization_uuid: &Arc<str>,
        interlay_id: &Arc<str>,
        req_type: ReqType,
        request_bytes: Option<&Bytes>,
        command_count: usize,
        telemetry_wrapper: &TelemetryWrapper,
        skip_reason: Option<&str>,
    ) {
        use endpoints::endpoint::ep_redis::ep::{RedisMultiplexedDispatchWithPermit, RedisMultiplexedResponseTarget};
        use endpoints::endpoint::ep_redis::protocol::RedisBytes;

        let mirror = state.mirror();
        if !mirror.enabled() {
            return;
        }

        let should_mirror_req_type = match &req_type {
            ReqType::Read => mirror.mirror_reads(),
            ReqType::Write => mirror.mirror_writes(),
        };
        if !should_mirror_req_type {
            return;
        }

        let primary_endpoint_uuid = state.endpoint_uuid_label_arc();
        let endpoint_kind = state.endpoint_kind().as_str();
        let req_type_label = Self::req_type_label(&req_type);

        if let Some(reason) = skip_reason {
            for mirror_target in state.mirror_targets() {
                Self::record_mirror_skip(
                    telemetry_wrapper,
                    organization_uuid.as_ref(),
                    interlay_id.as_ref(),
                    primary_endpoint_uuid.as_ref(),
                    mirror_target.endpoint_uuid_label(),
                    endpoint_kind,
                    &req_type,
                    reason,
                );
            }
            return;
        }

        if !Self::sample_allows_mirror(mirror.sample_ratio()) {
            for mirror_target in state.mirror_targets() {
                Self::record_mirror_skip(
                    telemetry_wrapper,
                    organization_uuid.as_ref(),
                    interlay_id.as_ref(),
                    primary_endpoint_uuid.as_ref(),
                    mirror_target.endpoint_uuid_label(),
                    endpoint_kind,
                    &req_type,
                    "sampled_out",
                );
            }
            return;
        }

        let Some(request_bytes) = request_bytes else {
            return;
        };

        for mirror_target in state.mirror_targets() {
            let mirror_endpoint_uuid = mirror_target.endpoint_uuid_label_arc();
            let Ok(permit) = mirror_target.try_acquire_owned() else {
                Self::record_mirror_skip(
                    telemetry_wrapper,
                    organization_uuid.as_ref(),
                    interlay_id.as_ref(),
                    primary_endpoint_uuid.as_ref(),
                    mirror_endpoint_uuid.as_ref(),
                    endpoint_kind,
                    &req_type,
                    "max_in_flight",
                );
                continue;
            };

            let mirror_dispatch_target = Self::mirror_dispatch_target(
                telemetry_wrapper,
                organization_uuid,
                interlay_id,
                &primary_endpoint_uuid,
                &mirror_endpoint_uuid,
                endpoint_kind,
                req_type_label,
            );
            mirror_dispatch_target.record_request();

            let dispatch_result = match RedisGatewayResponseProfile.response_policy_spec().mirror_response_mode() {
                Some(GatewayMirrorResponseMode::DrainOnly) => {
                    ep.try_dispatch_multiplexed_raw_bytes_with_completion_permit(RedisMultiplexedDispatchWithPermit {
                        endpoint_cache_uuid: mirror_target.endpoint_cache_uuid(),
                        bytes: RedisBytes::from(request_bytes.clone()),
                        req_type: req_type.clone(),
                        command_count,
                        response_target: RedisMultiplexedResponseTarget::StaticDiscard(mirror_dispatch_target.sink()),
                        request_received_at: std::time::Instant::now(),
                        completion_permit: permit,
                    })
                    .await
                }
                Some(GatewayMirrorResponseMode::CompareResponse) => {
                    Self::record_mirror_skip(
                        telemetry_wrapper,
                        organization_uuid.as_ref(),
                        interlay_id.as_ref(),
                        primary_endpoint_uuid.as_ref(),
                        mirror_endpoint_uuid.as_ref(),
                        endpoint_kind,
                        &req_type,
                        "compare_response_unsupported",
                    );
                    continue;
                }
                None => {
                    Self::record_mirror_skip(
                        telemetry_wrapper,
                        organization_uuid.as_ref(),
                        interlay_id.as_ref(),
                        primary_endpoint_uuid.as_ref(),
                        mirror_endpoint_uuid.as_ref(),
                        endpoint_kind,
                        &req_type,
                        "mirror_unsupported",
                    );
                    continue;
                }
            };

            if dispatch_result.is_err() {
                Self::record_mirror_skip(
                    telemetry_wrapper,
                    organization_uuid.as_ref(),
                    interlay_id.as_ref(),
                    primary_endpoint_uuid.as_ref(),
                    mirror_endpoint_uuid.as_ref(),
                    endpoint_kind,
                    &req_type,
                    "dispatch_unavailable",
                );
            }
        }
    }

    #[inline]
    fn mirror_accepts_req_type(state: &InterlayState, req_type: &ReqType) -> bool {
        let mirror = state.mirror();
        if !mirror.enabled() {
            return false;
        }

        match req_type {
            ReqType::Read => mirror.mirror_reads(),
            ReqType::Write => mirror.mirror_writes(),
        }
    }

    /// Best-effort cleanup of a pinned connection on disconnect.
    ///
    /// Sends DISCARD (if in a MULTI block) or UNWATCH (if watching) to avoid returning
    /// a connection with dirty state to the pool. On failure, poisons the connection
    /// so it is not reused (following the pattern in `handler.rs`).
    async fn cleanup_pinned_conn(
        guard: &mut PinnedGuard<redis_core::RedisConnectionManager>,
        watching: bool,
        in_multi: bool,
        ctx: &LogContext,
    ) {
        use endpoints::endpoint::ep_redis::protocol::RedisBytes;

        let Some(pinned) = guard.as_deref_mut() else {
            return;
        };

        let cleanup_cmd: &[u8] = if in_multi {
            b"*1\r\n$7\r\nDISCARD\r\n"
        } else if watching {
            b"*1\r\n$7\r\nUNWATCH\r\n"
        } else {
            guard.release();
            return;
        };

        if let Err(err) = RedisBytes::from(Bytes::from_static(cleanup_cmd)).send_raw_bytes_on_conn_no_reconnect(pinned).await {
            log_warn!(
                ctx.clone(),
                "Pinned connection cleanup failed, poisoning connection",
                audience = LogAudience::Internal,
                error = err.to_string()
            );
            guard.poison();
        } else {
            guard.release();
        }
    }

    /// Unified Redis wire protocol processor.
    ///
    /// This single function checks the current state from `interlay_endpoints`
    /// on each command batch so endpoint routing updates can be observed mid-connection.
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn process(
        ep: RedisEp,
        mut receiver: UnboundedReceiver<RedisWireBatch>,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        sender: BytesQueueSender,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        mut telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: std::net::SocketAddr,
        listener_id: String,
        _connection_id: u64,
    ) {
        use bytes::BytesMut;
        use eden_core::format::{
            EdenUuid, EndpointUuid as FormatEndpointUuid, InterlayUuid as FormatInterlayUuid, OrganizationUuid as FormatOrganizationUuid,
        };
        use endpoints::endpoint::ep_redis::api::RedisJsonValue;
        use endpoints::endpoint::ep_redis::protocol::{RedisBytes, RedisProtocol};
        use endpoints::endpoint::protocol::EpProtocol;
        let mut buffer = BytesMut::with_capacity(16 * 1024);
        let mut response_buffer = BytesMut::with_capacity(16 * 1024);

        // Cache the interlay_id string to avoid repeated allocations in telemetry
        let interlay_id_str = interlay_cache_uuid.uuid().to_string();
        let interlay_id_label: Arc<str> = Arc::from(interlay_id_str.as_str());

        // Get initial routing state
        let initial_state = match interlay_endpoints.get(&interlay_cache_uuid) {
            Some(state) => state.clone(),
            None => {
                log_error!(
                    ctx,
                    "Interlay not found in cache, closing connection",
                    audience = LogAudience::Internal,
                    interlay_uuid = &interlay_id_str
                );
                return;
            }
        };

        if ClusterSupport::supports_virtual_cluster_proxy(&initial_state, &listener_id) {
            let (bytes_tx, bytes_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
            let cluster_forward_task = eden_gateway_core::runtime::spawn_on_current_runtime(async move {
                while let Some(batch) = receiver.recv().await {
                    if bytes_tx.send(batch.into_bytes()).is_err() {
                        return;
                    }
                }
            });
            ClusterProcessor::process_virtualized(
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
            let _ = cluster_forward_task.await;
            return;
        }

        let org = initial_state.endpoint_uuid().org();
        let mut routing_state = match RoutingState::from_interlay_state(&initial_state, org.as_ref()) {
            Ok(rs) => rs,
            Err(e) => {
                log_error!(
                    ctx,
                    "Invalid routing configuration, closing connection",
                    audience = LogAudience::Internal,
                    error = e.to_string()
                );
                return;
            }
        };
        let mut routing_state_version = initial_state.version();

        // Subscribe to signals for gateway updates
        let mut signal_rx = interlay_endpoints.get(&interlay_cache_uuid).and_then(|state| state.subscribe_signals());

        log_info!(
            ctx.clone(),
            "Processor started with unified routing",
            audience = LogAudience::Internal,
            endpoint = routing_state.resolver.primary().to_string(),
            has_signal_rx = signal_rx.is_some()
        );

        let policy_mode = policy_enforcement_mode();
        let Some(organization_uuid) = routing_state.resolver.primary().org().map(|org| org.eden_uuid::<FormatOrganizationUuid>()) else {
            log_warn!(
                ctx.clone(),
                "Redis proxy routing endpoint has no organization UUID",
                audience = LogAudience::Internal,
                endpoint = routing_state.resolver.primary().to_string()
            );
            return;
        };
        let interlay_uuid = interlay_cache_uuid.eden_uuid::<FormatInterlayUuid>();
        let organization_uuid_string = organization_uuid.uuid().to_string();
        let organization_uuid_label: Arc<str> = Arc::from(organization_uuid_string.as_str());

        // Pinned connection state for WATCH/MULTI/EXEC
        let mut pin_tracker = PinnedTransactionTracker::new();
        let mut pinned_conn: PinnedGuard<redis_core::RedisConnectionManager> = PinnedGuard::empty();

        // Cache ProxySeries handles for zero-allocation metric recording.
        let cached_endpoint_uuid = routing_state.resolver.primary().eden_uuid::<FormatEndpointUuid>();
        let cached_endpoint_uuid_label = cached_endpoint_uuid.uuid().to_string();
        let proxy_series =
            telemetry_wrapper
                .metrics()
                .proxy()
                .series_for_organization(&organization_uuid, &interlay_uuid, &cached_endpoint_uuid, "redis");
        let bridge_series = telemetry_wrapper.metrics().proxy().bridge_series(&[
            ("org_uuid", organization_uuid_string.as_str()),
            ("interlay_uuid", interlay_id_label.as_ref()),
            ("endpoint_uuid", cached_endpoint_uuid_label.as_str()),
            ("endpoint_kind", "redis"),
        ]);

        // Store base context for creating fresh spans per iteration
        let base_ctx = ctx;

        // Diagnostic counters for tracking command lifecycle through the processor.
        let mut diag_chunks_received: u64 = 0;
        let mut diag_bytes_received: u64 = 0;
        let mut diag_batches_processed: u64 = 0;
        let mut diag_responses_sent: u64 = 0;

        loop {
            // Create fresh span for each loop iteration to avoid nested traces
            let ctx = base_ctx.clone().with_fresh_span();

            // Use select! to handle both data and signals
            let data = if let Some(ref mut rx) = signal_rx {
                tokio::select! {
                    data = receiver.recv() => data,
                    signal = rx.recv() => {
                        match signal {
                            Ok(InterlaySignal::MirrorUpdate) => {
                                log_debug!(
                                    ctx.clone(),
                                    "Mirror update signal received",
                                    audience = LogAudience::Internal
                                );
                                continue;
                            }
                            Ok(InterlaySignal::Shutdown) => {
                                log_info!(
                                    ctx.clone(),
                                    "Shutdown signal received - closing connection",
                                    audience = LogAudience::Internal
                                );
                                Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx).await;
                                return;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid)
                                    && state.shutdown_requested()
                                {
                                    Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx)
                                        .await;
                                    return;
                                }
                                let _ = RoutingRuntime::refresh_from_cache_if_changed(
                                    &mut routing_state,
                                    &mut routing_state_version,
                                    &interlay_cache_uuid,
                                    interlay_endpoints.as_ref(),
                                    org.as_ref(),
                                    &ctx,
                                );
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                // Channel closed, continue with normal processing
                                receiver.recv().await
                            }
                        }
                    }
                }
            } else {
                receiver.recv().await
            };

            let Some(data) = data else {
                log_info!(
                    base_ctx.clone(),
                    "Processor: channel closed",
                    audience = LogAudience::Internal,
                    chunks_received = diag_chunks_received,
                    bytes_received = diag_bytes_received,
                    batches_processed = diag_batches_processed,
                    responses_sent = diag_responses_sent
                );
                break;
            };
            let request_received_at = data.received_at();
            let bridge_request_queue_us = data.queue_wait_us();
            diag_chunks_received += 1;
            diag_bytes_received += data.len() as u64;
            bridge_series.record_request_queue(bridge_request_queue_us);
            let data = data.into_bytes();

            // Reset telemetry trace context so new spans aren't nested under previous iterations
            telemetry_wrapper.reset_trace_context();

            // Create a parent span for this whole processor batch. Keep the
            // receive-only measurement as a separate child span so the trace
            // waterfall does not label backend/response work as "received".
            let mut request_span = telemetry_wrapper.client_tracer("redis.request_batch");

            let bytes_read = data.len() as u64;
            let append_result = {
                let mut receive_span = telemetry_wrapper.start_client_span("redis.request_received");
                receive_span.add_event("data received from client", vec![FastSpanAttribute::new("bytes_read", bytes_read.to_string())]);
                let result = RedisWire::append_bounded(&mut buffer, &data, MAX_REQUEST_BUFFER_BYTES);
                if let Err(err) = &result {
                    receive_span.add_event("request receive error", vec![FastSpanAttribute::new("error", err.to_string())]);
                }
                result
            };
            if let Err(err) = append_result {
                log_warn!(
                    ctx.clone(),
                    "Redis request buffer exceeded maximum size",
                    audience = LogAudience::Internal,
                    limit = MAX_REQUEST_BUFFER_BYTES,
                    bytes_read = bytes_read,
                    error = err.to_string()
                );
                let err_bytes = RedisWire::format_resp_error_line("request too large");
                let _ = sender.send(err_bytes);
                Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx).await;
                return;
            }

            // Keep batch-level metadata on the lifecycle span too.
            request_span.add_event("data received from client", vec![FastSpanAttribute::new("bytes_read", bytes_read.to_string())]);

            // Track aggregated stats for this batch of data
            let batch_start = Instant::now();
            let mut command_count: u64 = 0;
            let mut total_bytes_written: u64 = 0;
            let mut backend_duration_sum: u64 = 0;
            let mut terminate_connection = false;

            let mut commands: Vec<CommandMeta> = Vec::new();
            let mut pipeline_bytes = BytesMut::new();
            let mut parse_error: Option<Bytes> = None;

            loop {
                let (parsed, consumed) = match RedisProtocol::parse_buffer(&buffer) {
                    Ok(Some(result)) => result,
                    Ok(None) => break,
                    Err(e) => {
                        log_error!(ctx.clone(), "Redis protocol parse error", audience = LogAudience::Internal, error = e.to_string());
                        parse_error = Some(RedisWire::format_resp_error_line(&e.to_string()));
                        buffer.clear();

                        telemetry_wrapper.record(MetricEvent::ProxyError {
                            org_uuid: organization_uuid_string.as_str(),
                            interlay_uuid: &interlay_id_str,
                            error_type: "parse_error",
                        });

                        break;
                    }
                };

                let command_bytes = buffer.split_to(consumed).freeze();
                pipeline_bytes.extend_from_slice(&command_bytes);
                commands.push(CommandMeta {
                    parsed,
                    command_bytes,
                    policy_override: None,
                    abort_after_response: false,
                });
            }

            if !commands.is_empty() {
                let _ = RoutingRuntime::refresh_from_cache_if_changed(
                    &mut routing_state,
                    &mut routing_state_version,
                    &interlay_cache_uuid,
                    interlay_endpoints.as_ref(),
                    org.as_ref(),
                    &ctx,
                );

                let endpoint_uuid_for_policy: FormatEndpointUuid = routing_state.resolver.primary().eden_uuid();
                let mut any_blocked = false;

                for command in &mut commands {
                    // Track Redis transactions (MULTI/EXEC/DISCARD) and WATCH state
                    // This must happen before command execution to track the state correctly
                    match command.parsed.command() {
                        RedisApi::Subscribe
                        | RedisApi::Psubscribe
                        | RedisApi::Ssubscribe
                        | RedisApi::Unsubscribe
                        | RedisApi::Punsubscribe
                        | RedisApi::Sunsubscribe
                        | RedisApi::Auth
                        | RedisApi::Select => {
                            command.policy_override = RedisWire::session_state_rejection(command.parsed.command());
                            command.abort_after_response = true;
                            any_blocked = true;
                            terminate_connection = true;
                            continue;
                        }
                        RedisApi::Watch => {
                            // Block WATCH during replicated migration (requires connection affinity)
                            // Acquire pinned connection on first WATCH
                            if pin_tracker.pin_action() == PinAction::AcquirePin {
                                let pin_target = routing_state.resolver.primary().clone();
                                let pin_result = {
                                    let mut pin_span = telemetry_wrapper.start_client_span("redis.pinned_connection.acquire");
                                    pin_span.add_event(
                                        "acquiring Redis pinned connection",
                                        vec![
                                            FastSpanAttribute::new("command", "WATCH"),
                                            FastSpanAttribute::new("endpoint_uuid", pin_target.uuid().to_string()),
                                        ],
                                    );
                                    let pin_start = Instant::now();
                                    let result = ep.pinned_write_connection(&pin_target, &mut telemetry_wrapper).await;
                                    let duration_us = pin_start.elapsed().as_micros() as u64;
                                    pin_span.add_event(
                                        "Redis pinned connection acquisition completed",
                                        vec![FastSpanAttribute::new("duration_us", duration_us.to_string())],
                                    );
                                    if let Err(err) = &result {
                                        pin_span.add_event(
                                            "Redis pinned connection acquisition failed",
                                            vec![FastSpanAttribute::new("error", err.to_string())],
                                        );
                                    }
                                    if duration_us >= SLOW_GATEWAY_OP_LOG_THRESHOLD_US {
                                        log_debug!(
                                            ctx.clone(),
                                            "Redis pinned connection acquisition was slow",
                                            audience = LogAudience::Internal,
                                            command = "WATCH",
                                            endpoint_uuid = pin_target.uuid().to_string(),
                                            duration_us = duration_us
                                        );
                                    }
                                    result
                                };
                                telemetry_wrapper.update_traceparent(&request_span);
                                match pin_result {
                                    Ok(conn) => {
                                        pinned_conn.insert(conn);
                                        pin_tracker.mark_pinned();
                                    }
                                    Err(err) => {
                                        log_error!(
                                            ctx.clone(),
                                            "Failed to acquire pinned connection for WATCH",
                                            audience = LogAudience::Internal,
                                            error = err.to_string()
                                        );
                                        command.policy_override = Some(RedisWire::format_resp_error_line(&err.to_string()));
                                        any_blocked = true;
                                        continue;
                                    }
                                }
                            }
                        }
                        RedisApi::Unwatch => {}
                        RedisApi::Multi => {
                            // Acquire pinned connection on MULTI if not already pinned via WATCH
                            if pin_tracker.pin_action() == PinAction::AcquirePin {
                                let pin_target = routing_state.resolver.primary().clone();
                                let pin_result = {
                                    let mut pin_span = telemetry_wrapper.start_client_span("redis.pinned_connection.acquire");
                                    pin_span.add_event(
                                        "acquiring Redis pinned connection",
                                        vec![
                                            FastSpanAttribute::new("command", "MULTI"),
                                            FastSpanAttribute::new("endpoint_uuid", pin_target.uuid().to_string()),
                                        ],
                                    );
                                    let pin_start = Instant::now();
                                    let result = ep.pinned_write_connection(&pin_target, &mut telemetry_wrapper).await;
                                    let duration_us = pin_start.elapsed().as_micros() as u64;
                                    pin_span.add_event(
                                        "Redis pinned connection acquisition completed",
                                        vec![FastSpanAttribute::new("duration_us", duration_us.to_string())],
                                    );
                                    if let Err(err) = &result {
                                        pin_span.add_event(
                                            "Redis pinned connection acquisition failed",
                                            vec![FastSpanAttribute::new("error", err.to_string())],
                                        );
                                    }
                                    if duration_us >= SLOW_GATEWAY_OP_LOG_THRESHOLD_US {
                                        log_debug!(
                                            ctx.clone(),
                                            "Redis pinned connection acquisition was slow",
                                            audience = LogAudience::Internal,
                                            command = "MULTI",
                                            endpoint_uuid = pin_target.uuid().to_string(),
                                            duration_us = duration_us
                                        );
                                    }
                                    result
                                };
                                telemetry_wrapper.update_traceparent(&request_span);
                                match pin_result {
                                    Ok(conn) => {
                                        pinned_conn.insert(conn);
                                        pin_tracker.mark_pinned();
                                    }
                                    Err(err) => {
                                        log_error!(
                                            ctx.clone(),
                                            "Failed to acquire pinned connection for MULTI",
                                            audience = LogAudience::Internal,
                                            error = err.to_string()
                                        );
                                        command.policy_override = Some(RedisWire::format_resp_error_line(&err.to_string()));
                                        any_blocked = true;
                                        continue;
                                    }
                                }
                            }
                        }
                        RedisApi::Exec => {}
                        RedisApi::Discard => {}
                        RedisApi::Psync => {
                            // Handle PSYNC locally via the replication manager
                            let interlay_uuid: InterlayUuid = interlay_cache_uuid.eden_uuid();
                            let manager = crate::replication::get_or_create_manager(
                                interlay_uuid,
                                routing_state.resolver.primary(),
                                routing_state.resolver.primary().org().unwrap_or_default(),
                            );
                            let repl_id = command
                                .parsed
                                .args()
                                .first()
                                .and_then(RedisRequestMetadata::value_to_string)
                                .and_then(|s| if s == "?" { None } else { Some(s) });
                            let offset = command
                                .parsed
                                .args()
                                .get(1)
                                .and_then(|v| match v {
                                    RedisJsonValue::Integer(i) => Some(*i),
                                    RedisJsonValue::String(s) => s.as_str().parse().ok(),
                                    _ => None,
                                })
                                .unwrap_or(-1);
                            let response = {
                                let mut psync_span = telemetry_wrapper.start_client_span("redis.psync.partial_sync");
                                psync_span
                                    .add_event("handling Redis partial sync", vec![FastSpanAttribute::new("offset", offset.to_string())]);
                                let psync_start = Instant::now();
                                let response = manager.handle_partial_sync(repl_id, offset).await;
                                manager.set_streaming_mode(true).await;
                                let duration_us = psync_start.elapsed().as_micros() as u64;
                                psync_span.add_event(
                                    "Redis partial sync handled",
                                    vec![FastSpanAttribute::new("duration_us", duration_us.to_string())],
                                );
                                if duration_us >= SLOW_GATEWAY_OP_LOG_THRESHOLD_US {
                                    log_debug!(
                                        ctx.clone(),
                                        "Redis partial sync handling was slow",
                                        audience = LogAudience::Internal,
                                        duration_us = duration_us,
                                        offset = offset
                                    );
                                }
                                response
                            };
                            command.policy_override = Some(response);
                            any_blocked = true;
                            continue;
                        }
                        _ => {}
                    }

                    let policy_override = policy_override_from_guard(
                        &ctx,
                        &command.parsed,
                        policy_mode,
                        Some(organization_uuid_string.as_str()),
                        Some(&endpoint_uuid_for_policy),
                        &mut telemetry_wrapper,
                    );

                    if policy_override.is_some() {
                        any_blocked = true;
                    }
                    command.policy_override = policy_override;

                    // Commit tracker state transitions only after policy allows the command.
                    // This prevents local tracker state from diverging from backend state
                    // when a command is policy-blocked and never forwarded.
                    if command.policy_override.is_none() {
                        match command.parsed.command() {
                            RedisApi::Watch => {
                                pin_tracker.confirm_watch();
                            }
                            RedisApi::Unwatch => {
                                pin_tracker.on_unwatch();
                            }
                            RedisApi::Multi => {
                                pin_tracker.confirm_multi();
                            }
                            RedisApi::Exec => {
                                pin_tracker.on_exec_or_discard();
                            }
                            RedisApi::Discard => {
                                pin_tracker.on_exec_or_discard();
                            }
                            _ => {}
                        }
                    }
                }

                // Pipeline batching is only safe for Direct routing. Non-Direct routing
                // (ReadReplica, Sharded, ShardedWithReplicas) requires per-command routing
                // decisions, so we must fall through to the per-command path.
                let should_pipeline = parse_error.is_none() && !any_blocked && routing_state.resolver.routing().is_direct();

                if should_pipeline {
                    // Conservative routing for pipelined requests:
                    // any write command (including mixed read/write pipelines) is treated as Write.
                    // This preserves correctness by avoiding read-routing for pipelines with side effects.
                    let req_type =
                        if commands.is_empty() || commands.iter().any(|command| !command.parsed.command().request_type().is_read()) {
                            ReqType::Write
                        } else {
                            ReqType::Read
                        };
                    let request_start = Instant::now();
                    let pipeline_request_bytes = pipeline_bytes.freeze();
                    let result = if let Some(pc) = pinned_conn.as_deref_mut() {
                        match RedisBytes::from(pipeline_request_bytes.clone()).send_raw_bytes_on_conn_no_reconnect(pc).await {
                            Ok(resp) => Ok(resp),
                            Err(err) => {
                                log_warn!(
                                    ctx.clone(),
                                    "Pinned connection write failed, poisoning",
                                    audience = LogAudience::Internal,
                                    error = err.to_string()
                                );
                                pinned_conn.poison();
                                pin_tracker.on_connection_error();
                                Err(err)
                            }
                        }
                    } else {
                        ep.raw_bytes_with_req_type(
                            routing_state.resolver.primary(),
                            RedisBytes::from(pipeline_request_bytes.clone()),
                            req_type.clone(),
                            settings,
                            &mut telemetry_wrapper,
                        )
                        .await
                    };
                    telemetry_wrapper.update_traceparent(&request_span);

                    let duration_us = request_start.elapsed().as_micros() as u64;
                    {
                        backend_duration_sum = duration_us;
                    }

                    match &result {
                        Ok(resp) => {
                            if RedisWire::append_bounded(&mut response_buffer, resp, MAX_RESPONSE_BUFFER_BYTES).is_err() {
                                response_buffer.clear();
                                let err = RedisWire::format_resp_error_line("response too large");
                                response_buffer.extend_from_slice(&err);
                                total_bytes_written = err.len() as u64;
                                terminate_connection = true;
                            } else {
                                total_bytes_written = resp.len() as u64;
                            }
                            command_count = commands.len() as u64;
                        }
                        Err(e) => {
                            // A single -ERR for N pipelined commands desyncs the
                            // client response stream. Drop the connection so it
                            // reconnects cleanly. Refs: EDEN-546
                            telemetry_wrapper.record(MetricEvent::ProxyError {
                                org_uuid: organization_uuid_string.as_str(),
                                interlay_uuid: &interlay_id_str,
                                error_type: "pipeline_error",
                            });

                            log_info!(
                                base_ctx.clone(),
                                "Processor: exiting on pipeline error",
                                audience = LogAudience::Internal,
                                error = e.to_string(),
                                chunks_received = diag_chunks_received,
                                bytes_received = diag_bytes_received,
                                batches_processed = diag_batches_processed,
                                responses_sent = diag_responses_sent
                            );
                            Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx)
                                .await;
                            return;
                        }
                    }

                    // Mirror successful commands best-effort; replication streaming is handled below.
                    if result.is_ok() {
                        if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone()) {
                            let skip_reason = if pinned_conn.as_deref_mut().is_some() {
                                Some("pinned_connection")
                            } else {
                                None
                            };
                            Self::mirror_redis_command(
                                &ep,
                                &state,
                                &organization_uuid_label,
                                &interlay_id_label,
                                req_type,
                                Some(&pipeline_request_bytes),
                                commands.len(),
                                &telemetry_wrapper,
                                skip_reason,
                            )
                            .await;
                        }

                        // Stream successful write commands to replicas through the existing replication manager.
                        let interlay_uuid: InterlayUuid = interlay_cache_uuid.eden_uuid();
                        let allow_replication_stream = true;
                        if let Some(manager) = REPLICATION_MANAGERS.get(&interlay_uuid)
                            && manager.is_streaming()
                            && allow_replication_stream
                        {
                            for command in &commands {
                                if !command.parsed.command().request_type().is_read() && command.policy_override.is_none() {
                                    manager.stream_write_command(command.command_bytes.clone());
                                }
                            }
                        }
                    }
                } else {
                    for mut command in commands {
                        // Create a child span for this command (child of the
                        // request batch lifecycle span).
                        let mut command_span = telemetry_wrapper.client_tracer(format!("redis.command.{}", command.parsed.command()));
                        command_span
                            .add_event("command received", vec![FastSpanAttribute::new("command", command.parsed.command().to_string())]);

                        log_trace!(
                            ctx.clone(),
                            "Processing Redis command",
                            audience = LogAudience::Internal,
                            command = command.parsed.command()
                        );

                        let _ = RoutingRuntime::refresh_from_cache_if_changed(
                            &mut routing_state,
                            &mut routing_state_version,
                            &interlay_cache_uuid,
                            interlay_endpoints.as_ref(),
                            org.as_ref(),
                            &ctx,
                        );

                        let policy_override = command.policy_override.take();
                        let has_pinned_connection = pinned_conn.as_deref_mut().is_some();
                        let dispatch_path = RedisDispatch::command_path(policy_override.is_some(), has_pinned_connection);
                        let was_policy_blocked = matches!(dispatch_path, CommandDispatchPath::PolicyOverride);
                        let command_req_type = command.parsed.command().request_type();
                        let is_write = !command_req_type.is_read();
                        let allow_replication_stream = true;
                        let repl_bytes = if RedisDispatch::should_capture_replication_bytes(
                            is_write,
                            was_policy_blocked,
                            REPLICATION_MANAGERS.contains_key(&interlay_cache_uuid.eden_uuid::<InterlayUuid>()),
                            allow_replication_stream,
                        ) {
                            Some(command.command_bytes.clone())
                        } else {
                            None
                        };
                        let mirror_state = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone());
                        let mirror_skip_reason = if was_policy_blocked {
                            Some("policy_override")
                        } else if has_pinned_connection || pin_tracker.is_watching() || pin_tracker.is_in_multi() {
                            Some("pinned_connection")
                        } else {
                            None
                        };
                        let mirror_request_bytes = mirror_state
                            .as_ref()
                            .filter(|state| mirror_skip_reason.is_none() && Self::mirror_accepts_req_type(state, &command_req_type))
                            .map(|_| command.command_bytes.clone());
                        let command_bytes = std::mem::take(&mut command.command_bytes);

                        let request_start = Instant::now();
                        let result = RedisWire::normalize_result_for_client(match dispatch_path {
                            CommandDispatchPath::PolicyOverride => {
                                let blocked_response = policy_override.expect("policy override path requires blocked response");
                                // Policy blocked - command_bytes not needed
                                drop(command_bytes);
                                Ok(Some(blocked_response))
                            }
                            CommandDispatchPath::PinnedConnection => {
                                let pc = pinned_conn.as_deref_mut().expect("pinned dispatch path requires an active pinned connection");
                                match RedisBytes::from(command_bytes).send_raw_bytes_on_conn_no_reconnect(pc).await {
                                    Ok(resp) => Ok(Some(resp)),
                                    Err(err) => {
                                        log_warn!(
                                            ctx.clone(),
                                            "Pinned connection write failed, poisoning",
                                            audience = LogAudience::Internal,
                                            error = err.to_string()
                                        );
                                        pinned_conn.poison();
                                        pin_tracker.on_connection_error();
                                        Err(err)
                                    }
                                }
                            }
                            CommandDispatchPath::RoutedConnection => {
                                // No migration feature - route based on routing strategy (supports Direct, ReadReplica, Sharded, ShardedWithReplicas)
                                let is_read = command.parsed.command().request_type().is_read();
                                // Extract the first key using the command's own key-extraction logic,
                                // which correctly handles commands where the key isn't the first arg
                                // (e.g. EVAL, XREAD). Falls back to None for keyless commands.
                                let key_bytes: Option<Vec<u8>> = command
                                    .parsed
                                    .command()
                                    .keys_from_args(command.parsed.args())
                                    .ok()
                                    .and_then(|keys| keys.into_iter().next())
                                    .map(|key| key.as_bytes().to_vec());
                                let target = routing_state.resolver.select_endpoint(key_bytes.as_deref(), is_read);
                                ep.raw_bytes_with_req_type(
                                    target,
                                    RedisBytes::from(command_bytes),
                                    command_req_type.clone(),
                                    settings,
                                    &mut telemetry_wrapper,
                                )
                                .await
                                .map(Some)
                            }
                        });
                        telemetry_wrapper.update_traceparent(&request_span);

                        let duration_us = request_start.elapsed().as_micros() as u64;
                        if !was_policy_blocked && result.is_ok() {
                            backend_duration_sum += duration_us;
                        }

                        if let Ok(Some(_primary_response)) = &result
                            && let Some(state) = mirror_state.as_ref()
                        {
                            Self::mirror_redis_command(
                                &ep,
                                state,
                                &organization_uuid_label,
                                &interlay_id_label,
                                command_req_type,
                                mirror_request_bytes.as_ref(),
                                1,
                                &telemetry_wrapper,
                                mirror_skip_reason,
                            )
                            .await;
                        }

                        match &result {
                            Ok(Some(resp)) => {
                                let bytes_written = resp.len() as u64;
                                if RedisWire::append_bounded(&mut response_buffer, resp, MAX_RESPONSE_BUFFER_BYTES).is_err() {
                                    response_buffer.clear();
                                    let err = RedisWire::format_resp_error_line("response too large");
                                    command_count += 1;
                                    total_bytes_written += err.len() as u64;
                                    response_buffer.extend_from_slice(&err);
                                    terminate_connection = true;
                                    command_span.add_event(
                                        "command error",
                                        vec![FastSpanAttribute::new("error", "response too large".to_string())],
                                    );
                                    telemetry_wrapper.record(MetricEvent::ProxyError {
                                        org_uuid: organization_uuid_string.as_str(),
                                        interlay_uuid: &interlay_id_str,
                                        error_type: "response_too_large",
                                    });
                                } else {
                                    command_count += 1;
                                    total_bytes_written += bytes_written;

                                    command_span.add_event(
                                        "command completed",
                                        vec![
                                            FastSpanAttribute::new("duration_us", duration_us.to_string()),
                                            FastSpanAttribute::new("bytes_written", bytes_written.to_string()),
                                        ],
                                    );
                                }
                            }
                            Ok(None) => unreachable!("client-visible results should be normalized before response handling"),
                            Err(e) => {
                                let err = RedisWire::format_resp_error_line(&e.to_string());
                                if RedisWire::append_bounded(&mut response_buffer, &err, MAX_RESPONSE_BUFFER_BYTES).is_err() {
                                    response_buffer.clear();
                                    let overflow = RedisWire::format_resp_error_line("response too large");
                                    command_count += 1;
                                    total_bytes_written += overflow.len() as u64;
                                    response_buffer.extend_from_slice(&overflow);
                                    terminate_connection = true;
                                } else {
                                    command_count += 1;
                                    total_bytes_written += err.len() as u64;
                                }

                                command_span.add_event("command error", vec![FastSpanAttribute::new("error", e.to_string())]);

                                telemetry_wrapper.record(MetricEvent::ProxyError {
                                    org_uuid: organization_uuid_string.as_str(),
                                    interlay_uuid: &interlay_id_str,
                                    error_type: "command_error",
                                });
                            }
                        }

                        // Stream successful writes to replicas through the existing replication manager.
                        if let Some(bytes) = repl_bytes
                            && result.is_ok()
                        {
                            let interlay_uuid: InterlayUuid = interlay_cache_uuid.eden_uuid();
                            if let Some(manager) = REPLICATION_MANAGERS.get(&interlay_uuid)
                                && manager.is_streaming()
                            {
                                manager.stream_write_command(bytes);
                            }
                        }

                        if terminate_connection || command.abort_after_response || pin_tracker.should_abort_connection() {
                            terminate_connection = true;
                            drop(command_span);
                            telemetry_wrapper.update_traceparent(&request_span);
                            break;
                        }
                        drop(command_span);
                        telemetry_wrapper.update_traceparent(&request_span);
                    }
                }
            }

            // Release pinned connection if no longer in a transaction
            if pin_tracker.should_release() {
                pinned_conn.release();
                pin_tracker.release();
            }

            if let Some(err_bytes) = parse_error {
                response_buffer.extend_from_slice(&err_bytes);
            }

            // Record single ProxyRequest for the entire batch (hot path: zero-allocation)
            if command_count > 0 {
                let batch_duration_us = batch_start.elapsed().as_micros() as u64;
                proxy_series.record_batch(ProxyBatchRecord {
                    duration_us: batch_duration_us,
                    comparable_duration_us: Some(batch_duration_us),
                    endpoint_duration_us: Some(backend_duration_sum),
                    overhead_us: Some(batch_duration_us.saturating_sub(backend_duration_sum)),
                    bytes_read,
                    bytes_written: total_bytes_written,
                    command_count,
                });
            }

            diag_batches_processed += 1;
            if !response_buffer.is_empty() {
                let response_bytes = response_buffer.len();
                request_span
                    .add_event("response sent to client", vec![FastSpanAttribute::new("bytes_written", response_bytes.to_string())]);
                diag_responses_sent += 1;

                let send_result = {
                    let mut response_span = telemetry_wrapper.start_client_span("redis.response_queued_to_client");
                    response_span.add_event(
                        "queueing response to bridge",
                        vec![FastSpanAttribute::new("bytes_written", response_bytes.to_string())],
                    );
                    let result = sender.send_with_request_received_at_and_command_count(
                        response_buffer.split().freeze(),
                        request_received_at,
                        command_count.max(1),
                    );
                    if result.is_err() {
                        response_span.add_simple_event("response queue closed");
                    }
                    result
                };
                if send_result.is_err() {
                    log_info!(
                        base_ctx,
                        "Processor: client disconnected mid-send",
                        audience = LogAudience::Internal,
                        chunks_received = diag_chunks_received,
                        bytes_received = diag_bytes_received,
                        batches_processed = diag_batches_processed,
                        responses_sent = diag_responses_sent
                    );
                    Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx).await;
                    return;
                }
                response_buffer.clear();
            }

            if terminate_connection {
                Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx).await;
                return;
            }
        }

        // Clean up pinned connection on normal loop exit (receiver closed)
        Self::cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_watching(), pin_tracker.is_in_multi(), &base_ctx).await;

        log_trace!(base_ctx, "Wire protocol receiver closed, ending processor", audience = LogAudience::Internal);
    }
}
