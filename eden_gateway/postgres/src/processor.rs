//! PostgreSQL proxy processor.
//!
//! Handles the PG wire protocol at the proxy layer:
//! - Startup/auth handshake (synthetic responses)
//! - Message framing (type byte + length)
//! - SQL classification and direct endpoint routing via `raw_bytes_with_req_type`
//! - Transaction state tracking for ReadyForQuery
//! - Pinned connections for transaction affinity (BEGIN..COMMIT on same connection)
//! - Signal handling for graceful shutdown and endpoint updates
//! - Command policy enforcement with audit recording
//! - OpenTelemetry tracing and proxy-level telemetry metrics

mod cancel;
mod els;
mod tx;
mod wire;

use crate::replay_queue::{ReplayEntry, ReplayQueue};
use crate::session_affinity::SessionAffinityTracker;
use crate::write_serializer::WriteSerializer;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use eden_core::error::ResultEP;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::{EdenUuid, EndpointUuid, InterlayUuid, OrganizationUuid};
use eden_core::telemetry::FastSpanAttribute;
use eden_core::telemetry::TelemetryWrapper;
use eden_core::telemetry::metric_event::{MetricEvent, RecordMetric};
use eden_core::telemetry::metrics::ProxyBatchRecord;
use eden_gateway_core::response::{GatewayMirrorResponseMode, GatewayResponsePolicySpec, GatewayResponseProfile};
use eden_gateway_core::traits::{BytesQueueSender, DatabaseProtocolProcessor, ProxyRequestChunk};
use eden_logger_internal::{LogAudience, LogContext, log_debug, log_error, log_info, log_trace, log_warn};
use endpoints::endpoint::EP;
use endpoints::endpoint::postgres::ep::PostgresEp;
use endpoints::endpoint::postgres::protocol::{PgPinnedConnection, PostgresBytes, skip_sql_comments};
use ep_core::ReqType;
use ep_core::database::schema::interlay::{InterlaySignal, InterlayState};
use ep_core::settings::EdenSettings;
use postgres_wire::types::{AuthenticationRequest, BackendKeyData, EmptyQueryResponse, ErrorResponse, ParameterStatus};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::stmt_cache::{self, ClientStmtMap};
use cancel::{cancel_registry_add, cancel_registry_clear, cancel_target_from_conn, cancel_targets, forward_cancel_request};
use els::{detect_service_name, resolve_els_prefix, sql_has_els_override_attempt};
use tx::{
    DualWriteTxBuffer, PgPinnedTransactionTracker, cleanup_2pc_conn, cleanup_pinned_conn, handle_two_phase_commit,
    handle_two_phase_rollback,
};
use wire::{
    CANCEL_REQUEST_CODE, CONNECTION_COUNTER, MSG_BIND, MSG_CLOSE, MSG_COPY_DATA, MSG_COPY_DONE, MSG_COPY_FAIL, MSG_DESCRIBE, MSG_EXECUTE,
    MSG_FLUSH, MSG_PARSE, MSG_QUERY, MSG_SYNC, MSG_TERMINATE, PROTOCOL_VERSION_3_0, SSL_REQUEST_CODE, TxState, build_command_complete_msg,
    build_q_message, extract_bind_statement, extract_close_statement, extract_parse_sql, parse_startup_params,
    response_has_ready_for_query, strip_leading_command_completes,
};

fn comparable_request_duration_us(batch_duration_us: u64, command_count: u64, backend_command_count: u64) -> Option<u64> {
    (command_count > 0 && command_count == backend_command_count).then_some(batch_duration_us)
}

fn pg_req_type_label(req_type: &ReqType) -> &'static str {
    match req_type {
        ReqType::Read => "read",
        ReqType::Write => "write",
    }
}

fn classify_sql(sql: &str) -> ReqType {
    let first = skip_sql_comments(sql.trim()).split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "SHOW" | "EXPLAIN" | "DESCRIBE" | "WITH" => ReqType::Read,
        _ => ReqType::Write,
    }
}

fn is_session_command(sql: &str) -> bool {
    matches!(
        skip_sql_comments(sql.trim()).split_whitespace().next().unwrap_or("").to_ascii_uppercase().as_str(),
        "SET" | "RESET" | "DISCARD"
    )
}

fn sample_allows_mirror(sample_ratio: f64) -> bool {
    if sample_ratio >= 1.0 {
        return true;
    }
    sample_ratio > 0.0 && rand::random::<f64>() <= sample_ratio
}

fn is_pg_mirror_unsafe_sql(first_token: &str, sql: &str) -> bool {
    matches!(first_token, "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE")
        || (first_token == "SET" || first_token == "RESET" || first_token == "DISCARD")
        || (first_token == "COPY" && sql.to_ascii_uppercase().contains("STDIN"))
}

#[allow(clippy::too_many_arguments)]
fn record_pg_mirror_skip(
    telemetry_wrapper: &TelemetryWrapper,
    organization_uuid: &str,
    interlay_id: &str,
    primary_endpoint_uuid: &str,
    mirror_endpoint_uuid: &str,
    endpoint_kind: &str,
    req_type: &ReqType,
    reason: &str,
) {
    let req_type = pg_req_type_label(req_type);
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
fn spawn_postgres_mirrors(
    ep: &PostgresEp,
    state: &InterlayState,
    organization_uuid: &str,
    interlay_id: &Arc<str>,
    req_type: ReqType,
    request_bytes: PostgresBytes,
    primary_response: Bytes,
    settings: EdenSettings,
    telemetry_wrapper: &TelemetryWrapper,
    ctx: &LogContext,
    skip_reason: Option<&str>,
) {
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
    let req_type_label = pg_req_type_label(&req_type);

    if let Some(reason) = skip_reason {
        for mirror_target in state.mirror_targets() {
            record_pg_mirror_skip(
                telemetry_wrapper,
                organization_uuid,
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

    if !sample_allows_mirror(mirror.sample_ratio()) {
        for mirror_target in state.mirror_targets() {
            record_pg_mirror_skip(
                telemetry_wrapper,
                organization_uuid,
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

    for mirror_target in state.mirror_targets() {
        let mirror_endpoint_uuid = mirror_target.endpoint_uuid_label_arc();
        let Ok(permit) = mirror_target.try_acquire_owned() else {
            record_pg_mirror_skip(
                telemetry_wrapper,
                organization_uuid,
                interlay_id.as_ref(),
                primary_endpoint_uuid.as_ref(),
                mirror_endpoint_uuid.as_ref(),
                endpoint_kind,
                &req_type,
                "max_in_flight",
            );
            continue;
        };

        let ep = ep.clone();
        let mut mirror_tw = telemetry_wrapper.clone();
        let mirror_ctx = ctx.clone();
        let mirror_endpoint = mirror_target.endpoint_cache_uuid().clone();
        let interlay_id = interlay_id.clone();
        let primary_endpoint_uuid = primary_endpoint_uuid.clone();
        let organization_uuid = organization_uuid.to_string();
        let req_type = req_type.clone();
        let request_bytes = request_bytes.clone();
        let primary_response = primary_response.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let labels = [
                ("org_uuid", organization_uuid.as_str()),
                ("interlay_uuid", interlay_id.as_ref()),
                ("primary_endpoint_uuid", primary_endpoint_uuid.as_ref()),
                ("mirror_endpoint_uuid", mirror_endpoint_uuid.as_ref()),
                ("endpoint_kind", endpoint_kind),
                ("req_type", req_type_label),
            ];
            mirror_tw.metrics().proxy().record_mirror_request(&labels);
            let start = Instant::now();
            let result = ep.raw_bytes_with_req_type(&mirror_endpoint, request_bytes, req_type, settings, &mut mirror_tw).await;
            mirror_tw.metrics().proxy().record_mirror_latency(start.elapsed().as_micros() as u64, &labels);

            match result {
                Ok(mirror_response) => {
                    if primary_response.as_ref() != mirror_response.as_ref() {
                        let labels = [
                            ("org_uuid", organization_uuid.as_str()),
                            ("interlay_uuid", interlay_id.as_ref()),
                            ("primary_endpoint_uuid", primary_endpoint_uuid.as_ref()),
                            ("mirror_endpoint_uuid", mirror_endpoint_uuid.as_ref()),
                            ("endpoint_kind", endpoint_kind),
                            ("req_type", req_type_label),
                            ("reason", "response_mismatch"),
                        ];
                        mirror_tw.metrics().proxy().record_mirror_divergence(&labels);
                    }
                }
                Err(err) => {
                    let labels = [
                        ("org_uuid", organization_uuid.as_str()),
                        ("interlay_uuid", interlay_id.as_ref()),
                        ("primary_endpoint_uuid", primary_endpoint_uuid.as_ref()),
                        ("mirror_endpoint_uuid", mirror_endpoint_uuid.as_ref()),
                        ("endpoint_kind", endpoint_kind),
                        ("req_type", req_type_label),
                        ("reason", "upstream_error"),
                    ];
                    mirror_tw.metrics().proxy().record_mirror_error(&labels);
                    log_warn!(
                        mirror_ctx,
                        "Postgres mirror dispatch failed",
                        audience = LogAudience::Internal,
                        interlay_uuid = interlay_id.as_ref(),
                        mirror_endpoint_uuid = mirror_endpoint_uuid.as_ref(),
                        error = err.to_string()
                    );
                }
            }
        });
    }
}

fn comparable_endpoint_duration_us(backend_duration_sum: u64, command_count: u64, backend_command_count: u64) -> Option<u64> {
    (command_count > 0 && command_count == backend_command_count).then_some(backend_duration_sum)
}

fn comparable_overhead_duration_us(
    batch_duration_us: u64,
    backend_duration_sum: u64,
    command_count: u64,
    backend_command_count: u64,
) -> Option<u64> {
    (command_count > 0 && command_count == backend_command_count).then(|| batch_duration_us.saturating_sub(backend_duration_sum))
}

struct RoutingState {
    endpoint: EndpointCacheUuid,
}

impl RoutingState {
    fn from_interlay_state(state: &InterlayState) -> Self {
        let endpoint = state.endpoint_uuid().clone();

        Self { endpoint }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Read Routing
// ──────────────────────────────────────────────────────────────────────────────

/// Type alias for the internal cache handle used for ELS credential lookup.
pub type ElsRedisPool = database::lib::RedisConn;

pub struct PostgresProtocolProcessor {
    ep: PostgresEp,
    /// DW-4: Shared write serializer for per-row ordering during dual-write.
    write_serializer: Arc<WriteSerializer>,
    /// Optional internal cache handle for looking up per-user ELS credentials at connection startup.
    rbac_redis: Option<ElsRedisPool>,
    /// Org-key provider for decrypting ELS cache entries.
    org_key_provider: Option<std::sync::Arc<dyn database::encryption::OrgKeyProvider>>,
}

impl PostgresProtocolProcessor {
    pub fn new(ep: PostgresEp) -> Self {
        Self {
            ep,
            write_serializer: Arc::new(WriteSerializer::new()),
            rbac_redis: None,
            org_key_provider: None,
        }
    }

    /// Set the internal cache handle for ELS credential lookups.
    pub fn with_rbac_redis(mut self, pool: ElsRedisPool) -> Self {
        self.rbac_redis = Some(pool);
        self
    }

    /// Set the org-key provider for decrypting ELS cache entries.
    pub fn with_org_key_provider(mut self, provider: std::sync::Arc<dyn database::encryption::OrgKeyProvider>) -> Self {
        self.org_key_provider = Some(provider);
        self
    }
}

impl GatewayResponseProfile for PostgresProtocolProcessor {
    type Observer = ();

    fn response_policy_spec(&self) -> GatewayResponsePolicySpec {
        GatewayResponsePolicySpec::new("postgres", Some(GatewayMirrorResponseMode::CompareResponse))
    }
}

impl DatabaseProtocolProcessor for PostgresProtocolProcessor {
    fn process(
        &self,
        mut receiver: UnboundedReceiver<ProxyRequestChunk>,
        sender: BytesQueueSender,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        mut telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: std::net::SocketAddr,
        _listener_id: String,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let connection_id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
            let mut buffer = BytesMut::new();
            let mut startup_done = false;
            let mut tx_state = TxState::Idle;

            // Pinned transaction state
            let mut pin_tracker = PgPinnedTransactionTracker::new();
            let mut pinned_conn: Option<PgPinnedConnection> = None;

            // Service name (populated from startup message or metadata)
            let mut service_name;

            // ELS session variable injection — per-user credentials from the internal cache.
            // Resolved per-query (not cached for the connection lifetime) so that
            // policy revocations take effect without requiring a reconnect.
            let mut els_user_name: Option<String> = None;

            // Extended query ELS tracking: whether the SET prefix has been injected
            // into the current extended query batch (reset after each SYNC).
            let mut extended_els_injected: bool = false;
            let mut extended_els_set_count: usize = 0;

            // Extended query protocol state
            let mut extended_buf: BytesMut = BytesMut::new();
            let mut extended_sql: Option<String> = None;
            let mut extended_statement_name: Option<String> = None;
            let mut extended_request_bytes: u32 = 0;
            let mut prepared_statements: HashMap<String, String> = HashMap::new();

            // Per-client prepared statement map for connection multiplexing.
            // Maps client statement names (e.g. "sqlx_s_1") to statement
            // identity (SQL + param types + hash). Used by the statement
            // cache to remap names to backend-assigned names at SYNC time,
            // allowing multiple clients to share backend connections.
            let mut client_stmt_map = ClientStmtMap::new();

            // DW-2: Per-connection replay queue for failed secondary dual-writes.
            // Initialized after routing_state is established (below).
            let replay_queue: Option<Arc<ReplayQueue>> = None;
            let mut replay_worker_handle: Option<tokio::task::JoinHandle<()>> = None;

            // DW-5: Per-connection session affinity tracker for read-your-writes.
            let mut session_affinity = SessionAffinityTracker::new();

            // DW-8: Transaction buffer for dual-write Replicated mode.
            // When a transaction occurs during Replicated writes, commands go
            // to the authoritative side only and are buffered for secondary
            // replay on COMMIT.
            let mut dw_tx_buffer = DualWriteTxBuffer::new();

            // DW-1 2PC: Secondary pinned connection for TwoPhaseCommit mode.
            // Both databases hold open transactions simultaneously.
            let mut pinned_conn_secondary: Option<PgPinnedConnection> = None;
            // True when inside a TwoPhaseCommit transaction.
            let mut two_phase_active: bool = false;
            // Tracks whether the secondary connection hit an error mid-transaction.
            // If true, COMMIT will ROLLBACK both sides instead of attempting PREPARE.
            let mut two_phase_doomed: bool = false;
            // Per-connection counter for generating unique 2PC global IDs.
            let mut two_phase_tx_counter: u64 = 0;

            // DW-11: Client identity for CancelRequest forwarding.
            // Populated during startup when BackendKeyData is sent to the client.
            let mut client_pid: i32 = 0;
            let mut client_secret: i32 = 0;

            // Cache string IDs to avoid repeated allocations in telemetry
            let interlay_id_str = interlay_cache_uuid.uuid().to_string();
            let interlay_id_label: Arc<str> = Arc::from(interlay_id_str.as_str());
            let client_ip = client_addr.ip().to_string();

            // Get initial routing state
            let initial_state = match interlay_endpoints.get(&interlay_cache_uuid) {
                Some(state) => state.clone(),
                None => {
                    log_error!(
                        ctx.clone(),
                        "PG processor: no interlay state found",
                        audience = LogAudience::Internal,
                        interlay = &interlay_id_str
                    );
                    return;
                }
            };

            let mut routing_state = RoutingState::from_interlay_state(&initial_state);

            // DW-4: Start periodic GC for write serializer locks (idempotent, low-cost).
            let ws_for_gc = Arc::clone(&self.write_serializer);
            let ws_gc_handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    ws_for_gc.gc();
                }
            });

            // Subscribe to signals for graceful shutdown and endpoint updates
            let mut signal_rx = initial_state.subscribe_signals();

            // Get organization and endpoint UUIDs for audit recording.
            let endpoint_uuid: EndpointUuid = routing_state.endpoint.eden_uuid();
            let Some(organization_uuid_value) = initial_state.endpoint_uuid().org().map(|org| org.eden_uuid::<OrganizationUuid>()) else {
                log_warn!(
                    ctx.clone(),
                    "Postgres proxy routing endpoint has no organization UUID",
                    audience = LogAudience::Internal,
                    endpoint = routing_state.endpoint.to_string()
                );
                return;
            };
            let interlay_uuid = interlay_cache_uuid.eden_uuid::<InterlayUuid>();
            let organization_uuid = organization_uuid_value.uuid().to_string();

            // Cache ProxySeries handles for zero-allocation metric recording.
            let mut cached_endpoint_uuid = routing_state.endpoint.eden_uuid::<EndpointUuid>();
            let mut cached_endpoint_id = cached_endpoint_uuid.uuid().to_string();
            let mut proxy_series = telemetry_wrapper.metrics().proxy().series_for_organization(
                &organization_uuid_value,
                &interlay_uuid,
                &cached_endpoint_uuid,
                "postgres",
            );

            // Detect service name from metadata (startup message override comes later)
            service_name = detect_service_name(&[], &ctx);

            // Connection lifecycle: log open
            log_info!(
                ctx.clone(),
                "PG proxy processor started",
                audience = LogAudience::Internal,
                connection_id = connection_id,
                endpoint = routing_state.endpoint.uuid().to_string(),
                client_ip = &client_ip
            );

            // NOTE: proxy connection lifecycle is owned by the interlay
            // listener's per-client guard. Counting it again here would
            // double-count the session in the proxy connection gauge.

            loop {
                // Use select! to handle both data and signals
                let data = if let Some(ref mut rx) = signal_rx {
                    tokio::select! {
                        data = receiver.recv() => data,
                        signal = rx.recv() => {
                            match signal {
                                Ok(InterlaySignal::Shutdown) => {
                                    log_info!(
                                        ctx.clone(),
                                        "Shutdown signal received - closing connection",
                                        audience = LogAudience::Internal,
                                        connection_id = connection_id
                                    );
                                    cancel_registry_clear(client_pid, client_secret);
                                    cleanup_pinned_conn(
                                        &mut pinned_conn,
                                        pin_tracker.is_in_transaction(),
                                        &ctx,
                                    ).await;
                                    cleanup_2pc_conn(&mut pinned_conn_secondary, two_phase_active, &ctx).await;
                                    // No session connection cleanup needed — multiplexing uses per-batch connections.
                                    // NOTE: proxy connection close is counted by the
                                    // interlay listener guard when the accept task finishes.
                                    return;
                                }
                                Ok(InterlaySignal::MirrorUpdate) => {
                                    log_debug!(
                                        ctx.clone(),
                                        "Mirror update signal received",
                                        audience = LogAudience::Internal,
                                        connection_id = connection_id
                                    );
                                    continue;
                                }
                                Err(_) => {
                                    // Channel closed, continue with normal processing
                                    receiver.recv().await
                                }
                            }
                        }
                    }
                } else {
                    receiver.recv().await
                };

                let data = match data {
                    Some(d) => d,
                    None => {
                        log_debug!(
                            ctx.clone(),
                            "PG client disconnected",
                            audience = LogAudience::Internal,
                            connection_id = connection_id
                        );
                        // Clean up pinned connection on disconnect
                        cancel_registry_clear(client_pid, client_secret);
                        cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_in_transaction(), &ctx).await;
                        cleanup_2pc_conn(&mut pinned_conn_secondary, two_phase_active, &ctx).await;
                        break;
                    }
                };
                let request_queue_wait_us = data.queue_wait_us();
                telemetry_wrapper.metrics().proxy().record_bridge_request_queue(
                    request_queue_wait_us,
                    &[
                        ("org_uuid", organization_uuid.as_str()),
                        ("interlay_uuid", interlay_id_str.as_str()),
                        ("endpoint_uuid", cached_endpoint_id.as_str()),
                        ("endpoint_kind", "postgres"),
                    ],
                );
                let data = data.into_bytes();

                let bytes_read = data.len() as u64;
                buffer.extend_from_slice(&data);

                // Reset trace context for each batch of data received
                telemetry_wrapper.reset_trace_context();
                let mut request_span = telemetry_wrapper.client_tracer("postgres.request_received");
                request_span.add_event("data received from client", vec![FastSpanAttribute::new("bytes_read", bytes_read.to_string())]);

                let batch_start = Instant::now();
                let mut total_bytes_written: u64 = 0;
                let mut command_count: u64 = 0;
                let mut backend_duration_sum: u64 = 0;
                let mut backend_command_count: u64 = 0;

                loop {
                    if !startup_done {
                        // Startup phase: messages have no type byte
                        if buffer.len() < 4 {
                            break;
                        }
                        let length = i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
                        if buffer.len() < length {
                            break;
                        }

                        let parse_start = Instant::now();
                        let msg_bytes = buffer.split_to(length);
                        telemetry_wrapper.metrics().proxy().record_parse_duration(
                            parse_start.elapsed().as_micros() as u64,
                            &[
                                ("org_uuid", organization_uuid.as_str()),
                                ("interlay_uuid", interlay_id_str.as_str()),
                                ("endpoint_uuid", cached_endpoint_id.as_str()),
                                ("endpoint_kind", "postgres"),
                            ],
                        );

                        // Check for SSL request or CancelRequest
                        if length >= 8 {
                            let code = i32::from_be_bytes([msg_bytes[4], msg_bytes[5], msg_bytes[6], msg_bytes[7]]);
                            if code == SSL_REQUEST_CODE && length == 8 {
                                if sender.send(Bytes::from_static(b"N")).is_err() {
                                    return;
                                }
                                continue;
                            }

                            // DW-11: CancelRequest — forward to backend(s) and close.
                            if code == CANCEL_REQUEST_CODE && length == 16 {
                                let cancel_pid = i32::from_be_bytes([msg_bytes[8], msg_bytes[9], msg_bytes[10], msg_bytes[11]]);
                                let cancel_secret = i32::from_be_bytes([msg_bytes[12], msg_bytes[13], msg_bytes[14], msg_bytes[15]]);
                                log_trace!(
                                    ctx.clone(),
                                    "CancelRequest received, forwarding to backend(s)",
                                    audience = LogAudience::Internal,
                                    cancel_pid = cancel_pid
                                );
                                if let Some(targets) = cancel_targets(cancel_pid, cancel_secret) {
                                    for target in targets {
                                        tokio::spawn(async move {
                                            let _ = forward_cancel_request(&target).await;
                                        });
                                    }
                                }
                                // Per PG protocol, cancel connections close immediately.
                                return;
                            }
                        }

                        // Parse as StartupMessage
                        if length >= 8 {
                            let version = i32::from_be_bytes([msg_bytes[4], msg_bytes[5], msg_bytes[6], msg_bytes[7]]);
                            if version == PROTOCOL_VERSION_3_0 {
                                // Parse startup parameters (application_name, user, database, etc.)
                                let startup_params = parse_startup_params(&msg_bytes);
                                service_name = detect_service_name(&startup_params, &ctx);

                                // Extract PG user for per-query ELS resolution.
                                els_user_name = startup_params
                                    .iter()
                                    .find(|(k, _)| k.eq_ignore_ascii_case("user"))
                                    .map(|(_, v)| v.trim().to_string())
                                    .filter(|v| !v.is_empty());

                                log_trace!(
                                    ctx.clone(),
                                    "PG startup message received",
                                    audience = LogAudience::Internal,
                                    service = &service_name
                                );

                                // Build startup response using postgres-wire types
                                let mut response = BytesMut::new();
                                response.extend_from_slice(&AuthenticationRequest::Ok.encode());
                                response
                                    .extend_from_slice(&ParameterStatus::new("server_version".to_string(), "16.0".to_string()).encode());
                                response
                                    .extend_from_slice(&ParameterStatus::new("client_encoding".to_string(), "UTF8".to_string()).encode());
                                response
                                    .extend_from_slice(&ParameterStatus::new("server_encoding".to_string(), "UTF8".to_string()).encode());
                                response.extend_from_slice(&ParameterStatus::new("DateStyle".to_string(), "ISO, MDY".to_string()).encode());
                                response.extend_from_slice(&ParameterStatus::new("TimeZone".to_string(), "UTC".to_string()).encode());
                                response
                                    .extend_from_slice(&ParameterStatus::new("integer_datetimes".to_string(), "on".to_string()).encode());
                                response.extend_from_slice(
                                    &ParameterStatus::new("standard_conforming_strings".to_string(), "on".to_string()).encode(),
                                );
                                // server_version_num: sqlx uses this for feature detection.
                                // Must be consistent with server_version above.
                                response.extend_from_slice(
                                    &ParameterStatus::new("server_version_num".to_string(), "160000".to_string()).encode(),
                                );
                                // DW-11: Unique process_id + random secret_key so
                                // CancelRequest can be mapped back to this connection.
                                client_pid = (connection_id as i32).wrapping_add(1);
                                client_secret = rand::random::<i32>();
                                response.extend_from_slice(&BackendKeyData::new(client_pid, client_secret).encode());
                                response.extend_from_slice(&TxState::Idle.ready_for_query());

                                if sender.send(response.freeze()).is_err() {
                                    return;
                                }
                                startup_done = true;
                                continue;
                            }
                        }

                        // Unknown startup message
                        log_warn!(ctx.clone(), "PG unsupported startup message", audience = LogAudience::Internal, length = length);
                        let err = ErrorResponse::simple("FATAL", "08P01", "unsupported startup message");
                        let _ = sender.send(Bytes::from(err.encode()));
                        return;
                    }

                    // Regular message phase: type(1) + length(4) + payload
                    if buffer.len() < 5 {
                        break;
                    }
                    let msg_type = buffer[0];
                    let length = i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                    let total_len = 1 + length;

                    if buffer.len() < total_len {
                        break;
                    }

                    let parse_start = Instant::now();
                    let msg_bytes = buffer.split_to(total_len);
                    telemetry_wrapper.metrics().proxy().record_parse_duration(
                        parse_start.elapsed().as_micros() as u64,
                        &[
                            ("org_uuid", organization_uuid.as_str()),
                            ("interlay_uuid", interlay_id_str.as_str()),
                            ("endpoint_uuid", cached_endpoint_id.as_str()),
                            ("endpoint_kind", "postgres"),
                        ],
                    );

                    // Refresh routing state on each command (unless pinned — deferred)
                    if !pin_tracker.should_defer_endpoint_update()
                        && let Some(state) = interlay_endpoints.get(&interlay_cache_uuid)
                    {
                        let new_routing = RoutingState::from_interlay_state(&state);
                        if routing_state.endpoint.uuid() != new_routing.endpoint.uuid() {
                            cached_endpoint_uuid = new_routing.endpoint.eden_uuid::<EndpointUuid>();
                            cached_endpoint_id = cached_endpoint_uuid.uuid().to_string();
                            proxy_series = telemetry_wrapper.metrics().proxy().series_for_organization(
                                &organization_uuid_value,
                                &interlay_uuid,
                                &cached_endpoint_uuid,
                                "postgres",
                            );
                        }
                        routing_state = new_routing;
                    }

                    match msg_type {
                        MSG_TERMINATE => {
                            log_debug!(
                                ctx.clone(),
                                "PG client terminated",
                                audience = LogAudience::Internal,
                                connection_id = connection_id
                            );
                            cancel_registry_clear(client_pid, client_secret);
                            cleanup_pinned_conn(&mut pinned_conn, pin_tracker.is_in_transaction(), &ctx).await;
                            cleanup_2pc_conn(&mut pinned_conn_secondary, two_phase_active, &ctx).await;
                            // Proxy connection close is counted by the interlay listener guard.
                            return;
                        }

                        MSG_QUERY => {
                            command_count += 1;

                            // Extract SQL from raw Q message
                            let sql_bytes = &msg_bytes[5..msg_bytes.len() - 1];
                            let sql = match std::str::from_utf8(sql_bytes) {
                                Ok(s) => s,
                                Err(_) => {
                                    let err = ErrorResponse::simple("ERROR", "22021", "invalid byte sequence for encoding UTF8");
                                    let mut resp = BytesMut::new();
                                    resp.extend_from_slice(&err.encode());
                                    resp.extend_from_slice(&tx_state.ready_for_query());
                                    let resp_bytes = resp.freeze();
                                    total_bytes_written += resp_bytes.len() as u64;
                                    if sender.send(resp_bytes).is_err() {
                                        return;
                                    }
                                    telemetry_wrapper.record(MetricEvent::ProxyError {
                                        org_uuid: organization_uuid.as_str(),
                                        interlay_uuid: &interlay_id_str,
                                        error_type: "parse_error",
                                    });
                                    continue;
                                }
                            };

                            let sql_trimmed = sql.trim();
                            if sql_trimmed.is_empty() {
                                let mut resp = BytesMut::new();
                                resp.extend_from_slice(&EmptyQueryResponse::encode());
                                resp.extend_from_slice(&tx_state.ready_for_query());
                                let resp_bytes = resp.freeze();
                                total_bytes_written += resp_bytes.len() as u64;
                                if sender.send(resp_bytes).is_err() {
                                    return;
                                }
                                continue;
                            }

                            // Extract SQL type (first keyword)
                            let upper_first = skip_sql_comments(sql_trimmed).split_whitespace().next().unwrap_or("").to_ascii_uppercase();

                            // Create per-query tracing span
                            let mut command_span = telemetry_wrapper.client_tracer(format!("postgres.query.{}", upper_first));
                            command_span.add_event("query received", vec![FastSpanAttribute::new("sql_type", upper_first.clone())]);

                            let policy_routing_start = Instant::now();

                            // Transaction state tracking + pinned connection
                            let is_begin = matches!(upper_first.as_str(), "BEGIN" | "START");
                            let is_end = matches!(upper_first.as_str(), "COMMIT" | "END" | "ROLLBACK");

                            if is_begin {
                                // Reset transaction command counter

                                tx_state = TxState::InTransaction;
                                pin_tracker.on_begin();

                                // Acquire pinned connection if not already pinned
                                if pin_tracker.needs_pin() {
                                    let pin_target = routing_state.endpoint.clone();

                                    let pool_wait_start = Instant::now();
                                    let pin_result = self.ep.pinned_write_connection(&pin_target, &mut telemetry_wrapper).await;
                                    telemetry_wrapper.metrics().proxy().record_backend_pool_wait(
                                        pool_wait_start.elapsed().as_micros() as u64,
                                        &[
                                            ("org_uuid", organization_uuid.as_str()),
                                            ("interlay_uuid", interlay_id_str.as_str()),
                                            ("endpoint_uuid", cached_endpoint_id.as_str()),
                                            ("endpoint_kind", "postgres"),
                                        ],
                                    );
                                    match pin_result {
                                        Ok(client) => {
                                            // DW-11: Register primary for cancel forwarding.
                                            if let Some(target) = cancel_target_from_conn(&client) {
                                                cancel_registry_add(client_pid, client_secret, target);
                                            }
                                            pinned_conn = Some(client);
                                            pin_tracker.mark_pinned();
                                            log_trace!(
                                                ctx.clone(),
                                                "PG pinned connection acquired for transaction",
                                                audience = LogAudience::Internal,
                                                connection_id = connection_id
                                            );
                                        }
                                        Err(e) => {
                                            log_error!(
                                                ctx.clone(),
                                                "Failed to acquire pinned connection",
                                                audience = LogAudience::Internal,
                                                error = e.to_string()
                                            );
                                            tx_state = TxState::Failed;
                                            pin_tracker.on_connection_error();
                                            dw_tx_buffer.discard();

                                            let err = ErrorResponse::simple(
                                                "ERROR",
                                                "08006",
                                                &format!("failed to acquire transaction connection: {}", e),
                                            );
                                            let mut resp = BytesMut::new();
                                            resp.extend_from_slice(&err.encode());
                                            resp.extend_from_slice(&tx_state.ready_for_query());
                                            let resp_bytes = resp.freeze();
                                            total_bytes_written += resp_bytes.len() as u64;
                                            if sender.send(resp_bytes).is_err() {
                                                return;
                                            }
                                            telemetry_wrapper.record(MetricEvent::ProxyError {
                                                org_uuid: organization_uuid.as_str(),
                                                interlay_uuid: &interlay_id_str,
                                                error_type: "connection_error",
                                            });
                                            continue;
                                        }
                                    }
                                }
                            } else if is_end && tx_state != TxState::Idle {
                                let is_commit = matches!(upper_first.as_str(), "COMMIT" | "END");

                                // DW-1 2PC: Handle COMMIT/ROLLBACK with two-phase commit protocol.
                                if two_phase_active {
                                    tx_state = TxState::Idle;
                                    pin_tracker.on_end();

                                    let two_phase_resp = if is_commit {
                                        if two_phase_doomed {
                                            // Secondary failed during transaction — ROLLBACK both.
                                            if let (Some(auth), Some(sec)) = (pinned_conn.as_mut(), pinned_conn_secondary.as_mut()) {
                                                handle_two_phase_rollback(auth, sec, &ctx).await;
                                            }
                                            log_warn!(
                                                ctx.clone(),
                                                "2PC: COMMIT aborted — secondary had errors during transaction",
                                                audience = LogAudience::Internal,
                                                connection_id = connection_id
                                            );
                                            let err = ErrorResponse::simple(
                                                "ERROR",
                                                "40000",
                                                "2PC transaction rollback: secondary database error during transaction",
                                            );
                                            let mut buf = BytesMut::new();
                                            buf.extend_from_slice(&err.encode());
                                            buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                            buf.freeze()
                                        } else if let (Some(auth), Some(sec)) = (pinned_conn.as_mut(), pinned_conn_secondary.as_mut()) {
                                            let gid = format!("eden_2pc_{}_{}", connection_id, two_phase_tx_counter);
                                            two_phase_tx_counter += 1;
                                            match handle_two_phase_commit(auth, sec, &gid, &ctx).await {
                                                Ok(()) => {
                                                    log_info!(
                                                        ctx.clone(),
                                                        "2PC: transaction committed on both databases",
                                                        audience = LogAudience::Internal,
                                                        connection_id = connection_id,
                                                        gid = gid.as_str()
                                                    );
                                                    let mut buf = BytesMut::new();
                                                    buf.extend_from_slice(&build_command_complete_msg("COMMIT"));
                                                    buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                                    buf.freeze()
                                                }
                                                Err(e) => {
                                                    log_error!(
                                                        ctx.clone(),
                                                        "2PC: commit failed",
                                                        audience = LogAudience::Internal,
                                                        connection_id = connection_id,
                                                        gid = gid.as_str(),
                                                        error = e.as_str()
                                                    );
                                                    let err = ErrorResponse::simple("ERROR", "40000", &format!("2PC commit failed: {}", e));
                                                    let mut buf = BytesMut::new();
                                                    buf.extend_from_slice(&err.encode());
                                                    buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                                    buf.freeze()
                                                }
                                            }
                                        } else {
                                            // Should not happen — 2PC active but missing connections.
                                            let err = ErrorResponse::simple("ERROR", "XX000", "2PC internal error: missing connections");
                                            let mut buf = BytesMut::new();
                                            buf.extend_from_slice(&err.encode());
                                            buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                            buf.freeze()
                                        }
                                    } else {
                                        // ROLLBACK — send to both connections.
                                        if let (Some(auth), Some(sec)) = (pinned_conn.as_mut(), pinned_conn_secondary.as_mut()) {
                                            handle_two_phase_rollback(auth, sec, &ctx).await;
                                        }
                                        let mut buf = BytesMut::new();
                                        buf.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
                                        buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                        buf.freeze()
                                    };

                                    // Release both connections and reset 2PC state.
                                    cancel_registry_clear(client_pid, client_secret);
                                    two_phase_active = false;
                                    two_phase_doomed = false;
                                    pinned_conn = None;
                                    pinned_conn_secondary = None;
                                    pin_tracker.release();

                                    total_bytes_written += two_phase_resp.len() as u64;
                                    if sender.send(two_phase_resp).is_err() {
                                        return;
                                    }
                                    continue;
                                }

                                tx_state = TxState::Idle;
                                pin_tracker.on_end();

                                // DW-8: On COMMIT, replay buffered writes to secondary.
                                // On ROLLBACK, discard the buffer.
                                if dw_tx_buffer.is_active() {
                                    if is_commit {
                                        let (session_cmds, writes, secondary_ep) = dw_tx_buffer.drain();
                                        if let Some(rq) = replay_queue.as_ref()
                                            && let Some(sec_ep) = secondary_ep
                                            && !writes.is_empty()
                                        {
                                            // DW-25: Replay as a single transaction batch
                                            // for atomicity and ordering on the secondary.
                                            let entry = ReplayEntry::new_transaction_batch(session_cmds, writes, sec_ep);
                                            rq.enqueue(entry, &ctx).await;
                                        }
                                    } else {
                                        dw_tx_buffer.discard();
                                    }
                                }
                            }

                            // DW-16: Track savepoints in the transaction buffer.
                            if dw_tx_buffer.is_active() {
                                if upper_first == "SAVEPOINT" {
                                    dw_tx_buffer.on_savepoint();
                                } else if upper_first.starts_with("RELEASE") {
                                    dw_tx_buffer.on_release_savepoint();
                                } else if upper_first.starts_with("ROLLBACK") && sql_trimmed.to_ascii_uppercase().contains(" TO ") {
                                    dw_tx_buffer.on_rollback_to_savepoint();
                                }
                            }

                            // Copy SQL before moving msg_bytes
                            let sql_owned = sql_trimmed.to_string();
                            let req_type = classify_sql(&sql_owned);

                            let is_write = req_type == ReqType::Write;

                            // Resolve ELS credentials per-query from Redis so that
                            // policy changes take effect without reconnecting.
                            let els_resolved = resolve_els_prefix(
                                self.rbac_redis.as_ref(),
                                &routing_state.endpoint,
                                els_user_name.as_deref(),
                                self.org_key_provider.as_deref(),
                            )
                            .await;

                            // When ELS is active, reject queries that attempt to override
                            // proxy-controlled session variables (SET app.*).
                            let (pg_bytes, els_set_count) = if let Some((ref prefix, set_count)) = els_resolved {
                                if sql_has_els_override_attempt(&sql_owned) {
                                    log_warn!(
                                        ctx.clone(),
                                        "ELS: rejected query attempting to override RLS session variables",
                                        audience = LogAudience::Internal,
                                        connection_id = connection_id
                                    );
                                    let err = ErrorResponse::simple(
                                        "ERROR",
                                        "42501",
                                        "setting app.* session variables is not permitted when ELS policies are active",
                                    );
                                    let mut resp = BytesMut::new();
                                    resp.extend_from_slice(&err.encode());
                                    resp.extend_from_slice(&tx_state.ready_for_query());
                                    let resp_bytes = resp.freeze();
                                    total_bytes_written += resp_bytes.len() as u64;
                                    if sender.send(resp_bytes).is_err() {
                                        return;
                                    }
                                    continue;
                                }
                                let modified_sql = format!("{}{}", prefix, sql_trimmed);
                                (PostgresBytes::from(build_q_message(&modified_sql)), set_count)
                            } else {
                                (PostgresBytes::from(Bytes::copy_from_slice(msg_bytes.as_ref())), 0)
                            };
                            telemetry_wrapper.metrics().proxy().record_policy_routing_duration(
                                policy_routing_start.elapsed().as_micros() as u64,
                                &[
                                    ("org_uuid", organization_uuid.as_str()),
                                    ("interlay_uuid", interlay_id_str.as_str()),
                                    ("endpoint_uuid", cached_endpoint_id.as_str()),
                                    ("endpoint_kind", "postgres"),
                                ],
                            );

                            // DW-8: Buffer write bytes during Replicated-mode transactions.
                            // Skip buffering in 2PC mode — writes go to both connections in real-time.
                            if dw_tx_buffer.is_active() && !two_phase_active && is_write {
                                dw_tx_buffer.push(Bytes::copy_from_slice(pg_bytes.bytes()));
                            }

                            // DW-15: Buffer session commands for replay context.
                            if dw_tx_buffer.is_active() && !two_phase_active && is_session_command(&sql_owned) {
                                dw_tx_buffer.push_session(Bytes::copy_from_slice(pg_bytes.bytes()));
                            }

                            let mirror_pg_bytes = pg_bytes.clone();

                            // Execute query
                            let query_start = Instant::now();
                            // response_has_rfq: true if the raw response already includes
                            // ReadyForQuery from the real backend (raw wire protocol path).
                            let (result, response_has_rfq) = if let Some(ref mut client) = pinned_conn {
                                // DW-1 2PC: Mirror all queries to secondary connection.
                                // Both databases see the same sequence of commands.
                                if two_phase_active
                                    && !two_phase_doomed
                                    && let Some(ref mut sec_client) = pinned_conn_secondary
                                {
                                    let sec_bytes = Bytes::copy_from_slice(pg_bytes.bytes());
                                    match sec_client.send_query_raw(&sec_bytes).await {
                                        Ok((resp, _)) => {
                                            if crate::replay_queue::response_has_error(&resp) {
                                                two_phase_doomed = true;
                                                log_warn!(
                                                    ctx.clone(),
                                                    "2PC: secondary query error — transaction doomed",
                                                    audience = LogAudience::Internal,
                                                    connection_id = connection_id
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            two_phase_doomed = true;
                                            log_warn!(
                                                ctx.clone(),
                                                "2PC: secondary connection error — transaction doomed",
                                                audience = LogAudience::Internal,
                                                error = e.to_string(),
                                                connection_id = connection_id
                                            );
                                        }
                                    }
                                }

                                // Use pinned connection for transaction affinity (raw wire protocol).
                                // Response includes ReadyForQuery from the real backend.
                                let r = pg_bytes.send_raw_on_pinned(client).await.map(|(bytes, _)| bytes);
                                (r, true)
                            } else {
                                // Normal routing through the pool (legacy bb8 path).
                                // Response does NOT include ReadyForQuery.
                                let r = route_query(
                                    &self.ep,
                                    &interlay_id_str,
                                    &routing_state,
                                    pg_bytes,
                                    &sql_owned,
                                    req_type.clone(),
                                    replay_queue.as_deref(),
                                    &mut session_affinity,
                                    &self.write_serializer,
                                    settings,
                                    &mut telemetry_wrapper,
                                    &ctx,
                                )
                                .await;
                                (r, false)
                            };

                            let query_duration_us = query_start.elapsed().as_micros() as u64;
                            backend_duration_sum += query_duration_us;
                            backend_command_count += 1;

                            if let Ok(primary_response) = &result
                                && let Some(state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone())
                            {
                                let mirror_skip_reason = if pinned_conn.is_some() {
                                    Some("pinned_connection")
                                } else if is_pg_mirror_unsafe_sql(&upper_first, sql_trimmed) {
                                    Some("session_affine")
                                } else {
                                    None
                                };
                                spawn_postgres_mirrors(
                                    &self.ep,
                                    &state,
                                    &organization_uuid,
                                    &interlay_id_label,
                                    req_type,
                                    mirror_pg_bytes,
                                    primary_response.clone(),
                                    settings,
                                    &telemetry_wrapper,
                                    &ctx,
                                    mirror_skip_reason,
                                );
                            }

                            let response_encode_start = Instant::now();
                            let mut response = BytesMut::new();
                            let query_success;
                            match result {
                                Ok(response_bytes) => {
                                    query_success = true;
                                    command_span.add_event(
                                        "query completed",
                                        vec![
                                            FastSpanAttribute::new("duration_us", query_duration_us as i64),
                                            FastSpanAttribute::new("bytes_written", response_bytes.len() as i64),
                                        ],
                                    );

                                    // Per-query audit trail
                                    eden_gateway_core::audit::pg_query_record(
                                        &organization_uuid,
                                        &endpoint_uuid,
                                        &upper_first,
                                        query_duration_us,
                                        true,
                                        &service_name,
                                        Some(&client_ip),
                                        connection_id,
                                    );

                                    // Extract transaction status from ReadyForQuery in raw response
                                    if response_has_rfq && response_bytes.len() >= 6 {
                                        let end = response_bytes.len();
                                        // ReadyForQuery: 'Z'(1) + length(4) + status(1) = 6 bytes at end
                                        if response_bytes[end - 6] == b'Z' {
                                            tx_state = match response_bytes[end - 1] {
                                                b'I' => TxState::Idle,
                                                b'T' => TxState::InTransaction,
                                                b'E' => TxState::Failed,
                                                _ => tx_state,
                                            };
                                        }
                                    }

                                    // Strip ELS SET CommandComplete messages before sending to client
                                    let client_response = if els_set_count > 0 {
                                        strip_leading_command_completes(response_bytes, els_set_count)
                                    } else {
                                        response_bytes
                                    };
                                    response.extend_from_slice(&client_response);
                                }
                                Err(e) => {
                                    query_success = false;
                                    if tx_state == TxState::InTransaction {
                                        tx_state = TxState::Failed;
                                    }
                                    command_span.add_event("query error", vec![FastSpanAttribute::new("error", e.to_string())]);
                                    log_error!(
                                        ctx.clone(),
                                        "PG query error",
                                        audience = LogAudience::Internal,
                                        error = e.to_string(),
                                        sql = &sql_owned
                                    );

                                    // Per-query audit trail (error)
                                    eden_gateway_core::audit::pg_query_record(
                                        &organization_uuid,
                                        &endpoint_uuid,
                                        &upper_first,
                                        query_duration_us,
                                        false,
                                        &service_name,
                                        Some(&client_ip),
                                        connection_id,
                                    );

                                    // If pinned and error, mark connection as failed
                                    if pinned_conn.is_some() && is_begin {
                                        cancel_registry_clear(client_pid, client_secret);
                                        pin_tracker.on_connection_error();
                                        pinned_conn = None;
                                    }

                                    response.extend_from_slice(&ErrorResponse::simple("ERROR", "XX000", &e.to_string()).encode());

                                    telemetry_wrapper.record(MetricEvent::ProxyError {
                                        org_uuid: organization_uuid.as_str(),
                                        interlay_uuid: &interlay_id_str,
                                        error_type: "command_error",
                                    });
                                }
                            }

                            // Append synthetic ReadyForQuery only when the raw response
                            // does not already contain one (legacy bb8 path + error responses).
                            if !response_has_rfq || !query_success {
                                response.extend_from_slice(&tx_state.ready_for_query());
                            }
                            telemetry_wrapper.metrics().proxy().record_response_encode_duration(
                                response_encode_start.elapsed().as_micros() as u64,
                                &[
                                    ("org_uuid", organization_uuid.as_str()),
                                    ("interlay_uuid", interlay_id_str.as_str()),
                                    ("endpoint_uuid", cached_endpoint_id.as_str()),
                                    ("endpoint_kind", "postgres"),
                                ],
                            );

                            let resp_bytes = response.freeze();
                            total_bytes_written += resp_bytes.len() as u64;
                            if sender.send(resp_bytes).is_err() {
                                telemetry_wrapper.metrics().proxy().record_bridge_enqueue_rejection(&[
                                    ("org_uuid", organization_uuid.as_str()),
                                    ("interlay_uuid", interlay_id_str.as_str()),
                                    ("endpoint_uuid", cached_endpoint_id.as_str()),
                                    ("endpoint_kind", "postgres"),
                                    ("queue", "response"),
                                    ("reason", "queue_full_or_closed"),
                                ]);
                                return;
                            }

                            // Release pinned connection after COMMIT/END/ROLLBACK
                            if pin_tracker.should_release() && pinned_conn.is_some() {
                                // Return pinned connection to pool (drop).
                                // Statement cache is per-backend and persists.
                                cancel_registry_clear(client_pid, client_secret);
                                pinned_conn = None;
                                pin_tracker.release();
                                log_trace!(
                                    ctx.clone(),
                                    "PG pinned connection released after transaction end",
                                    audience = LogAudience::Internal,
                                    connection_id = connection_id
                                );

                                // Apply deferred endpoint update if pending.
                                if pin_tracker.take_pending_endpoint_update() {
                                    log_info!(
                                        ctx.clone(),
                                        "Applying deferred endpoint update after transaction end",
                                        audience = LogAudience::Internal
                                    );
                                    if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid) {
                                        let new_routing = RoutingState::from_interlay_state(&state);
                                        if routing_state.endpoint.uuid() != new_routing.endpoint.uuid() {
                                            cached_endpoint_uuid = new_routing.endpoint.eden_uuid::<EndpointUuid>();
                                            cached_endpoint_id = cached_endpoint_uuid.uuid().to_string();
                                            proxy_series = telemetry_wrapper.metrics().proxy().series_for_organization(
                                                &organization_uuid_value,
                                                &interlay_uuid,
                                                &cached_endpoint_uuid,
                                                "postgres",
                                            );
                                        }
                                        routing_state = new_routing;
                                    }
                                }
                            }
                        }

                        // Extended Query Protocol: buffered passthrough
                        MSG_PARSE => {
                            if let Some((statement_name, sql)) = extract_parse_sql(&msg_bytes) {
                                let sql_trimmed = sql.trim();
                                let upper_first =
                                    skip_sql_comments(sql_trimmed).split_whitespace().next().unwrap_or("").to_ascii_uppercase();

                                if !sql_trimmed.is_empty() {
                                    let mut command_span = telemetry_wrapper.client_tracer(format!("postgres.parse.{}", upper_first));
                                    command_span.add_event("parse received", vec![FastSpanAttribute::new("sql_type", upper_first.clone())]);
                                }

                                prepared_statements.insert(statement_name.clone(), sql.clone());
                                extended_sql = Some(sql);
                                extended_statement_name = Some(statement_name);
                            }

                            // ELS: Resolve per-batch from Redis and inject SET commands as a
                            // simple query (Q message) before the first PARSE. Reject queries
                            // that attempt to override ELS-controlled session variables.
                            if !extended_els_injected {
                                let els_batch_resolved = resolve_els_prefix(
                                    self.rbac_redis.as_ref(),
                                    &routing_state.endpoint,
                                    els_user_name.as_deref(),
                                    self.org_key_provider.as_deref(),
                                )
                                .await;
                                if let Some((ref prefix, set_count)) = els_batch_resolved
                                    && let Some(ref sql_str) = extended_sql
                                {
                                    if sql_has_els_override_attempt(sql_str) {
                                        log_warn!(
                                            ctx.clone(),
                                            "ELS: rejected extended query attempting to override RLS session variables",
                                            audience = LogAudience::Internal,
                                            connection_id = connection_id
                                        );
                                        let err = ErrorResponse::simple(
                                            "ERROR",
                                            "42501",
                                            "setting app.* session variables is not permitted when ELS policies are active",
                                        );
                                        let mut resp = BytesMut::new();
                                        resp.extend_from_slice(&err.encode());
                                        resp.extend_from_slice(&tx_state.ready_for_query());
                                        let resp_bytes = resp.freeze();
                                        total_bytes_written += resp_bytes.len() as u64;
                                        if sender.send(resp_bytes).is_err() {
                                            return;
                                        }
                                        extended_buf.clear();
                                        extended_sql = None;
                                        extended_statement_name = None;
                                        extended_request_bytes = 0;
                                        extended_els_injected = false;
                                        extended_els_set_count = 0;
                                        continue;
                                    }
                                    extended_buf.extend_from_slice(&build_q_message(prefix));
                                    extended_els_injected = true;
                                    extended_els_set_count = set_count;
                                }
                            }

                            extended_request_bytes = extended_request_bytes.saturating_add(msg_bytes.len() as u32);
                            extended_buf.extend_from_slice(&msg_bytes);
                        }
                        MSG_BIND => {
                            if extended_sql.is_none()
                                && let Some(statement_name) = extract_bind_statement(&msg_bytes)
                            {
                                extended_statement_name = Some(statement_name.clone());
                                if let Some(sql) = prepared_statements.get(&statement_name) {
                                    extended_sql = Some(sql.clone());
                                }
                            }

                            extended_request_bytes = extended_request_bytes.saturating_add(msg_bytes.len() as u32);
                            extended_buf.extend_from_slice(&msg_bytes);
                        }
                        MSG_EXECUTE | MSG_DESCRIBE => {
                            extended_request_bytes = extended_request_bytes.saturating_add(msg_bytes.len() as u32);
                            extended_buf.extend_from_slice(&msg_bytes);
                        }
                        MSG_CLOSE => {
                            // Remove the named prepared statement from the local map to
                            // prevent unbounded growth on long-lived connections.
                            if let Some(name) = extract_close_statement(&msg_bytes) {
                                prepared_statements.remove(&name);
                            }
                            extended_request_bytes = extended_request_bytes.saturating_add(msg_bytes.len() as u32);
                            extended_buf.extend_from_slice(&msg_bytes);
                        }
                        MSG_FLUSH => {
                            // Flush: buffer the message but don't send yet.
                            // Extended query batches are sent atomically on Sync.
                            extended_request_bytes = extended_request_bytes.saturating_add(msg_bytes.len() as u32);
                            extended_buf.extend_from_slice(&msg_bytes);
                        }
                        MSG_SYNC => {
                            command_count += 1;

                            extended_request_bytes = extended_request_bytes.saturating_add(msg_bytes.len() as u32);
                            extended_buf.extend_from_slice(&msg_bytes);

                            if extended_sql.is_none()
                                && let Some(statement_name) = extended_statement_name.clone()
                                && let Some(sql) = prepared_statements.get(&statement_name)
                            {
                                extended_sql = Some(sql.clone());
                            }

                            let sql_owned = extended_sql.clone().unwrap_or_default();
                            let sql_trimmed = sql_owned.trim();
                            let upper_first = sql_trimmed.split_whitespace().next().unwrap_or("").to_ascii_uppercase();

                            // DW-18: Detect BEGIN/COMMIT/ROLLBACK/SAVEPOINT in extended query path.
                            // Must handle pinning and tx buffer before the batch is sent.
                            let is_begin = matches!(upper_first.as_str(), "BEGIN" | "START");
                            let is_end = matches!(upper_first.as_str(), "COMMIT" | "END" | "ROLLBACK")
                                && !sql_owned.trim().to_ascii_uppercase().contains(" TO ");

                            if is_begin {
                                pin_tracker.on_begin();

                                // Acquire pinned connection if not already pinned.
                                if pin_tracker.needs_pin() {
                                    let pin_target = routing_state.endpoint.clone();

                                    let pool_wait_start = Instant::now();
                                    let pin_result = self.ep.pinned_write_connection(&pin_target, &mut telemetry_wrapper).await;
                                    telemetry_wrapper.metrics().proxy().record_backend_pool_wait(
                                        pool_wait_start.elapsed().as_micros() as u64,
                                        &[
                                            ("org_uuid", organization_uuid.as_str()),
                                            ("interlay_uuid", interlay_id_str.as_str()),
                                            ("endpoint_uuid", cached_endpoint_id.as_str()),
                                            ("endpoint_kind", "postgres"),
                                        ],
                                    );
                                    match pin_result {
                                        Ok(client) => {
                                            // DW-11: Register primary for cancel forwarding.
                                            if let Some(target) = cancel_target_from_conn(&client) {
                                                cancel_registry_add(client_pid, client_secret, target);
                                            }
                                            pinned_conn = Some(client);
                                            pin_tracker.mark_pinned();
                                            log_trace!(
                                                ctx.clone(),
                                                "DW-18: pinned connection acquired for extended query transaction",
                                                audience = LogAudience::Internal,
                                                connection_id = connection_id
                                            );
                                        }
                                        Err(e) => {
                                            log_error!(
                                                ctx.clone(),
                                                "DW-18: failed to acquire pinned connection for extended query",
                                                audience = LogAudience::Internal,
                                                error = e.to_string()
                                            );
                                            tx_state = TxState::Failed;
                                            pin_tracker.on_connection_error();
                                            dw_tx_buffer.discard();

                                            let err = ErrorResponse::simple(
                                                "ERROR",
                                                "08006",
                                                &format!("failed to acquire transaction connection: {}", e),
                                            );
                                            let mut resp = BytesMut::new();
                                            resp.extend_from_slice(&err.encode());
                                            resp.extend_from_slice(&tx_state.ready_for_query());
                                            let resp_bytes = resp.freeze();
                                            total_bytes_written += resp_bytes.len() as u64;
                                            if sender.send(resp_bytes).is_err() {
                                                return;
                                            }
                                            telemetry_wrapper.record(MetricEvent::ProxyError {
                                                org_uuid: organization_uuid.as_str(),
                                                interlay_uuid: &interlay_id_str,
                                                error_type: "connection_error",
                                            });
                                            continue;
                                        }
                                    }
                                }
                            } else if is_end && tx_state != TxState::Idle {
                                let is_commit = matches!(upper_first.as_str(), "COMMIT" | "END");

                                // DW-1 2PC: Handle COMMIT/ROLLBACK with two-phase commit protocol.
                                if two_phase_active {
                                    tx_state = TxState::Idle;
                                    pin_tracker.on_end();

                                    let two_phase_resp = if is_commit {
                                        if two_phase_doomed {
                                            if let (Some(auth), Some(sec)) = (pinned_conn.as_mut(), pinned_conn_secondary.as_mut()) {
                                                handle_two_phase_rollback(auth, sec, &ctx).await;
                                            }
                                            log_warn!(
                                                ctx.clone(),
                                                "2PC: COMMIT aborted — secondary had errors (extended query)",
                                                audience = LogAudience::Internal,
                                                connection_id = connection_id
                                            );
                                            let err = ErrorResponse::simple(
                                                "ERROR",
                                                "40000",
                                                "2PC transaction rollback: secondary database error during transaction",
                                            );
                                            let mut buf = BytesMut::new();
                                            buf.extend_from_slice(&err.encode());
                                            buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                            buf.freeze()
                                        } else if let (Some(auth), Some(sec)) = (pinned_conn.as_mut(), pinned_conn_secondary.as_mut()) {
                                            let gid = format!("eden_2pc_{}_{}", connection_id, two_phase_tx_counter);
                                            two_phase_tx_counter += 1;
                                            match handle_two_phase_commit(auth, sec, &gid, &ctx).await {
                                                Ok(()) => {
                                                    log_info!(
                                                        ctx.clone(),
                                                        "2PC: transaction committed on both databases (extended query)",
                                                        audience = LogAudience::Internal,
                                                        connection_id = connection_id,
                                                        gid = gid.as_str()
                                                    );
                                                    let mut buf = BytesMut::new();
                                                    buf.extend_from_slice(&build_command_complete_msg("COMMIT"));
                                                    buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                                    buf.freeze()
                                                }
                                                Err(e) => {
                                                    log_error!(
                                                        ctx.clone(),
                                                        "2PC: commit failed (extended query)",
                                                        audience = LogAudience::Internal,
                                                        connection_id = connection_id,
                                                        gid = gid.as_str(),
                                                        error = e.as_str()
                                                    );
                                                    let err = ErrorResponse::simple("ERROR", "40000", &format!("2PC commit failed: {}", e));
                                                    let mut buf = BytesMut::new();
                                                    buf.extend_from_slice(&err.encode());
                                                    buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                                    buf.freeze()
                                                }
                                            }
                                        } else {
                                            let err = ErrorResponse::simple("ERROR", "XX000", "2PC internal error: missing connections");
                                            let mut buf = BytesMut::new();
                                            buf.extend_from_slice(&err.encode());
                                            buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                            buf.freeze()
                                        }
                                    } else {
                                        // ROLLBACK — send to both connections.
                                        if let (Some(auth), Some(sec)) = (pinned_conn.as_mut(), pinned_conn_secondary.as_mut()) {
                                            handle_two_phase_rollback(auth, sec, &ctx).await;
                                        }
                                        let mut buf = BytesMut::new();
                                        buf.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
                                        buf.extend_from_slice(&TxState::Idle.ready_for_query());
                                        buf.freeze()
                                    };

                                    cancel_registry_clear(client_pid, client_secret);
                                    two_phase_active = false;
                                    two_phase_doomed = false;
                                    pinned_conn = None;
                                    pinned_conn_secondary = None;
                                    pin_tracker.release();

                                    total_bytes_written += two_phase_resp.len() as u64;
                                    if sender.send(two_phase_resp).is_err() {
                                        return;
                                    }
                                    continue;
                                }

                                pin_tracker.on_end();

                                // DW-8: On COMMIT, replay buffered writes to secondary.
                                // On ROLLBACK, discard the buffer.
                                if dw_tx_buffer.is_active() {
                                    if is_commit {
                                        let (session_cmds, writes, secondary_ep) = dw_tx_buffer.drain();
                                        if let Some(rq) = replay_queue.as_ref()
                                            && let Some(sec_ep) = secondary_ep
                                            && !writes.is_empty()
                                        {
                                            let entry = ReplayEntry::new_transaction_batch(session_cmds, writes, sec_ep);
                                            rq.enqueue(entry, &ctx).await;
                                        }
                                    } else {
                                        dw_tx_buffer.discard();
                                    }
                                }
                            }

                            // DW-16: Track savepoints in extended query path.
                            if dw_tx_buffer.is_active() {
                                if upper_first == "SAVEPOINT" {
                                    dw_tx_buffer.on_savepoint();
                                } else if upper_first.starts_with("RELEASE") {
                                    dw_tx_buffer.on_release_savepoint();
                                } else if upper_first.starts_with("ROLLBACK") && sql_owned.trim().to_ascii_uppercase().contains(" TO ") {
                                    dw_tx_buffer.on_rollback_to_savepoint();
                                }
                            }

                            let mut command_span = telemetry_wrapper.client_tracer(format!("postgres.query.{}", upper_first));
                            command_span.add_event("query received", vec![FastSpanAttribute::new("sql_type", upper_first.clone())]);

                            let req_type = if sql_trimmed.is_empty() {
                                ReqType::Write
                            } else {
                                classify_sql(sql_trimmed)
                            };
                            let is_write = req_type == ReqType::Write;
                            let raw_batch = extended_buf.split();
                            let policy_routing_start = Instant::now();

                            // DW-20: Buffer write bytes during Replicated-mode transactions.
                            // Skip buffering in 2PC mode — writes go to both connections in real-time.
                            if dw_tx_buffer.is_active() && !two_phase_active && is_write {
                                dw_tx_buffer.push(Bytes::copy_from_slice(&raw_batch));
                            }

                            // DW-15: Buffer session commands for replay context.
                            if dw_tx_buffer.is_active() && !two_phase_active && is_session_command(&sql_owned) {
                                dw_tx_buffer.push_session(Bytes::copy_from_slice(&raw_batch));
                            }
                            telemetry_wrapper.metrics().proxy().record_policy_routing_duration(
                                policy_routing_start.elapsed().as_micros() as u64,
                                &[
                                    ("org_uuid", organization_uuid.as_str()),
                                    ("interlay_uuid", interlay_id_str.as_str()),
                                    ("endpoint_uuid", cached_endpoint_id.as_str()),
                                    ("endpoint_kind", "postgres"),
                                ],
                            );

                            // Execute query via raw wire protocol
                            let query_start = Instant::now();
                            let result: Result<Bytes, eden_core::error::EpError> = if let Some(ref mut client) = pinned_conn {
                                // DW-1 2PC: Mirror queries to secondary using simple query protocol.
                                // We use simple protocol to avoid statement cache complexity.
                                if two_phase_active
                                    && !two_phase_doomed
                                    && !sql_owned.is_empty()
                                    && let Some(ref mut sec_client) = pinned_conn_secondary
                                {
                                    let sec_msg = postgres_core::client::build_query_message(&sql_owned);
                                    match sec_client.send_query_raw(&sec_msg).await {
                                        Ok((resp, _)) => {
                                            if crate::replay_queue::response_has_error(&resp) {
                                                two_phase_doomed = true;
                                                log_warn!(
                                                    ctx.clone(),
                                                    "2PC: secondary query error — transaction doomed (extended query)",
                                                    audience = LogAudience::Internal,
                                                    connection_id = connection_id
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            two_phase_doomed = true;
                                            log_warn!(
                                                ctx.clone(),
                                                "2PC: secondary connection error — transaction doomed (extended query)",
                                                audience = LogAudience::Internal,
                                                error = e.to_string(),
                                                connection_id = connection_id
                                            );
                                        }
                                    }
                                }

                                // Transaction-pinned: rewrite batch using this backend's cache.
                                let backend_id = client.backend_key_data().unwrap_or((0, 0));
                                let rewritten = stmt_cache::rewrite_batch(&raw_batch, &mut client_stmt_map, backend_id);
                                match client.send_query_raw(&rewritten.backend_bytes).await {
                                    Ok((backend_resp, _)) => Ok(stmt_cache::merge_responses(&backend_resp, rewritten.response_slots())),
                                    Err(e) => Err(e),
                                }
                            } else {
                                // Direct routing through the current endpoint.
                                route_extended_query(&self.ep, &routing_state, &raw_batch, &mut client_stmt_map, req_type.clone()).await
                            };

                            let query_duration_us = query_start.elapsed().as_micros() as u64;
                            backend_duration_sum += query_duration_us;
                            backend_command_count += 1;

                            if let Ok(primary_response) = &result
                                && let Some(state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone())
                            {
                                spawn_postgres_mirrors(
                                    &self.ep,
                                    &state,
                                    &organization_uuid,
                                    &interlay_id_label,
                                    req_type,
                                    PostgresBytes::from(Bytes::copy_from_slice(&raw_batch)),
                                    primary_response.clone(),
                                    settings,
                                    &telemetry_wrapper,
                                    &ctx,
                                    Some("extended_query"),
                                );
                            }

                            let response_encode_start = Instant::now();
                            let mut response = BytesMut::new();
                            match result {
                                Ok(response_bytes) => {
                                    command_span.add_event(
                                        "query completed",
                                        vec![
                                            FastSpanAttribute::new("duration_us", query_duration_us as i64),
                                            FastSpanAttribute::new("bytes_written", response_bytes.len() as i64),
                                        ],
                                    );

                                    // Extract transaction status from ReadyForQuery in raw response
                                    if response_bytes.len() >= 6 {
                                        let end = response_bytes.len();
                                        if response_bytes[end - 6] == b'Z' {
                                            tx_state = match response_bytes[end - 1] {
                                                b'I' => TxState::Idle,
                                                b'T' => TxState::InTransaction,
                                                b'E' => TxState::Failed,
                                                _ => tx_state,
                                            };
                                        }
                                    }

                                    // Per-query audit trail
                                    eden_gateway_core::audit::pg_query_record(
                                        &organization_uuid,
                                        &endpoint_uuid,
                                        &upper_first,
                                        query_duration_us,
                                        true,
                                        &service_name,
                                        Some(&client_ip),
                                        connection_id,
                                    );

                                    // Strip ELS SET CommandComplete messages before sending to client
                                    let client_response = if extended_els_set_count > 0 {
                                        strip_leading_command_completes(response_bytes, extended_els_set_count)
                                    } else {
                                        response_bytes
                                    };
                                    response.extend_from_slice(&client_response);
                                }
                                Err(e) => {
                                    if tx_state == TxState::InTransaction {
                                        tx_state = TxState::Failed;
                                    }
                                    command_span.add_event("query error", vec![FastSpanAttribute::new("error", e.to_string())]);
                                    log_error!(
                                        ctx.clone(),
                                        "PG query error",
                                        audience = LogAudience::Internal,
                                        error = e.to_string(),
                                        sql = &sql_owned
                                    );

                                    // Per-query audit trail (error)
                                    eden_gateway_core::audit::pg_query_record(
                                        &organization_uuid,
                                        &endpoint_uuid,
                                        &upper_first,
                                        query_duration_us,
                                        false,
                                        &service_name,
                                        Some(&client_ip),
                                        connection_id,
                                    );

                                    response.extend_from_slice(&ErrorResponse::simple("ERROR", "XX000", &e.to_string()).encode());

                                    telemetry_wrapper.record(MetricEvent::ProxyError {
                                        org_uuid: organization_uuid.as_str(),
                                        interlay_uuid: &interlay_id_str,
                                        error_type: "command_error",
                                    });
                                }
                            }

                            if !response_has_ready_for_query(&response) {
                                response.extend_from_slice(&tx_state.ready_for_query());
                            }
                            telemetry_wrapper.metrics().proxy().record_response_encode_duration(
                                response_encode_start.elapsed().as_micros() as u64,
                                &[
                                    ("org_uuid", organization_uuid.as_str()),
                                    ("interlay_uuid", interlay_id_str.as_str()),
                                    ("endpoint_uuid", cached_endpoint_id.as_str()),
                                    ("endpoint_kind", "postgres"),
                                ],
                            );

                            // DW-18: Undo BEGIN pin/buffer if the BEGIN batch failed.
                            // If we set up pinning for BEGIN but the backend remained Idle
                            // (BEGIN failed), release the pin and discard the buffer.
                            if is_begin && tx_state == TxState::Idle {
                                pin_tracker.on_end();
                                dw_tx_buffer.discard();
                                // 2PC: Clean up secondary connection if BEGIN failed.
                                if two_phase_active {
                                    cancel_registry_clear(client_pid, client_secret);
                                    cleanup_2pc_conn(&mut pinned_conn_secondary, false, &ctx).await;
                                    two_phase_active = false;
                                    two_phase_doomed = false;
                                }
                            }

                            let resp_bytes = response.freeze();
                            total_bytes_written += resp_bytes.len() as u64;
                            if sender.send(resp_bytes).is_err() {
                                telemetry_wrapper.metrics().proxy().record_bridge_enqueue_rejection(&[
                                    ("org_uuid", organization_uuid.as_str()),
                                    ("interlay_uuid", interlay_id_str.as_str()),
                                    ("endpoint_uuid", cached_endpoint_id.as_str()),
                                    ("endpoint_kind", "postgres"),
                                    ("queue", "response"),
                                    ("reason", "queue_full_or_closed"),
                                ]);
                                return;
                            }

                            // Release pinned connection at transaction end
                            if tx_state == TxState::Idle && pin_tracker.should_release() && pinned_conn.is_some() {
                                cancel_registry_clear(client_pid, client_secret);
                                pinned_conn = None;
                                pin_tracker.release();
                            }

                            extended_sql = None;
                            extended_statement_name = None;
                            extended_request_bytes = 0;
                            extended_els_injected = false;
                            extended_els_set_count = 0;
                        }

                        // DW-9: COPY protocol passthrough.
                        // CopyData/CopyDone/CopyFail are forwarded to the pinned connection
                        // (which was established when the COPY SQL was processed via Q message).
                        // During dual-write, COPY data is buffered and replayed to the secondary
                        // on CopyDone via the ReplayQueue.
                        MSG_COPY_DATA | MSG_COPY_DONE | MSG_COPY_FAIL => {
                            if let Some(ref mut client) = pinned_conn {
                                // Forward COPY message to pinned connection.
                                let copy_bytes = msg_bytes.freeze();
                                match client.send_query_raw(&copy_bytes).await {
                                    Ok((resp, _)) => {
                                        let resp_len = resp.len() as u64;
                                        total_bytes_written += resp_len;
                                        if !resp.is_empty() && sender.send(resp).is_err() {
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        log_error!(
                                            ctx.clone(),
                                            "COPY forwarding failed on pinned connection",
                                            audience = LogAudience::Internal,
                                            error = e.to_string()
                                        );
                                        let err = ErrorResponse::simple("ERROR", "08006", &format!("COPY forwarding failed: {}", e));
                                        let mut resp = BytesMut::new();
                                        resp.extend_from_slice(&err.encode());
                                        resp.extend_from_slice(&tx_state.ready_for_query());
                                        let resp_bytes = resp.freeze();
                                        total_bytes_written += resp_bytes.len() as u64;
                                        if sender.send(resp_bytes).is_err() {
                                            return;
                                        }
                                    }
                                }
                            } else {
                                // No pinned connection — COPY not supported outside transactions/pinned.
                                let err =
                                    ErrorResponse::simple("ERROR", "0A000", "COPY protocol requires a transaction context in Eden gateway");
                                let mut resp = BytesMut::new();
                                resp.extend_from_slice(&err.encode());
                                resp.extend_from_slice(&tx_state.ready_for_query());
                                let resp_bytes = resp.freeze();
                                total_bytes_written += resp_bytes.len() as u64;
                                if sender.send(resp_bytes).is_err() {
                                    return;
                                }
                            }
                        }

                        _ => {
                            log_trace!(
                                ctx.clone(),
                                "PG unknown message type, ignoring",
                                audience = LogAudience::Internal,
                                msg_type = msg_type
                            );
                        }
                    }
                }

                // Record batch telemetry
                if command_count > 0 {
                    let batch_duration_us = batch_start.elapsed().as_micros() as u64;
                    let comparable_duration_us = comparable_request_duration_us(batch_duration_us, command_count, backend_command_count);
                    let endpoint_duration_us = comparable_endpoint_duration_us(backend_duration_sum, command_count, backend_command_count);
                    let overhead_us =
                        comparable_overhead_duration_us(batch_duration_us, backend_duration_sum, command_count, backend_command_count);
                    proxy_series.record_batch(ProxyBatchRecord {
                        duration_us: batch_duration_us,
                        comparable_duration_us,
                        endpoint_duration_us,
                        overhead_us,
                        bytes_read,
                        bytes_written: total_bytes_written,
                        command_count,
                    });
                }
            }

            // DW-17: Gracefully drain replay queue before aborting worker.
            if let Some(rq) = replay_queue.as_ref() {
                let remaining = rq.drain_remaining().await;
                if !remaining.is_empty() {
                    log_warn!(
                        ctx.clone(),
                        "DW-17: replay queue drained on connection close — entries lost",
                        audience = LogAudience::Internal,
                        entries = remaining.len(),
                        connection_id = connection_id
                    );
                }
            }
            // Abort background workers on connection close.
            if let Some(handle) = replay_worker_handle.take() {
                handle.abort(); // DW-2: replay worker
            }
            ws_gc_handle.abort(); // DW-4: write serializer GC

            // Connection lifecycle: log close
            log_info!(
                ctx.clone(),
                "PG proxy processor stopped",
                audience = LogAudience::Internal,
                connection_id = connection_id
            );
            // Proxy connection close is counted by the interlay listener guard
            // at the single canonical accept-task boundary.
        })
    }
}

/// Route and execute a SQL query through the current endpoint.
#[allow(clippy::too_many_arguments, unused_variables)]
async fn route_query(
    ep: &PostgresEp,
    interlay_uuid: &str,
    routing_state: &RoutingState,
    pg_bytes: PostgresBytes,
    sql: &str,
    req_type: ReqType,
    replay_queue: Option<&ReplayQueue>,
    session_affinity: &mut SessionAffinityTracker,
    write_serializer: &WriteSerializer,
    settings: EdenSettings,
    telemetry_wrapper: &mut TelemetryWrapper,
    ctx: &LogContext,
) -> ResultEP<Bytes> {
    ep.raw_bytes_with_req_type(&routing_state.endpoint, pg_bytes, req_type, settings, telemetry_wrapper).await
}

// ──────────────────────────────────────────────────────────────────────────────
// Extended Query Routing
// ──────────────────────────────────────────────────────────────────────────────

/// Execute an extended query batch on a single target endpoint.
///
/// Acquires a raw connection, rewrites statement names via the stmt cache,
/// sends the batch, and returns the merged response.
async fn execute_extended_single(
    ep: &PostgresEp,
    target: &EndpointCacheUuid,
    raw_batch: &[u8],
    client_stmt_map: &mut ClientStmtMap,
    req_type: ReqType,
) -> ResultEP<Bytes> {
    let mut conn = ep.raw_connection(target, req_type.clone()).await?;
    let backend_id = conn.backend_key_data().unwrap_or((0, 0));
    let rewritten = stmt_cache::rewrite_batch(raw_batch, client_stmt_map, backend_id);
    match conn.send_query_raw(&rewritten.backend_bytes).await {
        Ok((backend_resp, _)) => {
            let merged = stmt_cache::merge_responses(&backend_resp, rewritten.response_slots());
            // DW-7: If the response contains a schema-mismatch error, invalidate
            // the backend statement cache and retry once on a fresh connection.
            // This handles cases where DDL altered the target schema after the
            // statement was originally cached.
            if stmt_cache::has_schema_mismatch_error(&merged) {
                stmt_cache::invalidate_backend_cache(backend_id);
                drop(conn);
                let mut conn2 = ep.raw_connection(target, req_type).await?;
                let backend_id2 = conn2.backend_key_data().unwrap_or((0, 0));
                let rewritten2 = stmt_cache::rewrite_batch(raw_batch, client_stmt_map, backend_id2);
                match conn2.send_query_raw(&rewritten2.backend_bytes).await {
                    Ok((resp2, _)) => Ok(stmt_cache::merge_responses(&resp2, rewritten2.response_slots())),
                    Err(e) => {
                        stmt_cache::invalidate_backend_cache(backend_id2);
                        Err(e)
                    }
                }
            } else {
                Ok(merged)
            }
        }
        Err(e) => {
            stmt_cache::invalidate_backend_cache(backend_id);
            Err(e)
        }
    }
}

/// Route and execute an extended query batch through the current endpoint.
async fn route_extended_query(
    ep: &PostgresEp,
    routing_state: &RoutingState,
    raw_batch: &[u8],
    client_stmt_map: &mut ClientStmtMap,
    eq_req_type: ReqType,
) -> ResultEP<Bytes> {
    execute_extended_single(ep, &routing_state.endpoint, raw_batch, client_stmt_map, eq_req_type).await
}
