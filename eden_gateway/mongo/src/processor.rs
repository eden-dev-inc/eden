//! MongoDB proxy processor.
//!
//! Handles the MongoDB wire protocol at the proxy layer:
//! - Message framing (16-byte header + body)
//! - OP_MSG command classification and direct endpoint routing
//! - Session tracking for transaction pinning
//! - Command policy enforcement with audit recording
//! - Signal handling for graceful shutdown and mirror updates

use crate::policy::{CommandGuardConfig, PolicyEnforcementMode, apply_policy};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::{CacheUuid, EndpointUuid, InterlayUuid, OrganizationUuid};
use eden_core::telemetry::metric_event::{MetricEvent, RecordMetric};
use eden_core::telemetry::metrics::ProxyBatchRecord;
use eden_core::telemetry::{FastSpanAttribute, TelemetryWrapper};
use eden_gateway_core::audit::blocked_record;
use eden_gateway_core::response::{GatewayMirrorResponseMode, GatewayResponsePolicySpec, GatewayResponseProfile};
use eden_gateway_core::traits::{BytesQueueSender, DatabaseProtocolProcessor, ProxyRequestChunk};
use eden_logger_internal::{LogAudience, LogContext, log_debug, log_error, log_info, log_trace, log_warn};
use endpoints::endpoint::mongo::ep::MongoEp;
use ep_core::GetPool;
use ep_core::ReqType;
use ep_core::database::schema::interlay::{InterlaySignal, InterlayState};
use ep_core::settings::EdenSettings;
use mongo_wire::{MessageHeader, OpCode, OpMsg, OpMsgSection, OpQuery, SliceStream};
use mongodb::bson::{self, Bson, Document};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc::UnboundedReceiver;

const MAX_MONGO_MESSAGE_SIZE: usize = 48 * 1024 * 1024;
const SLOW_GATEWAY_OP_LOG_THRESHOLD_US: u64 = 10_000;

/// Global connection counter for unique connection IDs.
static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Read commands in MongoDB
const READ_COMMANDS: &[&str] = &[
    "find",
    "aggregate",
    "count",
    "countdocuments",
    "distinct",
    "listcollections",
    "listdatabases",
    "listindexes",
    "getmore",
    "explain",
    "collstats",
    "dbstats",
    "serverstatus",
    "buildinfo",
    "connectionstatus",
    "ismaster",
    "hello",
    "ping",
    "whatsmyuri",
    "getlasterror",
    "getlog",
    "hostinfo",
    "features",
    "loglevel",
    "getfreemonitoringstatus",
    "getclustertime",
    "replsetgetstatus",
    "saslstart",
    "saslcontinue",
];

/// Write commands in MongoDB
const WRITE_COMMANDS: &[&str] = &[
    "insert",
    "update",
    "delete",
    "findandmodify",
    "create",
    "drop",
    "dropdatabase",
    "createindexes",
    "dropindexes",
    "renamecollection",
    "converttocapped",
    "applyops",
];

/// Classify a MongoDB command as read, write, or admin
fn classify_command(command_name: &str) -> ReqType {
    let lower = command_name.to_lowercase();
    if WRITE_COMMANDS.contains(&lower.as_str()) {
        ReqType::Write
    } else if READ_COMMANDS.contains(&lower.as_str()) {
        ReqType::Read
    } else {
        // Unknown commands default to write for safety
        ReqType::Write
    }
}

fn mongo_req_type_label(req_type: &ReqType) -> &'static str {
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

/// Parse an OP_MSG body using the `mongo_wire` parser.
///
/// Handles section kind 0 (body) and kind 1 (document sequence), checksums,
/// and flag validation.
fn parse_op_msg_sections(body: &[u8]) -> Option<OpMsg> {
    let stream = SliceStream::new(body);
    OpMsg::parse_sync(&stream, body.len()).ok()
}

/// Extract the command name from an OP_MSG body section.
fn extract_op_msg_command_name(body: &[u8]) -> Option<String> {
    let msg = parse_op_msg_sections(body)?;
    let body_doc = msg.body()?;
    let doc = bson::from_slice::<Document>(body_doc).ok()?;
    doc.keys().next().map(|k| k.to_string())
}

/// Parse OP_MSG body into a command document and database name.
///
/// Merges section kind 1 document sequences (e.g. `documents` for insert,
/// `updates` for update, `deletes` for delete) back into the body document
/// so that `run_command` receives a complete command.
fn parse_op_msg_command(body: &[u8]) -> Option<(String, Document)> {
    let msg = parse_op_msg_sections(body)?;
    let body_doc = msg.body()?;
    let mut doc = bson::from_slice::<Document>(body_doc).ok()?;

    // Merge document sequence sections into the command document.
    for section in &msg.sections {
        if let OpMsgSection::DocumentSequence { identifier, documents } = section {
            let bson_array: Vec<Bson> =
                documents.iter().filter_map(|raw| bson::from_slice::<Document>(raw).ok()).map(Bson::Document).collect();
            doc.insert(identifier.clone(), Bson::Array(bson_array));
        }
    }

    let db_name = doc
        .remove("$db")
        .and_then(|v| if let Bson::String(s) = v { Some(s) } else { None })
        .unwrap_or_else(|| "admin".to_string());

    Some((db_name, doc))
}

/// Parse an OP_QUERY body using the `mongo_wire` parser.
fn parse_op_query_sections(body: &[u8]) -> Option<OpQuery> {
    let stream = SliceStream::new(body);
    OpQuery::parse_sync(&stream, body.len()).ok()
}

/// Extract the command name from an OP_QUERY body.
fn extract_op_query_command_name(body: &[u8]) -> Option<String> {
    let query = parse_op_query_sections(body)?;
    let doc = bson::from_slice::<Document>(&query.query).ok()?;
    doc.keys().next().map(|k| k.to_string())
}

/// Parse OP_QUERY body into a command document and database name.
fn parse_op_query_command(body: &[u8]) -> Option<(String, Document)> {
    let query = parse_op_query_sections(body)?;
    let doc = bson::from_slice::<Document>(&query.query).ok()?;
    let db_name = query.split_collection_name().map(|(db, _)| db.to_string()).unwrap_or_else(|| "admin".to_string());
    Some((db_name, doc))
}

/// Extract the command name from the body, dispatching by opcode.
fn extract_command_name(op_code: Option<OpCode>, body: &[u8]) -> Option<String> {
    match op_code {
        Some(OpCode::Msg) => extract_op_msg_command_name(body),
        Some(OpCode::Query) => extract_op_query_command_name(body),
        _ => None,
    }
}

/// Parse a command document and database name from the body, dispatching by opcode.
fn parse_command(op_code: Option<OpCode>, body: &[u8]) -> Option<(String, Document)> {
    match op_code {
        Some(OpCode::Msg) => parse_op_msg_command(body),
        Some(OpCode::Query) => parse_op_query_command(body),
        _ => None,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing State
// ──────────────────────────────────────────────────────────────────────────────

/// Current routing state, derived from the interlay endpoints cache.
struct RoutingState {
    endpoint: EndpointCacheUuid,
}

impl RoutingState {
    fn from_interlay_state(state: &InterlayState) -> Self {
        let endpoint = state.endpoint_uuid().clone();

        Self { endpoint }
    }
}

/// MongoDB protocol processor implementing the `DatabaseProtocolProcessor` trait.
pub struct MongoProtocolProcessor {
    ep: MongoEp,
}

impl MongoProtocolProcessor {
    pub fn new(ep: MongoEp) -> Self {
        Self { ep }
    }
}

impl GatewayResponseProfile for MongoProtocolProcessor {
    type Observer = ();

    fn response_policy_spec(&self) -> GatewayResponsePolicySpec {
        GatewayResponsePolicySpec::new("mongo", Some(GatewayMirrorResponseMode::CompareResponse))
    }
}

impl DatabaseProtocolProcessor for MongoProtocolProcessor {
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
        _listener_id: String,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(process_mongo_unified(
            &self.ep,
            receiver,
            sender,
            settings,
            interlay_cache_uuid,
            interlay_endpoints,
            telemetry_wrapper,
            ctx,
            client_addr,
        ))
    }
}

/// Forward a command to the MongoDB endpoint using `run_command`.
async fn forward_to_mongo(
    ep: &MongoEp,
    target_endpoint: &EndpointCacheUuid,
    interlay_uuid: &str,
    req_type: ReqType,
    body: &[u8],
    op_code: Option<OpCode>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Document> {
    let (db_name, mut cmd_doc) =
        parse_command(op_code, body).ok_or_else(|| EpError::parse("Failed to parse MongoDB command from message body"))?;

    // Strip fields that conflict with the driver's session/handshake on pool connections.
    // The proxy's driver manages its own sessions and handshake metadata.
    for key in &["client", "compression", "lsid", "txnNumber", "$clusterTime", "$readPreference"] {
        cmd_doc.remove(*key);
    }
    let command_name = cmd_doc.keys().next().cloned().unwrap_or_else(|| "unknown".to_string());

    let pool = match req_type {
        ReqType::Read => ep.pool().read_conn_async(target_endpoint).await?,
        ReqType::Write => ep.pool().write_conn_async(target_endpoint).await?,
    };

    let endpoint_uuid = target_endpoint.uuid().to_string();
    let pool_wait_start = Instant::now();
    let client_result = {
        let mut pool_span = telemetry_wrapper.start_client_span("mongo.backend.pool_checkout");
        pool_span.add_event(
            "checking out MongoDB client",
            vec![
                FastSpanAttribute::new("endpoint_uuid", endpoint_uuid.clone()),
                FastSpanAttribute::new("req_type", format!("{:?}", req_type)),
                FastSpanAttribute::new("command", command_name.clone()),
            ],
        );
        let result = pool.get().await.map_err(|e| EpError::parse(format!("Failed to get MongoDB client: {}", e)));
        let duration_us = pool_wait_start.elapsed().as_micros() as u64;
        pool_span.add_event(
            "MongoDB pool checkout completed",
            vec![FastSpanAttribute::new("duration_us", duration_us.to_string())],
        );
        if let Err(err) = &result {
            pool_span.add_event("MongoDB pool checkout failed", vec![FastSpanAttribute::new("error", err.to_string())]);
        }
        if let Some(org_uuid) = target_endpoint.org() {
            let org_uuid_label = org_uuid.eden_uuid::<OrganizationUuid>().to_string();
            telemetry_wrapper.metrics().proxy().record_backend_pool_wait(
                duration_us,
                &[
                    ("org_uuid", org_uuid_label.as_str()),
                    ("interlay_uuid", interlay_uuid),
                    ("endpoint_uuid", endpoint_uuid.as_str()),
                    ("endpoint_kind", "mongo"),
                ],
            );
        }
        result
    };
    let client = client_result?;

    let db = client.database(&db_name);
    let response = {
        let mut backend_span = telemetry_wrapper.start_client_span("mongo.backend.run_command");
        backend_span.add_event(
            "running MongoDB command",
            vec![
                FastSpanAttribute::new("endpoint_uuid", endpoint_uuid),
                FastSpanAttribute::new("db.system", "mongodb"),
                FastSpanAttribute::new("db.operation.name", command_name),
                FastSpanAttribute::new("req_type", format!("{:?}", req_type)),
            ],
        );
        let backend_start = Instant::now();
        let result = db.run_command(cmd_doc, None).await.map_err(|e| EpError::parse(format!("MongoDB command failed: {}", e)));
        let duration_us = backend_start.elapsed().as_micros() as u64;
        backend_span.add_event("MongoDB command completed", vec![FastSpanAttribute::new("duration_us", duration_us.to_string())]);
        if let Err(err) = &result {
            backend_span.add_event("MongoDB command failed", vec![FastSpanAttribute::new("error", err.to_string())]);
        }
        result?
    };

    Ok(response)
}

fn is_mongo_mirror_unsafe_request(op_code: Option<OpCode>, body: &[u8], command_name: Option<&str>) -> bool {
    let Some(command_name) = command_name else {
        return true;
    };
    let command_name = command_name.to_ascii_lowercase();
    if matches!(command_name.as_str(), "committransaction" | "aborttransaction" | "startsession" | "endsessions") {
        return true;
    }

    parse_command(op_code, body)
        .map(|(_, doc)| {
            matches!(doc.get("startTransaction"), Some(Bson::Boolean(true))) || matches!(doc.get("autocommit"), Some(Bson::Boolean(false)))
        })
        .unwrap_or(true)
}

#[allow(clippy::too_many_arguments)]
fn record_mongo_mirror_skip(
    telemetry_wrapper: &TelemetryWrapper,
    organization_uuid: &str,
    interlay_id: &str,
    primary_endpoint_uuid: &str,
    mirror_endpoint_uuid: &str,
    endpoint_kind: &str,
    req_type: &ReqType,
    reason: &str,
) {
    let req_type = mongo_req_type_label(req_type);
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
fn spawn_mongo_mirrors(
    ep: &MongoEp,
    state: &InterlayState,
    organization_uuid: &str,
    interlay_id: &Arc<str>,
    req_type: ReqType,
    body: Bytes,
    op_code: Option<OpCode>,
    primary_response: Document,
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
    let req_type_label = mongo_req_type_label(&req_type);

    if let Some(reason) = skip_reason {
        for mirror_target in state.mirror_targets() {
            record_mongo_mirror_skip(
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
            record_mongo_mirror_skip(
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
            record_mongo_mirror_skip(
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
        let body = body.clone();
        let primary_response = primary_response.clone();
        let req_type = req_type.clone();

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
            let result = forward_to_mongo(&ep, &mirror_endpoint, &interlay_id, req_type, body.as_ref(), op_code, &mut mirror_tw).await;
            mirror_tw.metrics().proxy().record_mirror_latency(start.elapsed().as_micros() as u64, &labels);

            match result {
                Ok(mirror_response) => {
                    if primary_response != mirror_response {
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
                        "MongoDB mirror dispatch failed",
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

/// Main MongoDB processing loop with unified routing.
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
async fn process_mongo_unified(
    ep: &MongoEp,
    mut receiver: UnboundedReceiver<ProxyRequestChunk>,
    sender: BytesQueueSender,
    _settings: EdenSettings,
    interlay_cache_uuid: InterlayCacheUuid,
    interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
    mut telemetry_wrapper: TelemetryWrapper,
    ctx: LogContext,
    client_addr: std::net::SocketAddr,
) {
    let connection_id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let client_ip = client_addr.ip().to_string();
    let interlay_id_str = interlay_cache_uuid.uuid().to_string();
    let interlay_uuid_value = interlay_cache_uuid.eden_uuid::<InterlayUuid>();
    let interlay_id_label: Arc<str> = Arc::from(interlay_id_str.as_str());

    // Extract organization and endpoint info for analytics.
    let (organization_uuid_value, endpoint_uuid) = interlay_endpoints
        .get(&interlay_cache_uuid)
        .map(|state| {
            let organization_uuid = state.endpoint_uuid().org().map(|org| org.eden_uuid::<OrganizationUuid>()).unwrap_or_default();
            let ep_uuid = state.endpoint_uuid().eden_uuid::<EndpointUuid>().clone();
            (organization_uuid, ep_uuid)
        })
        .unwrap_or_else(|| (OrganizationUuid::default(), EndpointUuid::default()));
    let organization_uuid = organization_uuid_value.to_string();
    let initial_endpoint_id = endpoint_uuid.to_string();

    log_info!(
        ctx.clone(),
        "MongoDB proxy connection established",
        audience = LogAudience::Internal,
        connection_id = connection_id,
        client_addr = client_addr.to_string()
    );

    // Subscribe to shutdown and mirror update signals if available.
    let mut signal_rx = interlay_endpoints.get(&interlay_cache_uuid).and_then(|state| state.signal_tx().map(|tx| tx.subscribe()));

    // Load initial policy config
    let policy_config = interlay_endpoints
        .get(&interlay_cache_uuid)
        .and_then(|state| state.command_policy_value().and_then(|v| serde_json::from_value::<CommandGuardConfig>(v.clone()).ok()))
        .unwrap_or_default();

    let policy_mode = if policy_config.presets.is_empty() && policy_config.blocked_commands.is_empty() {
        PolicyEnforcementMode::Observe
    } else {
        PolicyEnforcementMode::Block
    };

    if interlay_cache_uuid.org().is_none() {
        log_error!(
            ctx.clone(),
            "InterlayCacheUuid missing organization UUID",
            audience = LogAudience::Internal,
            connection_id = connection_id
        );
        return;
    }

    // Buffer for accumulating partial messages
    let mut buffer = BytesMut::new();

    loop {
        // Check for signals (mirror updates, shutdown).
        if let Some(ref mut rx) = signal_rx {
            while let Ok(signal) = rx.try_recv() {
                match signal {
                    InterlaySignal::Shutdown => {
                        log_info!(
                            ctx.clone(),
                            "Received shutdown signal, closing MongoDB proxy connection",
                            audience = LogAudience::Internal,
                            connection_id = connection_id
                        );
                        return;
                    }
                    InterlaySignal::MirrorUpdate => {
                        log_debug!(
                            ctx.clone(),
                            "Received mirror update signal for MongoDB proxy connection",
                            audience = LogAudience::Internal,
                            connection_id = connection_id
                        );
                    }
                }
            }
        }

        // Receive data from client
        let data = match receiver.recv().await {
            Some(data) => data,
            None => {
                log_debug!(
                    ctx.clone(),
                    "Client channel closed, ending MongoDB proxy connection",
                    audience = LogAudience::Internal,
                    connection_id = connection_id
                );
                return;
            }
        };
        let request_queue_wait_us = data.queue_wait_us();
        telemetry_wrapper.metrics().proxy().record_bridge_request_queue(
            request_queue_wait_us,
            &[
                ("org_uuid", organization_uuid.as_str()),
                ("interlay_uuid", interlay_id_str.as_str()),
                ("endpoint_uuid", initial_endpoint_id.as_str()),
                ("endpoint_kind", "mongo"),
            ],
        );
        let data = data.into_bytes();

        buffer.extend_from_slice(&data);

        // Process all complete messages in the buffer
        while buffer.len() >= MessageHeader::SIZE {
            // Peek at the message length from the header
            let msg_length = i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

            if msg_length < MessageHeader::SIZE as i32 {
                log_error!(
                    ctx.clone(),
                    "Invalid MongoDB message length",
                    audience = LogAudience::Internal,
                    connection_id = connection_id,
                    msg_length = msg_length
                );
                return;
            }

            let msg_length = msg_length as usize;
            if msg_length > MAX_MONGO_MESSAGE_SIZE {
                log_error!(
                    ctx.clone(),
                    "MongoDB message exceeds maximum supported size",
                    audience = LogAudience::Internal,
                    connection_id = connection_id,
                    msg_length = msg_length,
                    max_msg_length = MAX_MONGO_MESSAGE_SIZE
                );
                return;
            }

            // Wait for the complete message
            if buffer.len() < msg_length {
                break;
            }

            // Extract the complete message
            let msg_bytes = buffer.split_to(msg_length).freeze();
            let request_start = Instant::now();
            let parse_start = Instant::now();

            // Parse header
            let header_bytes = &msg_bytes[..MessageHeader::SIZE];
            let header = MessageHeader {
                message_length: i32::from_le_bytes([header_bytes[0], header_bytes[1], header_bytes[2], header_bytes[3]]),
                request_id: i32::from_le_bytes([header_bytes[4], header_bytes[5], header_bytes[6], header_bytes[7]]),
                response_to: i32::from_le_bytes([header_bytes[8], header_bytes[9], header_bytes[10], header_bytes[11]]),
                op_code: i32::from_le_bytes([header_bytes[12], header_bytes[13], header_bytes[14], header_bytes[15]]),
            };

            let body = &msg_bytes[MessageHeader::SIZE..];

            let op_code = header.op_code();
            let (command_name, req_type) = match extract_command_name(op_code, body) {
                Some(name) => {
                    let rt = classify_command(&name);
                    (Some(name), rt)
                }
                None => (None, ReqType::Write),
            };
            telemetry_wrapper.metrics().proxy().record_parse_duration(
                parse_start.elapsed().as_micros() as u64,
                &[
                    ("org_uuid", organization_uuid.as_str()),
                    ("interlay_uuid", interlay_id_str.as_str()),
                    ("endpoint_uuid", initial_endpoint_id.as_str()),
                    ("endpoint_kind", "mongo"),
                ],
            );

            telemetry_wrapper.reset_trace_context();
            let command_span_name = format!("mongo.command.{}", command_name.as_deref().unwrap_or("unknown"));
            let mut command_span = telemetry_wrapper.client_tracer(command_span_name);
            command_span.add_event(
                "MongoDB command received",
                vec![
                    FastSpanAttribute::new("command", command_name.as_deref().unwrap_or("unknown").to_string()),
                    FastSpanAttribute::new("req_type", format!("{:?}", req_type)),
                    FastSpanAttribute::new("op_code", format!("{:?}", op_code)),
                    FastSpanAttribute::new("bytes_read", msg_length.to_string()),
                    FastSpanAttribute::new("queue_wait_us", request_queue_wait_us.to_string()),
                ],
            );
            if request_queue_wait_us >= SLOW_GATEWAY_OP_LOG_THRESHOLD_US {
                log_debug!(
                    ctx.clone(),
                    "MongoDB request waited in bridge queue",
                    audience = LogAudience::Internal,
                    connection_id = connection_id,
                    command = command_name.as_deref().unwrap_or("unknown"),
                    req_type = format!("{:?}", req_type),
                    queue_wait_us = request_queue_wait_us
                );
            }

            log_trace!(
                ctx.clone(),
                "MongoDB message received",
                audience = LogAudience::Internal,
                connection_id = connection_id,
                op_code = header.op_code,
                command = command_name.as_deref().unwrap_or("unknown"),
                req_type = format!("{:?}", req_type),
                msg_length = header.message_length
            );

            let policy_routing_start = Instant::now();

            // Apply policy check
            if let Some(ref cmd) = command_name
                && let Some(reason) = apply_policy(cmd, &policy_config, policy_mode, &ctx)
            {
                log_warn!(
                    ctx.clone(),
                    "MongoDB command blocked by policy",
                    audience = LogAudience::Internal,
                    connection_id = connection_id,
                    command = cmd.as_str(),
                    reason = reason.as_str()
                );

                // Record to telemetry
                let ep_id_str = endpoint_uuid.to_string();
                telemetry_wrapper.record(MetricEvent::PolicyBlocked {
                    org_uuid: organization_uuid.as_str(),
                    endpoint_id: &ep_id_str,
                    command: cmd,
                    count: 1,
                });

                // Record the blocked command for audit
                if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid) {
                    let ep_uuid = state.endpoint_uuid().eden_uuid::<EndpointUuid>();
                    blocked_record(&interlay_cache_uuid.uuid().to_string(), &ep_uuid, cmd, &reason, 2u8, "mongo", Some(&client_ip));
                }
                telemetry_wrapper.metrics().proxy().record_policy_routing_duration(
                    policy_routing_start.elapsed().as_micros() as u64,
                    &[
                        ("org_uuid", organization_uuid.as_str()),
                        ("interlay_uuid", interlay_id_str.as_str()),
                        ("endpoint_uuid", initial_endpoint_id.as_str()),
                        ("endpoint_kind", "mongo"),
                    ],
                );

                // Send error response back to client
                let response_encode_start = Instant::now();
                let error_response = build_error_response(header.request_id, op_code, &reason);
                telemetry_wrapper.metrics().proxy().record_response_encode_duration(
                    response_encode_start.elapsed().as_micros() as u64,
                    &[
                        ("org_uuid", organization_uuid.as_str()),
                        ("interlay_uuid", interlay_id_str.as_str()),
                        ("endpoint_uuid", initial_endpoint_id.as_str()),
                        ("endpoint_kind", "mongo"),
                    ],
                );
                command_span.add_event(
                    "MongoDB command blocked by policy",
                    vec![
                        FastSpanAttribute::new("reason", reason.clone()),
                        FastSpanAttribute::new("status_code", "403"),
                    ],
                );
                if sender.send(error_response).is_err() {
                    command_span.add_event("MongoDB response queue closed", vec![FastSpanAttribute::new("queue", "response")]);
                    telemetry_wrapper.metrics().proxy().record_bridge_enqueue_rejection(&[
                        ("org_uuid", organization_uuid.as_str()),
                        ("interlay_uuid", interlay_id_str.as_str()),
                        ("endpoint_uuid", initial_endpoint_id.as_str()),
                        ("endpoint_kind", "mongo"),
                        ("queue", "response"),
                        ("reason", "queue_full_or_closed"),
                    ]);
                    return;
                }
                continue;
            }

            // Get routing state
            let routing_state = match interlay_endpoints.get(&interlay_cache_uuid) {
                Some(state) => RoutingState::from_interlay_state(&state),
                None => {
                    log_error!(
                        ctx.clone(),
                        "Interlay state not found in cache",
                        audience = LogAudience::Internal,
                        connection_id = connection_id
                    );
                    return;
                }
            };
            let ep_id_str = routing_state.endpoint.uuid().to_string();
            telemetry_wrapper.metrics().proxy().record_policy_routing_duration(
                policy_routing_start.elapsed().as_micros() as u64,
                &[
                    ("org_uuid", organization_uuid.as_str()),
                    ("interlay_uuid", interlay_id_str.as_str()),
                    ("endpoint_uuid", ep_id_str.as_str()),
                    ("endpoint_kind", "mongo"),
                ],
            );

            // Route the command and forward
            let cmd_start = Instant::now();
            let request_bytes_len = body.len() as u32;

            let forward_result: ResultEP<Document> = forward_to_mongo(
                ep,
                &routing_state.endpoint,
                &interlay_id_str,
                req_type.clone(),
                body,
                op_code,
                &mut telemetry_wrapper,
            )
            .await;

            let cmd_duration_us = cmd_start.elapsed().as_micros() as u64;
            if cmd_duration_us >= SLOW_GATEWAY_OP_LOG_THRESHOLD_US {
                log_debug!(
                    ctx.clone(),
                    "MongoDB command backend path was slow",
                    audience = LogAudience::Internal,
                    connection_id = connection_id,
                    command = command_name.as_deref().unwrap_or("unknown"),
                    req_type = format!("{:?}", req_type),
                    duration_us = cmd_duration_us
                );
            }

            // Capture response counts from the response doc before building the wire response.
            // These are only used on the cold path (sampled events) but extracting them here
            // avoids re-parsing BSON later.
            let response_encode_start = Instant::now();
            let (response, is_success, _response_matched, _response_modified, _response_deleted, _response_upserted, _response_inserted) =
                match forward_result {
                    Ok(response_doc) => {
                        if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid) {
                            let mirror_skip_reason = if is_mongo_mirror_unsafe_request(op_code, body, command_name.as_deref()) {
                                Some("session_affine")
                            } else {
                                None
                            };
                            spawn_mongo_mirrors(
                                ep,
                                &state,
                                &organization_uuid,
                                &interlay_id_label,
                                req_type.clone(),
                                Bytes::copy_from_slice(body),
                                op_code,
                                response_doc.clone(),
                                &telemetry_wrapper,
                                &ctx,
                                mirror_skip_reason,
                            );
                        }

                        let matched = response_doc.get_i64("n").ok().map(|v| v as u64);
                        let modified = response_doc.get_i64("nModified").ok().map(|v| v as u64);
                        let deleted = if command_name.as_deref().is_some_and(|c| c.eq_ignore_ascii_case("delete")) {
                            matched
                        } else {
                            None
                        };
                        let upserted = response_doc.get_i64("nUpserted").ok().map(|v| v as u64);
                        let inserted = response_doc
                            .get_i64("nInserted")
                            .ok()
                            .map(|v| v as u64)
                            .or_else(|| response_doc.get_document("insertedIds").ok().map(|ids| ids.len() as u64));
                        (
                            build_response(header.request_id, op_code, &response_doc),
                            true,
                            matched,
                            modified,
                            deleted,
                            upserted,
                            inserted,
                        )
                    }
                    Err(e) => {
                        command_span.add_event("MongoDB command error", vec![FastSpanAttribute::new("error", e.to_string())]);
                        log_error!(
                            ctx.clone(),
                            "Failed to forward MongoDB command",
                            audience = LogAudience::Internal,
                            connection_id = connection_id,
                            error = e.to_string()
                        );
                        telemetry_wrapper.record(MetricEvent::ProxyError {
                            org_uuid: organization_uuid.as_str(),
                            interlay_uuid: &interlay_id_str,
                            error_type: "command_error",
                        });
                        (
                            build_error_response(header.request_id, op_code, &format!("Proxy error: {}", e)),
                            false,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                };
            telemetry_wrapper.metrics().proxy().record_response_encode_duration(
                response_encode_start.elapsed().as_micros() as u64,
                &[
                    ("org_uuid", organization_uuid.as_str()),
                    ("interlay_uuid", interlay_id_str.as_str()),
                    ("endpoint_uuid", ep_id_str.as_str()),
                    ("endpoint_kind", "mongo"),
                ],
            );

            let response_bytes_len = response.len() as u32;
            command_span.add_event(
                "MongoDB command completed",
                vec![
                    FastSpanAttribute::new("duration_us", cmd_duration_us.to_string()),
                    FastSpanAttribute::new("bytes_written", response_bytes_len.to_string()),
                    FastSpanAttribute::new("success", is_success.to_string()),
                ],
            );

            // Record wire analytics
            let analytics_start = Instant::now();

            telemetry_wrapper.metrics().proxy().record_analytics_record_duration(
                analytics_start.elapsed().as_micros() as u64,
                &[
                    ("org_uuid", organization_uuid.as_str()),
                    ("interlay_uuid", interlay_id_str.as_str()),
                    ("endpoint_uuid", ep_id_str.as_str()),
                    ("endpoint_kind", "mongo"),
                ],
            );

            // Record proxy request telemetry
            let request_duration_us = request_start.elapsed().as_micros() as u64;
            let overhead_us = request_duration_us.saturating_sub(cmd_duration_us);
            let endpoint_uuid_value = routing_state.endpoint.eden_uuid::<EndpointUuid>();
            let proxy_series = telemetry_wrapper.metrics().proxy().series_for_organization(
                &organization_uuid_value,
                &interlay_uuid_value,
                &endpoint_uuid_value,
                "mongo",
            );
            proxy_series.record_batch(ProxyBatchRecord {
                duration_us: request_duration_us,
                comparable_duration_us: Some(request_duration_us),
                endpoint_duration_us: Some(cmd_duration_us),
                overhead_us: Some(overhead_us),
                bytes_read: request_bytes_len as u64,
                bytes_written: response_bytes_len as u64,
                command_count: 1,
            });

            // Send response back to client
            if sender.send(response).is_err() {
                command_span.add_event("MongoDB response queue closed", vec![FastSpanAttribute::new("queue", "response")]);
                telemetry_wrapper.metrics().proxy().record_bridge_enqueue_rejection(&[
                    ("org_uuid", organization_uuid.as_str()),
                    ("interlay_uuid", interlay_id_str.as_str()),
                    ("endpoint_uuid", ep_id_str.as_str()),
                    ("endpoint_kind", "mongo"),
                    ("queue", "response"),
                    ("reason", "queue_full_or_closed"),
                ]);
                log_debug!(
                    ctx.clone(),
                    "Client sender closed, ending MongoDB proxy connection",
                    audience = LogAudience::Internal,
                    connection_id = connection_id
                );
                // Proxy connection close is counted by the interlay listener guard.
                return;
            }
        }
    }
}

/// Build an OP_MSG response wrapping a BSON document.
fn build_op_msg_response(request_id: i32, response_doc: &Document) -> Bytes {
    let mut doc_bytes = Vec::new();
    if response_doc.to_writer(&mut doc_bytes).is_err() {
        return build_op_msg_error(request_id, "Failed to serialize response");
    }

    // OP_MSG format: header(16) + flags(4) + kind(1) + document
    let body_len = 4 + 1 + doc_bytes.len();
    let total_len = MessageHeader::SIZE + body_len;

    let mut buf = BytesMut::with_capacity(total_len);

    // Header
    buf.put_i32_le(total_len as i32);
    buf.put_i32_le(0); // requestID
    buf.put_i32_le(request_id); // responseTo
    buf.put_i32_le(OpCode::Msg as i32);

    // Body
    buf.put_u32_le(0); // flagBits
    buf.put_u8(0); // section kind 0 (body)
    buf.extend_from_slice(&doc_bytes);

    buf.freeze()
}

/// Build an OP_MSG error response.
fn build_op_msg_error(request_id: i32, error_message: &str) -> Bytes {
    let error_doc = bson::doc! {
        "ok": 0,
        "errmsg": error_message,
        "code": 13,
        "codeName": "Unauthorized",
    };
    build_op_msg_response(request_id, &error_doc)
}

/// Build an OP_REPLY response wrapping a BSON document.
///
/// OP_REPLY format: header(16) + responseFlags(4) + cursorID(8) +
/// startingFrom(4) + numberReturned(4) + document
fn build_op_reply_response(request_id: i32, response_doc: &Document) -> Bytes {
    let mut doc_bytes = Vec::new();
    if response_doc.to_writer(&mut doc_bytes).is_err() {
        return build_op_reply_error(request_id, "Failed to serialize response");
    }

    let body_len = 4 + 8 + 4 + 4 + doc_bytes.len();
    let total_len = MessageHeader::SIZE + body_len;

    let mut buf = BytesMut::with_capacity(total_len);

    // Header
    buf.put_i32_le(total_len as i32);
    buf.put_i32_le(0); // requestID
    buf.put_i32_le(request_id); // responseTo
    buf.put_i32_le(OpCode::Reply as i32);

    // Body
    buf.put_u32_le(0); // responseFlags
    buf.put_i64_le(0); // cursorID
    buf.put_i32_le(0); // startingFrom
    buf.put_i32_le(1); // numberReturned
    buf.extend_from_slice(&doc_bytes);

    buf.freeze()
}

/// Build an OP_REPLY error response.
fn build_op_reply_error(request_id: i32, error_message: &str) -> Bytes {
    let error_doc = bson::doc! {
        "ok": 0,
        "errmsg": error_message,
        "code": 13,
        "codeName": "Unauthorized",
    };
    build_op_reply_response(request_id, &error_doc)
}

/// Build a response in the format matching the request opcode.
fn build_response(request_id: i32, op_code: Option<OpCode>, response_doc: &Document) -> Bytes {
    match op_code {
        Some(OpCode::Query) => build_op_reply_response(request_id, response_doc),
        _ => build_op_msg_response(request_id, response_doc),
    }
}

/// Build an error response in the format matching the request opcode.
fn build_error_response(request_id: i32, op_code: Option<OpCode>, error_message: &str) -> Bytes {
    match op_code {
        Some(OpCode::Query) => build_op_reply_error(request_id, error_message),
        _ => build_op_msg_error(request_id, error_message),
    }
}
