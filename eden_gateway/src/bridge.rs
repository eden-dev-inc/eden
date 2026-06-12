use super::*;

const DEFAULT_MAX_BRIDGE_PENDING_MESSAGES: u64 = 128;
const DEFAULT_MAX_BRIDGE_PENDING_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_GATEWAY_FAIRNESS_YIELD_INTERVAL: u64 = 64;
const DEFAULT_GATEWAY_FINAL_RESPONSE_DRAIN_TIMEOUT_MS: u64 = 2_000;

const ENV_MAX_BRIDGE_PENDING_MESSAGES: &str = "EDEN_GATEWAY_BRIDGE_MAX_PENDING_MESSAGES";
const ENV_MAX_BRIDGE_PENDING_BYTES: &str = "EDEN_GATEWAY_BRIDGE_MAX_PENDING_BYTES";
const ENV_GATEWAY_FAIRNESS_YIELD_INTERVAL: &str = "EDEN_GATEWAY_FAIRNESS_YIELD_INTERVAL";
const ENV_GATEWAY_FINAL_RESPONSE_DRAIN_TIMEOUT_MS: &str = "EDEN_GATEWAY_FINAL_RESPONSE_DRAIN_TIMEOUT_MS";

#[derive(Clone, Debug)]
pub(crate) struct ProxyBridgeMetricLabels {
    org_uuid: String,
    interlay_uuid: String,
    endpoint_uuid: Option<String>,
    endpoint_kind: String,
}

impl ProxyBridgeMetricLabels {
    pub(crate) fn from_interlay_state(
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
    ) -> Self {
        let Some(state) = interlay_endpoints.get(interlay_cache_uuid) else {
            return Self {
                org_uuid: interlay_cache_uuid
                    .org()
                    .map(|org| org.eden_uuid::<eden_core::format::OrganizationUuid>().to_string())
                    .unwrap_or_default(),
                interlay_uuid: interlay_cache_uuid.uuid().to_string(),
                endpoint_uuid: None,
                endpoint_kind: "unknown".to_string(),
            };
        };

        Self {
            org_uuid: interlay_cache_uuid
                .org()
                .or_else(|| state.endpoint_uuid().org())
                .map(|org| org.eden_uuid::<eden_core::format::OrganizationUuid>().to_string())
                .unwrap_or_default(),
            interlay_uuid: interlay_cache_uuid.uuid().to_string(),
            endpoint_uuid: Some(state.endpoint_uuid().uuid().to_string()),
            endpoint_kind: state.endpoint_kind().as_str().to_string(),
        }
    }

    pub(crate) fn unknown(org_uuid: impl Into<String>, interlay_uuid: impl Into<String>) -> Self {
        Self {
            org_uuid: org_uuid.into(),
            interlay_uuid: interlay_uuid.into(),
            endpoint_uuid: None,
            endpoint_kind: "unknown".to_string(),
        }
    }

    fn bridge_series(&self, metrics: &AllMetrics) -> eden_core::telemetry::metrics::ProxyBridgeSeries {
        if let Some(endpoint_uuid) = self.endpoint_uuid.as_deref() {
            metrics.proxy().bridge_series(&[
                ("org_uuid", self.org_uuid.as_str()),
                ("interlay_uuid", self.interlay_uuid.as_str()),
                ("endpoint_uuid", endpoint_uuid),
                ("endpoint_kind", self.endpoint_kind.as_str()),
            ])
        } else {
            metrics.proxy().bridge_series(&[
                ("org_uuid", self.org_uuid.as_str()),
                ("interlay_uuid", self.interlay_uuid.as_str()),
                ("endpoint_kind", self.endpoint_kind.as_str()),
            ])
        }
    }

    pub(crate) fn record_enqueue_rejection(&self, metrics: &AllMetrics, queue: &'static str, reason: &'static str) {
        if let Some(endpoint_uuid) = self.endpoint_uuid.as_deref() {
            metrics.proxy().record_bridge_enqueue_rejection(&[
                ("org_uuid", self.org_uuid.as_str()),
                ("interlay_uuid", self.interlay_uuid.as_str()),
                ("endpoint_uuid", endpoint_uuid),
                ("endpoint_kind", self.endpoint_kind.as_str()),
                ("queue", queue),
                ("reason", reason),
            ]);
        } else {
            metrics.proxy().record_bridge_enqueue_rejection(&[
                ("org_uuid", self.org_uuid.as_str()),
                ("interlay_uuid", self.interlay_uuid.as_str()),
                ("endpoint_kind", self.endpoint_kind.as_str()),
                ("queue", queue),
                ("reason", reason),
            ]);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GatewayRuntimeConfig {
    pub(crate) max_bridge_pending_messages: u64,
    pub(crate) max_bridge_pending_bytes: u64,
    fairness_yield_interval: u64,
    pub(crate) final_response_drain_timeout: Duration,
}

impl GatewayRuntimeConfig {
    fn from_env() -> Self {
        let max_bridge_pending_messages = env_u64_at_least_one(ENV_MAX_BRIDGE_PENDING_MESSAGES, DEFAULT_MAX_BRIDGE_PENDING_MESSAGES);
        let max_bridge_pending_bytes = env_u64_at_least_one(ENV_MAX_BRIDGE_PENDING_BYTES, DEFAULT_MAX_BRIDGE_PENDING_BYTES);
        let fairness_yield_interval = env_u64_at_least_one(ENV_GATEWAY_FAIRNESS_YIELD_INTERVAL, DEFAULT_GATEWAY_FAIRNESS_YIELD_INTERVAL);
        let final_response_drain_timeout_ms =
            env_u64_at_least_one(ENV_GATEWAY_FINAL_RESPONSE_DRAIN_TIMEOUT_MS, DEFAULT_GATEWAY_FINAL_RESPONSE_DRAIN_TIMEOUT_MS);

        Self {
            max_bridge_pending_messages,
            max_bridge_pending_bytes,
            fairness_yield_interval,
            final_response_drain_timeout: Duration::from_millis(final_response_drain_timeout_ms),
        }
    }
}

static GATEWAY_RUNTIME_CONFIG: OnceLock<GatewayRuntimeConfig> = OnceLock::new();

pub(crate) fn gateway_runtime_config() -> &'static GatewayRuntimeConfig {
    GATEWAY_RUNTIME_CONFIG.get_or_init(GatewayRuntimeConfig::from_env)
}

fn env_u64_at_least_one(name: &str, default: u64) -> u64 {
    let Ok(value) = std::env::var(name) else {
        return default.max(1);
    };

    value.trim().parse::<u64>().unwrap_or(default).max(1)
}

pub(crate) async fn yield_after_gateway_chunk(gateway_yield_counter: &mut u64) {
    *gateway_yield_counter = gateway_yield_counter.wrapping_add(1);
    if (*gateway_yield_counter).is_multiple_of(gateway_runtime_config().fairness_yield_interval) {
        tokio::task::yield_now().await;
    }
}

pub(crate) fn elapsed_us(start: Instant) -> u64 {
    crate::gateway_telemetry::GatewayTelemetry::elapsed_since_us(start)
}

#[derive(Debug, Default)]
pub(crate) struct BridgeQueueCounters {
    request_enqueued_messages: std::sync::atomic::AtomicU64,
    request_enqueued_bytes: std::sync::atomic::AtomicU64,
    request_dequeued_messages: std::sync::atomic::AtomicU64,
    request_dequeued_bytes: std::sync::atomic::AtomicU64,
    response_enqueued_messages: std::sync::atomic::AtomicU64,
    response_enqueued_bytes: std::sync::atomic::AtomicU64,
    response_dequeued_messages: std::sync::atomic::AtomicU64,
    response_dequeued_bytes: std::sync::atomic::AtomicU64,
}

impl BridgeQueueCounters {
    pub(crate) fn record_request_enqueued(&self, len: usize) {
        self.request_enqueued_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.request_enqueued_bytes.fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_request_dequeued(&self, len: usize) {
        self.request_dequeued_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.request_dequeued_bytes.fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_response_enqueued(&self, len: usize) {
        self.response_enqueued_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.response_enqueued_bytes.fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_response_dequeued(&self, len: usize) {
        self.response_dequeued_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.response_dequeued_bytes.fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> BridgeQueueSnapshot {
        let request_enqueued_messages = self.request_enqueued_messages.load(std::sync::atomic::Ordering::Relaxed);
        let request_enqueued_bytes = self.request_enqueued_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let request_dequeued_messages = self.request_dequeued_messages.load(std::sync::atomic::Ordering::Relaxed);
        let request_dequeued_bytes = self.request_dequeued_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let response_enqueued_messages = self.response_enqueued_messages.load(std::sync::atomic::Ordering::Relaxed);
        let response_enqueued_bytes = self.response_enqueued_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let response_dequeued_messages = self.response_dequeued_messages.load(std::sync::atomic::Ordering::Relaxed);
        let response_dequeued_bytes = self.response_dequeued_bytes.load(std::sync::atomic::Ordering::Relaxed);

        BridgeQueueSnapshot {
            request_enqueued_messages,
            request_enqueued_bytes,
            request_dequeued_messages,
            request_dequeued_bytes,
            request_pending_messages: request_enqueued_messages.saturating_sub(request_dequeued_messages),
            request_pending_bytes: request_enqueued_bytes.saturating_sub(request_dequeued_bytes),
            response_enqueued_messages,
            response_enqueued_bytes,
            response_dequeued_messages,
            response_dequeued_bytes,
            response_pending_messages: response_enqueued_messages.saturating_sub(response_dequeued_messages),
            response_pending_bytes: response_enqueued_bytes.saturating_sub(response_dequeued_bytes),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BridgeQueueObserver(Option<Arc<BridgeQueueCounters>>);

impl BridgeQueueObserver {
    pub(crate) fn disabled() -> Self {
        Self(None)
    }

    pub(crate) fn enabled(counters: Arc<BridgeQueueCounters>) -> Self {
        Self(Some(counters))
    }

    pub(crate) fn record_request_enqueued(&self, len: usize) {
        if let Some(counters) = &self.0 {
            counters.record_request_enqueued(len);
        }
    }

    pub(crate) fn record_response_dequeued(&self, len: usize) {
        if let Some(counters) = &self.0 {
            counters.record_response_dequeued(len);
        }
    }

    pub(crate) fn snapshot(&self) -> BridgeQueueSnapshot {
        self.0.as_ref().map(|counters| counters.snapshot()).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BridgeQueueSnapshot {
    pub request_enqueued_messages: u64,
    pub request_enqueued_bytes: u64,
    pub request_dequeued_messages: u64,
    pub request_dequeued_bytes: u64,
    pub request_pending_messages: u64,
    pub request_pending_bytes: u64,
    pub response_enqueued_messages: u64,
    pub response_enqueued_bytes: u64,
    pub response_dequeued_messages: u64,
    pub response_dequeued_bytes: u64,
    pub response_pending_messages: u64,
    pub response_pending_bytes: u64,
}

#[named]
pub async fn handle_connection<SR, SW>(
    stream: InterlayStream,
    client_addr: SocketAddr,
    server_reader: &mut SR,
    server_writer: &mut SW,
) -> io::Result<()>
where
    SR: AsyncRead + Unpin + Send,
    SW: AsyncWrite + Unpin + Send,
{
    let _ctx = ctx_with_trace!().with_feature("gateway").with_additional("client_addr", client_addr.to_string());

    log_debug!(
        _ctx.clone(),
        "Handling connection",
        audience = LogAudience::Internal,
        client_addr = client_addr.to_string()
    );

    match &stream {
        InterlayStream::Tcp(tcp_stream) => {
            tcp_stream.set_nodelay(true)?;
        }

        InterlayStream::Tls(tls_stream) => {
            tls_stream.get_ref().0.set_nodelay(true)?;
        }
    }

    let (mut cr, mut cw): (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>) = match stream {
        InterlayStream::Tcp(stream) => {
            let (r, w) = stream.into_split();
            (Box::new(r), Box::new(w))
        }

        InterlayStream::Tls(stream) => {
            let (r, w) = io::split(stream);
            (Box::new(r), Box::new(w))
        }
    };

    // Run both pumps concurrently WITHOUT spawning (so no 'static needed)
    let res = tokio::try_join!(
        // this pump just copies the incoming commands to the protocol
        pump(&mut cr, server_writer),
        // this pump copies the results of the protocol to the client
        pump(server_reader, &mut cw),
    );

    if let Err(e) = res {
        log_error!(_ctx, "Pump error", audience = LogAudience::Internal, error = e.to_string());
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_eof_after_empty_response_queue_does_not_wait_for_sender_drop() {
        let (mut client, bridge) = tokio::io::duplex(4096);
        let (bridge_reader, bridge_writer) = tokio::io::split(bridge);
        let (request_sender, mut request_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (response_sender_raw, response_receiver) = tokio::sync::mpsc::unbounded_channel();
        let response_sender = BytesQueueSender::new(response_sender_raw, 16, 4096);

        let bridge_response_sender = response_sender.clone();
        let bridge_task = tokio::spawn(run_proxy_bridge_loop(
            eden_logger_internal::LogContext::default().with_feature("bridge_test"),
            bridge_reader,
            bridge_writer,
            ProxyBridgeQueues {
                sender: request_sender,
                response_sender: bridge_response_sender,
                response_receiver,
                observer: BridgeQueueObserver::disabled(),
            },
            ProxyBridgeTelemetry::new(Arc::new(AllMetrics::new()), ProxyBridgeMetricLabels::unknown("bridge-test-org", "bridge-test")),
        ));

        client.write_all(b"GET /v1/health HTTP/1.1\r\nHost: eden\r\n\r\n").await.expect("write request to bridge");
        let request = request_receiver.recv().await.expect("bridge should forward request bytes");
        assert!(!request.into_bytes().is_empty());

        let response = Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK");
        response_sender.send(response).expect("send response to bridge");

        let mut response_buf = BytesMut::new();
        client.read_buf(&mut response_buf).await.expect("read response from bridge");
        assert!(response_buf.ends_with(b"OK"));

        drop(client);

        tokio::time::timeout(Duration::from_millis(100), bridge_task)
            .await
            .expect("bridge should not wait for final drain timeout")
            .expect("bridge task should join")
            .expect("bridge should exit cleanly");
    }
}

pub(crate) async fn pump<R, W>(reader: &mut R, writer: &mut W) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut gateway_yield_counter = 0;
    loop {
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                let _ = writer.shutdown().await;
                return Ok(());
            }
            Ok(_) => {}
            Err(e) => return Err(e),
        };
        writer.write_all(&buf).await?;
        writer.flush().await?;
        buf.clear();
        yield_after_gateway_chunk(&mut gateway_yield_counter).await;
    }
}

pub(crate) struct ProxyBridgeQueues {
    pub(crate) sender: tokio::sync::mpsc::UnboundedSender<ProxyRequestChunk>,
    pub(crate) response_sender: BytesQueueSender,
    pub(crate) response_receiver: tokio::sync::mpsc::UnboundedReceiver<QueuedBytes>,
    pub(crate) observer: BridgeQueueObserver,
}

pub(crate) struct ProxyBridgeTelemetry {
    pub(crate) metrics: Arc<AllMetrics>,
    pub(crate) labels: ProxyBridgeMetricLabels,
    series: eden_core::telemetry::metrics::ProxyBridgeSeries,
}

impl ProxyBridgeTelemetry {
    pub(crate) fn new(metrics: Arc<AllMetrics>, labels: ProxyBridgeMetricLabels) -> Self {
        let series = labels.bridge_series(&metrics);
        Self { metrics, labels, series }
    }

    #[inline]
    pub(crate) fn record_response_queue(&self, duration_us: u64) {
        self.series.record_response_queue(duration_us);
    }

    #[inline]
    pub(crate) fn record_client_write(&self, duration_us: u64) {
        self.series.record_client_write(duration_us);
    }

    #[inline]
    pub(crate) fn record_request_chunk(&self) {
        self.series.record_request_chunk();
    }

    #[inline]
    pub(crate) fn record_response_chunk(&self) {
        self.series.record_response_chunk();
    }

    #[inline]
    pub(crate) fn record_end_to_end(&self, duration_us: u64, request_command_count: u64) {
        self.series.record_end_to_end(duration_us, request_command_count);
    }

    #[inline]
    pub(crate) fn record_enqueue_rejection(&self, queue: &'static str, reason: &'static str) {
        self.labels.record_enqueue_rejection(&self.metrics, queue, reason);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProxyBridgeTerminationReason {
    ClientEof,
    ClientReadError,
    ProcessorChannelClosed,
    ProcessorFinished,
    ClientWriteError,
}

impl ProxyBridgeTerminationReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ClientEof => "client_eof",
            Self::ClientReadError => "client_read_error",
            Self::ProcessorChannelClosed => "processor_channel_closed",
            Self::ProcessorFinished => "processor_finished",
            Self::ClientWriteError => "client_write_error",
        }
    }

    pub(crate) fn is_error(self) -> bool {
        !matches!(self, Self::ClientEof | Self::ProcessorFinished)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn log_proxy_bridge_termination(
    base_ctx: &eden_logger_internal::LogContext,
    reason: ProxyBridgeTerminationReason,
    chunks_to_processor: u64,
    bytes_to_processor: u64,
    responses_from_processor: u64,
    queue_snapshot: BridgeQueueSnapshot,
    drain_outcome: &str,
    error: Option<&str>,
) {
    let error = error.unwrap_or_default();
    if reason.is_error() {
        log_warn!(
            base_ctx.clone(),
            "Gateway bridge connection ended",
            audience = LogAudience::Internal,
            reason = reason.as_str(),
            chunks_sent = chunks_to_processor,
            bytes_sent = bytes_to_processor,
            responses_written = responses_from_processor,
            request_pending_messages = queue_snapshot.request_pending_messages,
            request_pending_bytes = queue_snapshot.request_pending_bytes,
            response_pending_messages = queue_snapshot.response_pending_messages,
            response_pending_bytes = queue_snapshot.response_pending_bytes,
            final_drain = drain_outcome,
            error = error
        );
    } else {
        log_info!(
            base_ctx.clone(),
            "Gateway bridge connection ended",
            audience = LogAudience::Internal,
            reason = reason.as_str(),
            chunks_sent = chunks_to_processor,
            bytes_sent = bytes_to_processor,
            responses_written = responses_from_processor,
            request_pending_messages = queue_snapshot.request_pending_messages,
            request_pending_bytes = queue_snapshot.request_pending_bytes,
            response_pending_messages = queue_snapshot.response_pending_messages,
            response_pending_bytes = queue_snapshot.response_pending_bytes,
            final_drain = drain_outcome,
            error = error
        );
    }
}

pub(crate) async fn run_proxy_bridge_loop<R, W>(
    base_ctx: eden_logger_internal::LogContext,
    mut bridge_reader: R,
    mut bridge_writer: W,
    queues: ProxyBridgeQueues,
    telemetry: ProxyBridgeTelemetry,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let ProxyBridgeQueues { sender, response_sender, mut response_receiver, observer } = queues;
    let mut read_buf = BytesMut::with_capacity(16 * 1024);
    let mut chunks_to_processor: u64 = 0;
    let mut bytes_to_processor: u64 = 0;
    let mut responses_from_processor: u64 = 0;
    let mut gateway_yield_counter = 0;
    let mut termination_error: Option<String> = None;

    let termination_reason = loop {
        #[cfg(feature = "log-trace")]
        let iter_ctx = base_ctx.clone().with_fresh_span();

        select! {
            n = bridge_reader.read_buf(&mut read_buf) => {
                let bytes_read = match n {
                    Ok(0) => break ProxyBridgeTerminationReason::ClientEof,
                    Ok(n) => n,
                    Err(e) => {
                        termination_error = Some(e.to_string());
                        break ProxyBridgeTerminationReason::ClientReadError;
                    }
                };
                #[cfg(not(feature = "log-trace"))]
                let _ = bytes_read;

                #[cfg(feature = "log-trace")]
                log_trace!(iter_ctx.clone(), "Gateway read",
                    audience = LogAudience::Internal,
                    bytes_read = bytes_read,
                    data = str::from_utf8(&read_buf).unwrap_or_default()
                );

                let request_received_at = Instant::now();
                let chunk = read_buf.split().freeze();
                observer.record_request_enqueued(chunk.len());
                bytes_to_processor += chunk.len() as u64;
                chunks_to_processor += 1;
                telemetry.record_request_chunk();
                if let Err(e) = sender.send(ProxyRequestChunk::new_with_received_at(chunk, request_received_at)) {
                    telemetry.record_enqueue_rejection("request", "processor_closed");
                    let err_ctx = base_ctx.clone().with_fresh_span();
                    log_error!(err_ctx, "Error sending data to processor channel",
                        audience = LogAudience::Internal,
                        error = e.to_string()
                    );
                    termination_error = Some(e.to_string());
                    break ProxyBridgeTerminationReason::ProcessorChannelClosed;
                }
                yield_after_gateway_chunk(&mut gateway_yield_counter).await;
            }

            response = response_receiver.recv() => {
                let Some(resp) = response else {
                    break ProxyBridgeTerminationReason::ProcessorFinished;
                };
                let response_queue_us = resp.queue_wait_us();
                let request_received_at = resp.request_received_at();
                let request_command_count = resp.request_command_count();
                let resp = resp.into_bytes();
                responses_from_processor += 1;
                observer.record_response_dequeued(resp.len());
                response_sender.record_dequeued(resp.len());
                telemetry.record_response_queue(response_queue_us);
                telemetry.record_response_chunk();

                #[cfg(feature = "log-trace")]
                log_trace!(iter_ctx.clone(), "Gateway response",
                    audience = LogAudience::Internal,
                    response_len = resp.len(),
                    data = str::from_utf8(&resp).unwrap_or_default()
                );

                let write_start = Instant::now();
                if let Err(e) = bridge_writer.write_all(&resp).await {
                    let err_ctx = base_ctx.clone().with_fresh_span();
                    log_error!(err_ctx, "Error writing result of proxy handler",
                        audience = LogAudience::Internal,
                        error = e.to_string()
                    );
                    termination_error = Some(e.to_string());
                    break ProxyBridgeTerminationReason::ClientWriteError;
                }
                if let Some(request_received_at) = request_received_at {
                    telemetry.record_end_to_end(elapsed_us(request_received_at), request_command_count);
                }
                telemetry.record_client_write(elapsed_us(write_start));
                yield_after_gateway_chunk(&mut gateway_yield_counter).await;
            }
        }
    };

    drop(sender);
    log_trace!(
        base_ctx.clone(),
        "Gateway dropped, trying to receive any remaining messages",
        audience = LogAudience::Internal
    );
    let final_response_drain_timeout = gateway_runtime_config().final_response_drain_timeout;
    let mut final_drain_outcome = "completed";
    let drain_result = tokio::time::timeout(final_response_drain_timeout, async {
        while response_sender.pending_messages() > 0 {
            let Some(resp) = response_receiver.recv().await else {
                break;
            };
            let response_queue_us = resp.queue_wait_us();
            let request_received_at = resp.request_received_at();
            let request_command_count = resp.request_command_count();
            let resp = resp.into_bytes();
            response_sender.record_dequeued(resp.len());
            observer.record_response_dequeued(resp.len());
            telemetry.record_response_queue(response_queue_us);
            telemetry.record_response_chunk();
            let cleanup_ctx = base_ctx.clone().with_fresh_span();
            log_trace!(
                cleanup_ctx.clone(),
                "Proxy final response",
                audience = LogAudience::Internal,
                response_len = resp.len(),
                data = str::from_utf8(&resp).unwrap_or_default()
            );
            let write_start = Instant::now();
            if let Err(e) = bridge_writer.write_all(&resp).await {
                log_error!(
                    cleanup_ctx,
                    "Error writing result of proxy handler",
                    audience = LogAudience::Internal,
                    error = e.to_string()
                );
                final_drain_outcome = "client_write_error";
                if termination_error.is_none() {
                    termination_error = Some(e.to_string());
                }
                break;
            }
            if let Some(request_received_at) = request_received_at {
                telemetry.record_end_to_end(elapsed_us(request_received_at), request_command_count);
            }
            telemetry.record_client_write(elapsed_us(write_start));
        }
    })
    .await;
    if drain_result.is_err() {
        final_drain_outcome = "timed_out";
        log_warn!(
            base_ctx.clone(),
            "Timed out draining gateway responses after client disconnect",
            audience = LogAudience::Internal,
            timeout_ms = final_response_drain_timeout.as_millis()
        );
    }
    if let Err(e) = bridge_writer.flush().await {
        log_error!(
            base_ctx,
            "Gateway handler connection closed, flush error",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    }
    log_proxy_bridge_termination(
        &base_ctx,
        termination_reason,
        chunks_to_processor,
        bytes_to_processor,
        responses_from_processor,
        observer.snapshot(),
        final_drain_outcome,
        termination_error.as_deref(),
    );

    if termination_reason.is_error() {
        return Err(io::Error::other(termination_error.unwrap_or_else(|| termination_reason.as_str().to_string())));
    }

    Ok(())
}
