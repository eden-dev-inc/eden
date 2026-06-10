//! Redis byte-stream adapters for processor entry points.

use super::runtime::RedisUnifiedProcessor;
use super::*;

pub struct RedisStreamingProcessor;

impl RedisStreamingProcessor {
    /// Streaming entry point for the Redis processor. The current bridge has already
    /// parsed RESP frame boundaries before sending [`RedisIngressBatch`] values; the
    /// refactored processor below still consumes raw frame bytes, so this adapter
    /// preserves the bridge-facing API while feeding the unified processor.
    #[allow(clippy::too_many_arguments)]
    pub fn process_streaming(
        ep: RedisEp,
        receiver: UnboundedReceiver<RedisIngressBatch>,
        sender: BytesQueueSender,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: std::net::SocketAddr,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        listener_id: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            let (bytes_tx, bytes_rx) = tokio::sync::mpsc::unbounded_channel::<RedisWireBatch>();
            let parse_error_sender = sender.clone();
            let forward_task = eden_gateway_core::runtime::spawn_on_current_runtime(async move {
                Self::forward_ingress_batches(receiver, bytes_tx, parse_error_sender).await;
            });

            Self::run_bytes(
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

    pub(super) async fn forward_proxy_chunks_as_batches(
        mut receiver: UnboundedReceiver<ProxyRequestChunk>,
        sender: tokio::sync::mpsc::UnboundedSender<RedisWireBatch>,
    ) {
        while let Some(chunk) = receiver.recv().await {
            if sender.send(RedisWireBatch::from_proxy_chunk(chunk)).is_err() {
                return;
            }
        }
    }

    pub(super) async fn forward_ingress_batches(
        mut receiver: UnboundedReceiver<RedisIngressBatch>,
        sender: tokio::sync::mpsc::UnboundedSender<RedisWireBatch>,
        response_sender: BytesQueueSender,
    ) {
        while let Some(batch) = receiver.recv().await {
            if let Some(err) = batch.parse_error() {
                let _ = response_sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                return;
            }
            if !batch.batch_bytes().is_empty() && sender.send(RedisWireBatch::from_ingress_batch(batch)).is_err() {
                return;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_bytes(
        ep: RedisEp,
        receiver: UnboundedReceiver<RedisWireBatch>,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        sender: BytesQueueSender,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: std::net::SocketAddr,
        listener_id: String,
    ) {
        let connection_id = WIRE_CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
        RedisUnifiedProcessor::process(
            ep,
            receiver,
            database_manager,
            sender,
            settings,
            interlay_cache_uuid,
            interlay_endpoints,
            telemetry_wrapper,
            ctx,
            client_addr,
            listener_id,
            connection_id,
        )
        .await;
    }
}
