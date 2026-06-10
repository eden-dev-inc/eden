use super::*;
use crate::bridge::{BridgeQueueCounters, pump};
use eden_logger_internal::ctx_with_trace;
use function_name::named;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ValidationBridgeHarness {
    client: DuplexStream,
    request_receiver: tokio::sync::mpsc::UnboundedReceiver<ProxyRequestChunk>,
    response_sender: BytesQueueSender,
    counters: Arc<BridgeQueueCounters>,
    task: tokio::task::JoinHandle<()>,
}

impl ValidationBridgeHarness {
    #[named]
    pub fn spawn(buffer_size: usize) -> Self {
        let (client, bridge) = tokio::io::duplex(buffer_size);
        let (bridge_reader, bridge_writer) = tokio::io::split(bridge);
        let (request_sender, request_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (response_sender_raw, response_receiver) = tokio::sync::mpsc::unbounded_channel();
        let config = gateway_runtime_config();
        let response_sender =
            BytesQueueSender::new(response_sender_raw, config.max_bridge_pending_messages, config.max_bridge_pending_bytes);
        let counters = Arc::new(BridgeQueueCounters::default());
        let observer = BridgeQueueObserver::enabled(counters.clone());
        let base_ctx = ctx_with_trace!().with_feature("proxy_validation_bridge");

        let bridge_response_sender = response_sender.clone();
        let task = tokio::spawn(async move {
            let _ = run_proxy_bridge_loop(
                base_ctx,
                bridge_reader,
                bridge_writer,
                ProxyBridgeQueues {
                    sender: request_sender,
                    response_sender: bridge_response_sender,
                    response_receiver,
                    observer,
                },
                ProxyBridgeTelemetry::new(Arc::new(AllMetrics::new()), ProxyBridgeMetricLabels::unknown("validation-org", "validation")),
            )
            .await;
        });

        Self { client, request_receiver, response_sender, counters, task }
    }

    pub fn client_mut(&mut self) -> &mut DuplexStream {
        &mut self.client
    }

    pub async fn recv_request(&mut self) -> Option<Bytes> {
        let request = self.request_receiver.recv().await?;
        self.counters.record_request_dequeued(request.len());
        Some(request.into_bytes())
    }

    pub fn send_response(&self, response: Bytes) -> Result<(), tokio::sync::mpsc::error::SendError<Bytes>> {
        let len = response.len();
        self.counters.record_response_enqueued(len);
        match self.response_sender.send(response) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.counters.record_response_dequeued(len);
                Err(err)
            }
        }
    }

    pub fn snapshot(&self) -> BridgeQueueSnapshot {
        self.counters.snapshot()
    }

    pub fn abort(&self) {
        self.task.abort();
    }
}

impl Drop for ValidationBridgeHarness {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct PartialThenFailWriter {
    first_write_limit: usize,
    written: usize,
    failed: bool,
}

impl PartialThenFailWriter {
    fn new(first_write_limit: usize) -> Self {
        Self {
            first_write_limit: first_write_limit.max(1),
            written: 0,
            failed: false,
        }
    }
}

impl AsyncWrite for PartialThenFailWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.failed {
            return Poll::Ready(Err(io::Error::other("intentional partial write failure")));
        }

        let to_write = self.first_write_limit.min(buf.len()).max(1);
        self.written += to_write;
        self.failed = true;
        Poll::Ready(Ok(to_write))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub struct PumpPartialWriteLossValidation;

impl PumpPartialWriteLossValidation {
    pub async fn run(payload: Bytes, first_write_limit: usize) -> Result<(usize, usize), String> {
        let (mut feeder, mut reader) = tokio::io::duplex(payload.len().max(64));
        let expected_len = payload.len();
        let feeder_task = tokio::spawn(async move {
            feeder.write_all(&payload).await?;
            feeder.shutdown().await
        });

        let mut writer = PartialThenFailWriter::new(first_write_limit);
        let pump_result = pump(&mut reader, &mut writer).await;
        let _ = feeder_task.await.map_err(|err| err.to_string())?;

        match pump_result {
            Ok(()) => Err("pump unexpectedly succeeded without surfacing a partial write failure".to_string()),
            Err(_) => Ok((writer.written, expected_len)),
        }
    }
}
