//! Database-agnostic protocol processing traits.
//!
//! This module defines the abstraction layer for wire protocol processing,
//! allowing different database types (Redis, Postgres, etc.) to implement
//! their own protocol handling while sharing common routing logic.

use bytes::Bytes;
use dashmap::DashMap;
use eden_core::format::cache_uuid::InterlayCacheUuid;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContext;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::settings::EdenSettings;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::response::GatewayResponsePolicy;

#[derive(Debug)]
pub struct ProxyRequestChunk {
    bytes: Bytes,
    enqueued_at: Instant,
    received_at: Instant,
}

impl ProxyRequestChunk {
    pub fn new(bytes: Bytes) -> Self {
        let now = Instant::now();
        Self { bytes, enqueued_at: now, received_at: now }
    }

    pub fn new_with_received_at(bytes: Bytes, received_at: Instant) -> Self {
        Self { bytes, enqueued_at: Instant::now(), received_at }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn queue_wait_us(&self) -> u64 {
        elapsed_us(self.enqueued_at)
    }

    pub fn received_at(&self) -> Instant {
        self.received_at
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

#[derive(Debug)]
pub struct QueuedBytes {
    payload: QueuedBytesPayload,
    enqueued_at: Instant,
    /// Optional pass-through of the original request-received timestamp from
    /// the bridge, threaded through processor → response queue → bridge so
    /// the bridge can compute end-to-end latency on write completion. Only
    /// populated when the processor calls `BytesQueueSender::send_with_request_received_at`.
    request_received_at: Option<Instant>,
    request_command_count: u64,
}

impl QueuedBytes {
    fn new(bytes: Bytes) -> Self {
        Self {
            payload: QueuedBytesPayload::single(bytes),
            enqueued_at: Instant::now(),
            request_received_at: None,
            request_command_count: 1,
        }
    }

    fn new_with_request_received_at(bytes: Bytes, request_received_at: Instant) -> Self {
        Self::new_with_request_received_at_and_command_count(bytes, request_received_at, 1)
    }

    fn new_with_request_received_at_and_command_count(bytes: Bytes, request_received_at: Instant, request_command_count: u64) -> Self {
        Self {
            payload: QueuedBytesPayload::single(bytes),
            enqueued_at: Instant::now(),
            request_received_at: Some(request_received_at),
            request_command_count: request_command_count.max(1),
        }
    }

    fn new_pair_with_request_received_at(first: Bytes, second: Bytes, request_received_at: Instant) -> Self {
        Self {
            payload: QueuedBytesPayload::pair(first, second),
            enqueued_at: Instant::now(),
            request_received_at: Some(request_received_at),
            request_command_count: 1,
        }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    pub fn queue_wait_us(&self) -> u64 {
        elapsed_us(self.enqueued_at)
    }

    /// Wall clock at which the bridge first observed the request bytes for
    /// this response. `None` for response payloads emitted outside the
    /// per-batch flow (e.g., spontaneous error frames).
    pub fn request_received_at(&self) -> Option<Instant> {
        self.request_received_at
    }

    /// Number of client commands represented by this queued response.
    ///
    /// Single-command and ad-hoc responses default to 1. Redis pipelined
    /// batches set this to the parsed command count so bridge end-to-end
    /// telemetry can publish a command-level latency view separately from
    /// the raw batch drain latency.
    pub fn request_command_count(&self) -> u64 {
        self.request_command_count.max(1)
    }

    pub fn into_payload(self) -> QueuedBytesPayload {
        self.payload
    }

    pub fn into_bytes(self) -> Bytes {
        self.payload.into_bytes()
    }
}

#[derive(Debug)]
pub enum QueuedBytesPayload {
    Single(Bytes),
    Pair(Bytes, Bytes),
}

impl QueuedBytesPayload {
    fn single(bytes: Bytes) -> Self {
        Self::Single(bytes)
    }

    fn pair(first: Bytes, second: Bytes) -> Self {
        Self::Pair(first, second)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Single(bytes) => bytes.len(),
            Self::Pair(first, second) => first.len().saturating_add(second.len()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Single(bytes) => bytes.is_empty(),
            Self::Pair(first, second) => first.is_empty() && second.is_empty(),
        }
    }

    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Single(bytes) => bytes,
            Self::Pair(first, second) => {
                let mut combined = Vec::with_capacity(first.len().saturating_add(second.len()));
                combined.extend_from_slice(&first);
                combined.extend_from_slice(&second);
                Bytes::from(combined)
            }
        }
    }

    pub fn trace_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Single(bytes) => Some(bytes),
            Self::Pair(_, _) => None,
        }
    }
}

fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub struct BytesQueueSender {
    inner: UnboundedSender<QueuedBytes>,
    pending_messages: Arc<AtomicU64>,
    pending_bytes: Arc<AtomicU64>,
    max_pending_messages: u64,
    max_pending_bytes: u64,
}

impl BytesQueueSender {
    pub fn new(inner: UnboundedSender<QueuedBytes>, max_pending_messages: u64, max_pending_bytes: u64) -> Self {
        Self {
            inner,
            pending_messages: Arc::new(AtomicU64::new(0)),
            pending_bytes: Arc::new(AtomicU64::new(0)),
            max_pending_messages,
            max_pending_bytes,
        }
    }

    pub fn send(&self, bytes: Bytes) -> Result<(), tokio::sync::mpsc::error::SendError<Bytes>> {
        self.send_inner(QueuedBytes::new(bytes))
    }

    /// Send a response payload with the request-received timestamp
    /// attached so the bridge can record end-to-end proxy-induced
    /// latency on the write side. Use this for per-batch responses
    /// where end-to-end attribution applies; use [`send`] for ad-hoc
    /// payloads (e.g. control frames, errors not tied to a request).
    pub fn send_with_request_received_at(
        &self,
        bytes: Bytes,
        request_received_at: Instant,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Bytes>> {
        self.send_inner(QueuedBytes::new_with_request_received_at(bytes, request_received_at))
    }

    /// Send a response payload with both the request-received timestamp
    /// and the number of client commands represented by the response.
    pub fn send_with_request_received_at_and_command_count(
        &self,
        bytes: Bytes,
        request_received_at: Instant,
        request_command_count: u64,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Bytes>> {
        self.send_inner(QueuedBytes::new_with_request_received_at_and_command_count(
            bytes,
            request_received_at,
            request_command_count,
        ))
    }

    pub fn send_pair_with_request_received_at(
        &self,
        first: Bytes,
        second: Bytes,
        request_received_at: Instant,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Bytes>> {
        let first_len = first.len() as u64;
        let second_len = second.len() as u64;
        let total_len = first_len.saturating_add(second_len);
        if !self.try_reserve(1, total_len) {
            return Err(tokio::sync::mpsc::error::SendError(first));
        }

        let payload = QueuedBytes::new_pair_with_request_received_at(first, second, request_received_at);
        if let Err(err) = self.inner.send(payload) {
            let dropped = err.0.into_bytes();
            self.pending_messages.fetch_sub(1, Ordering::Relaxed);
            self.pending_bytes.fetch_sub(total_len, Ordering::Relaxed);
            return Err(tokio::sync::mpsc::error::SendError(dropped));
        }

        Ok(())
    }

    fn send_inner(&self, payload: QueuedBytes) -> Result<(), tokio::sync::mpsc::error::SendError<Bytes>> {
        let len = payload.len() as u64;
        if !self.try_reserve(1, len) {
            return Err(tokio::sync::mpsc::error::SendError(payload.into_bytes()));
        }

        if let Err(err) = self.inner.send(payload) {
            let dropped = err.0.into_bytes();
            let dropped_len = dropped.len() as u64;
            self.pending_messages.fetch_sub(1, Ordering::Relaxed);
            self.pending_bytes.fetch_sub(dropped_len, Ordering::Relaxed);
            Err(tokio::sync::mpsc::error::SendError(dropped))
        } else {
            Ok(())
        }
    }

    pub fn record_dequeued(&self, len: usize) {
        self.pending_messages.fetch_sub(1, Ordering::Relaxed);
        self.pending_bytes.fetch_sub(len as u64, Ordering::Relaxed);
    }

    pub fn pending_messages(&self) -> u64 {
        self.pending_messages.load(Ordering::Relaxed)
    }

    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    fn try_reserve(&self, message_delta: u64, byte_delta: u64) -> bool {
        if !reserve_counter(&self.pending_messages, message_delta, self.max_pending_messages) {
            return false;
        }

        if reserve_counter(&self.pending_bytes, byte_delta, self.max_pending_bytes) {
            true
        } else {
            self.pending_messages.fetch_sub(message_delta, Ordering::Relaxed);
            false
        }
    }
}

fn reserve_counter(counter: &AtomicU64, delta: u64, max: u64) -> bool {
    loop {
        let current = counter.load(Ordering::Relaxed);
        let Some(next) = current.checked_add(delta) else {
            return false;
        };

        if next > max {
            return false;
        }

        if counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            return true;
        }
    }
}

/// Trait for database-specific wire protocol processing.
///
/// Implementors handle:
/// - Parsing protocol-specific frames (RESP for Redis, Postgres protocol, etc.)
/// - Classifying commands as read/write
/// - Applying command policies
/// - Formatting error responses in the database protocol format
///
/// Each database type (Redis, Postgres, MySQL) implements this trait to provide
/// its own protocol handling while the generic routing logic in `processor.rs`
/// remains database-agnostic.
///
/// # Unified Routing
///
/// The processor checks `interlay_endpoints` on each command to determine the current
/// routing state. This allows migrations to be started/stopped mid-connection without
/// requiring separate code paths.
pub trait DatabaseProtocolProcessor: GatewayResponsePolicy + Send + Sync {
    /// Process wire protocol with unified routing.
    ///
    /// The processor checks `interlay_endpoints` on each command to determine:
    /// - The current endpoint to route to
    /// - Whether a migration is active (and its configuration)
    /// - How to route reads vs writes during migration
    ///
    /// This unified approach handles both migration and non-migration cases,
    /// and allows migrations to be started/stopped mid-connection.
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[cfg(test)]
mod tests {
    use super::BytesQueueSender;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn send_releases_reservation_when_channel_is_closed() {
        let (tx, rx) = unbounded_channel();
        drop(rx);

        let sender = BytesQueueSender::new(tx, 4, 64);
        let err = sender.send(Bytes::from_static(b"ping")).expect_err("send should fail on closed channel");

        assert_eq!(err.0.as_ref(), b"ping");
        assert_eq!(sender.pending_messages(), 0);
        assert_eq!(sender.pending_bytes(), 0);
    }

    #[test]
    fn send_pair_reserves_one_message_for_both_chunks() {
        let (tx, mut rx) = unbounded_channel();
        let sender = BytesQueueSender::new(tx, 1, 64);

        sender
            .send_pair_with_request_received_at(Bytes::from_static(b"headers"), Bytes::from_static(b"body"), std::time::Instant::now())
            .expect("pair should count as one queued message");

        let queued = rx.try_recv().expect("paired chunk");
        assert_eq!(queued.len(), 11);
        assert_eq!(queued.into_bytes().as_ref(), b"headersbody");
        assert_eq!(sender.pending_messages(), 1);
        assert_eq!(sender.pending_bytes(), 11);

        sender.record_dequeued(11);
        assert_eq!(sender.pending_messages(), 0);
        assert_eq!(sender.pending_bytes(), 0);
    }

    #[test]
    fn send_pair_preserves_payload_parts() {
        let (tx, mut rx) = unbounded_channel();
        let sender = BytesQueueSender::new(tx, 4, 64);

        sender
            .send_pair_with_request_received_at(Bytes::from_static(b"headers"), Bytes::from_static(b"body"), std::time::Instant::now())
            .expect("pair should enqueue");

        let queued = rx.try_recv().expect("paired chunk");
        match queued.into_payload() {
            super::QueuedBytesPayload::Pair(first, second) => {
                assert_eq!(first.as_ref(), b"headers");
                assert_eq!(second.as_ref(), b"body");
            }
            super::QueuedBytesPayload::Single(bytes) => panic!("expected paired payload, got {bytes:?}"),
        }
        assert!(rx.try_recv().is_err());
        assert_eq!(sender.pending_messages(), 1);
        assert_eq!(sender.pending_bytes(), 11);

        sender.record_dequeued(11);
        assert_eq!(sender.pending_messages(), 0);
        assert_eq!(sender.pending_bytes(), 0);
    }

    #[test]
    fn send_with_request_received_at_preserves_command_count() {
        let (tx, mut rx) = unbounded_channel();
        let sender = BytesQueueSender::new(tx, 4, 64);

        sender
            .send_with_request_received_at_and_command_count(Bytes::from_static(b"+OK\r\n"), std::time::Instant::now(), 7)
            .expect("response should enqueue");

        let queued = rx.try_recv().expect("queued response");
        assert_eq!(queued.request_command_count(), 7);
        assert_eq!(queued.into_bytes().as_ref(), b"+OK\r\n");
    }

    #[test]
    fn concurrent_senders_do_not_oversubscribe_hard_limits() {
        let (tx, mut rx) = unbounded_channel();
        let sender = Arc::new(BytesQueueSender::new(tx, 8, 8));
        let barrier = Arc::new(Barrier::new(16));
        let successes = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..15 {
            let sender = sender.clone();
            let barrier = barrier.clone();
            let successes = successes.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                if sender.send(Bytes::from_static(b"xx")).is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        barrier.wait();

        for handle in handles {
            handle.join().expect("worker thread should join cleanly");
        }

        let successful_sends = successes.load(Ordering::Relaxed);
        assert_eq!(successful_sends, 4, "byte limit should cap concurrent reservations");
        assert_eq!(sender.pending_messages(), 4);
        assert_eq!(sender.pending_bytes(), 8);

        while let Ok(bytes) = rx.try_recv() {
            sender.record_dequeued(bytes.len());
        }

        assert_eq!(sender.pending_messages(), 0);
        assert_eq!(sender.pending_bytes(), 0);
    }
}
