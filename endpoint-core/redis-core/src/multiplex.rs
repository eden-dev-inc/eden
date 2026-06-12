//! Direct request multiplexer for Redis backend connections.
//!
//! A [`RedisDirectMultiplexer`] owns a fixed-then-autoscaling set of
//! [`DirectWorkerHandle`]s. Each worker holds one TCP connection to
//! the backend, split into independent writer / reader halves
//! ([`RedisClientWriter`] / [`RedisClientReader`]) so the wire
//! protocol can be pipelined: the writer task pumps requests onto the
//! socket without waiting for replies, while the reader task drains
//! responses in order from a shared FIFO ([`PendingFifoEntry`]). This
//! is what lets a single backend connection carry many in-flight
//! commands per client connection — Redis processes them in
//! submission order and the FIFO preserves response matching.
//!
//! ## Dispatch paths
//!
//! Two response delivery modes share the worker plumbing:
//! - **Slot path** ([`MultiplexInflight`] returned from
//!   [`RedisDirectMultiplexer::dispatch`]): the caller awaits the
//!   response on a [`ResponseSlot`], a one-shot signaling primitive
//!   tuned to keep the per-request scheduler wakeup off the hot path.
//! - **Sink path** ([`RedisDirectMultiplexer::dispatch_to_sink`]): the
//!   caller registers a [`DispatchResponseSink`] at dispatch time and
//!   never awaits — the worker reader pushes the response directly
//!   into the sink, removing a cross-thread wakeup of the dispatcher
//!   task. Used by the proxy bridge to short-circuit responses
//!   straight to the client write queue.
//!
//! ## Autoscaling
//!
//! The optional [`autoscaler_loop`] grows the worker set up to
//! `max_workers` when average in-flight per worker stays above the
//! configured threshold. Workers are appended (never shrunk while the
//! multiplexer is alive) so existing in-flight requests are never
//! reassigned.
//!
//! ## Failure handling
//!
//! On worker disconnect, in-flight requests on that worker observe a
//! [`ResponseSlot`] cancellation (slot path) or sink delivery of an
//! `Err` (sink path). Sinks are responsible for rendering RESP error
//! frames since they own the downstream channel format.

use crate::client::{RedisClient, RedisClientReader, RedisClientWriter};
use crate::connection::RedisConnection;
use arc_swap::ArcSwap;
use bytes::Bytes;
use eden_logger_internal::{LogAudience, LogContext, log_info, log_warn};
use error::{EpError, ResultEP};
use format::EndpointUuid;
use futures::task::AtomicWaker;
use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use telemetry::{AllMetrics, ProxyMultiplexSeries, global_metrics};
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{OwnedSemaphorePermit, mpsc};

const ENDPOINT_KIND_LABEL: &str = "redis";

/// Destination the worker reader writes a completed response into. Used
/// to bypass the processor on the response path: instead of fulfilling a
/// slot that the processor task awaits (which incurs a cross-thread
/// scheduler wakeup), the worker delivers the response directly to
/// whatever the caller registered — typically the bridge's
/// `BytesQueueSender`. Implementations are responsible for rendering
/// RESP error frames if `Err` is delivered, since they own the
/// downstream channel format.
///
/// Defined in `redis-core` so the multiplexer doesn't need to depend on
/// `eden-gateway-core` (where `BytesQueueSender` lives). The processor in
/// `eden_gateway/redis` provides the impl.
pub trait DispatchResponseSink: Send + Sync + 'static {
    /// Called exactly once per dispatch, by the worker reader on success
    /// or by the worker writer / supervisor on failure. `command_count`
    /// is needed when rendering error frames (one error slot per
    /// command in the original batch). `network_latency_us` is the
    /// backend-side wire latency (write start → read done); 0 on
    /// dispatch-time failures (no write happened).
    fn deliver(&self, response: Result<Bytes, EpError>, command_count: usize, request_received_at: Instant, network_latency_us: u64);
}

/// Resolve the tenant-scoped label slice used on every multiplexer metric.
/// Used at construction time only; the hot path uses
/// `RedisDirectMultiplexerInner::label_views`.
#[inline]
fn metric_labels<'a>(org_uuid: &'a str, endpoint_uuid: &'a str) -> [(&'static str, &'a str); 3] {
    [
        ("org_uuid", org_uuid),
        ("endpoint_uuid", endpoint_uuid),
        ("endpoint_kind", ENDPOINT_KIND_LABEL),
    ]
}

#[inline]
pub fn endpoint_multiplexer_label(endpoint_uuid: Option<&str>) -> &str {
    endpoint_uuid.unwrap_or("")
}

#[inline]
pub fn endpoint_multiplexer_label_for_uuid(endpoint_uuid: &EndpointUuid) -> String {
    endpoint_uuid.to_string()
}

// === Response slot ====================================================
//
// A purpose-built one-shot signaling primitive for handing the worker's
// response back to the dispatcher. Used in place of
// `tokio::sync::oneshot` to keep the per-request scheduler wakeup off
// the hot path; wakeup latency is exported as
// `multiplex_oneshot_delivery_microseconds`.
//
// The slot holds a single `WorkerResponse`. The worker fulfills it once;
// the dispatcher polls `ResponseSlotFuture` to read it. State transitions:
//   EMPTY -> FILLED  (worker called fulfill)
//   FILLED -> TAKEN  (poller read the value)
//   EMPTY -> CANCELED (request was dropped before a fulfill)
//
// Single-producer, single-consumer; ownership is enforced by the API
// (DirectRequest holds the producer Arc, MultiplexInflight holds the
// consumer Arc). The value lives in an UnsafeCell because the producer
// writes it before the state becomes FILLED, and the consumer reads it
// only after observing FILLED — the AtomicU8 state with Acquire/Release
// ordering provides the happens-before that lets us bypass a Mutex.

const SLOT_EMPTY: u8 = 0;
const SLOT_FILLED: u8 = 1;
const SLOT_TAKEN: u8 = 2;
const SLOT_CANCELED: u8 = 3;

struct ResponseSlot {
    state: AtomicU8,
    waker: AtomicWaker,
    value: UnsafeCell<MaybeUninit<WorkerResponse>>,
}

// SAFETY: producer/consumer protocol enforced by the API (the producer
// arrives at `state=FILLED` only after writing the value; the consumer
// observes `FILLED` only after the producer's Release store has paired
// with its Acquire load, so the UnsafeCell access is data-race-free).
unsafe impl Send for ResponseSlot {}
unsafe impl Sync for ResponseSlot {}

impl ResponseSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            waker: AtomicWaker::new(),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Producer-side: write the value and notify the consumer. Must be
    /// called at most once per slot; double-fulfill is a programmer error.
    fn fulfill(&self, value: WorkerResponse) {
        // SAFETY: caller guarantees single-fulfill; we are the only writer.
        unsafe {
            (*self.value.get()).write(value);
        }
        // Release: pairs with the consumer's Acquire load of `state`.
        // Once they observe FILLED they may read `value` safely.
        self.state.store(SLOT_FILLED, Ordering::Release);
        self.waker.wake();
    }

    /// Producer-side cancellation: signal the consumer that no fulfillment
    /// is coming, so its poll can wake up and return an error rather
    /// than hang. Used by the Drop impl on `DirectRequest` /
    /// `PendingFifoEntry` if the holder is dropped without an explicit
    /// fulfill (e.g., the request mpsc was dropped while requests were
    /// queued). Idempotent and races-safe with `fulfill`.
    fn cancel_if_unfulfilled(&self) {
        if self.state.compare_exchange(SLOT_EMPTY, SLOT_CANCELED, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            self.waker.wake();
        }
    }
}

impl Drop for ResponseSlot {
    fn drop(&mut self) {
        // If the slot is dropped while still holding an unread value
        // (consumer was dropped before take), free the inner resources.
        // Use the relaxed load is fine — we have exclusive access in Drop.
        if *self.state.get_mut() == SLOT_FILLED {
            // SAFETY: state was FILLED, value was initialized, never taken.
            unsafe {
                (*self.value.get()).assume_init_drop();
            }
        }
    }
}

/// Future for `MultiplexInflight::await_response`. Polls the slot's
/// state; returns the value (or a canceled-error placeholder) when the
/// producer signals.
struct ResponseSlotFuture {
    slot: Arc<ResponseSlot>,
}

impl Future for ResponseSlotFuture {
    type Output = WorkerResponse;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Fast path: already filled before the first poll. Avoids the
        // waker register cost when the worker beat us to the punch.
        let s = self.slot.state.load(Ordering::Acquire);
        if s == SLOT_FILLED {
            return Poll::Ready(take_filled(&self.slot));
        }
        if s == SLOT_CANCELED {
            return Poll::Ready((Err(EpError::request("redis multiplexer request canceled before fulfillment")), Instant::now()));
        }

        // Register waker, then re-check state. The re-check pairs with
        // the producer's Release store of `state` and closes the
        // register-then-wake race.
        self.slot.waker.register(cx.waker());
        let s = self.slot.state.load(Ordering::Acquire);
        match s {
            SLOT_FILLED => Poll::Ready(take_filled(&self.slot)),
            SLOT_CANCELED => Poll::Ready((Err(EpError::request("redis multiplexer request canceled before fulfillment")), Instant::now())),
            _ => Poll::Pending,
        }
    }
}

#[inline]
fn take_filled(slot: &ResponseSlot) -> WorkerResponse {
    // CAS FILLED -> TAKEN to ensure only one read of the UnsafeCell.
    // If we race with `Drop`, we still win because Drop runs only when
    // the last Arc is dropped — which can't happen while we hold one.
    match slot.state.compare_exchange(SLOT_FILLED, SLOT_TAKEN, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => {
            // SAFETY: state was FILLED → value initialized; we won the
            // CAS so we are the unique reader.
            unsafe { (*slot.value.get()).assume_init_read() }
        }
        Err(_) => unreachable!("response slot taken twice"),
    }
}

#[inline]
fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}

/// Try to resolve the global `AllMetrics` handle for telemetry emission. Returns
/// `None` before the proxy installs the handle (tests, early startup).
#[inline]
fn metrics_handle() -> Option<Arc<AllMetrics>> {
    global_metrics()
}

#[inline]
fn multiplex_series_for_endpoint_label(org_label: &str, endpoint_label: &str) -> Option<ProxyMultiplexSeries> {
    let metrics = metrics_handle()?;
    let labels = [
        ("org_uuid", org_label),
        ("endpoint_uuid", endpoint_label),
        ("endpoint_kind", ENDPOINT_KIND_LABEL),
    ];
    Some(metrics.proxy().multiplex_series(&labels))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisDirectMultiplexerConfig {
    /// Initial worker count at multiplexer construction. Autoscaler grows
    /// from here up to `max_workers` under load.
    pub worker_count: usize,
    /// Hard ceiling on how many workers the autoscaler may spawn. The
    /// multiplexer never shrinks below `worker_count` and never grows past
    /// `max_workers`.
    pub max_workers: usize,
    /// Maximum number of in-flight requests a single worker tolerates
    /// before it stops draining its request mpsc and waits for the
    /// reader to fulfill responses. Caps how deep one worker pipelines
    /// onto its backend connection.
    pub max_inflight_per_worker: usize,
    /// Capacity of each worker's request mpsc. Producers (dispatchers)
    /// observe backpressure once this fills, which is what triggers
    /// the autoscaler to consider growing the worker pool.
    pub queue_capacity_per_worker: usize,
    /// Maximum number of requests a worker writes to the backend in a
    /// single inner-loop burst before yielding to read responses.
    /// Bounds head-of-line blocking when many small commands arrive
    /// faster than the backend can reply.
    pub write_burst: usize,
    /// Cadence for the autoscaler's sampling loop, in milliseconds.
    pub scale_interval_ms: u64,
    /// Scale up when the average in-flight per worker exceeds this percent
    /// of `max_inflight_per_worker`.
    pub scale_up_threshold_percent: u32,
}

#[derive(Clone)]
pub struct RedisDirectMultiplexer {
    inner: Arc<RedisDirectMultiplexerInner>,
}

/// How worker / autoscaler tasks are spawned. Determines whether the
/// multiplexer runs on the shared multi-threaded runtime (`Global`) or
/// pinned to the *current* `LocalSet` (`Local`). The `Local` mode is
/// what powers per-shard multiplexers — every task on a shard's
/// multiplexer (workers, autoscaler) stays on the shard's runtime
/// thread, so dispatch and response delivery never cross threads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpawnMode {
    /// `tokio::spawn` — task may run on any worker thread of the
    /// multi-threaded runtime. Send is required.
    Global,
    /// `tokio::task::spawn_local` — task runs on the LocalSet of the
    /// runtime that called this. Caller must be inside a LocalSet.
    /// Send is *not* required; `'static` still is.
    Local,
}

impl SpawnMode {
    /// Spawn a future according to this mode. The future bound is
    /// `Send + 'static` so a single signature works for both modes
    /// (the global runtime requires Send; spawn_local accepts it too).
    fn spawn<F>(self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        match self {
            SpawnMode::Global => {
                tokio::spawn(fut);
            }
            SpawnMode::Local => {
                tokio::task::spawn_local(fut);
            }
        }
    }
}

struct RedisDirectMultiplexerInner {
    workers: ArcSwap<Vec<DirectWorkerHandle>>,
    next_worker: AtomicUsize,
    /// Sum of in-flight requests across all workers (incremented on accepted
    /// dispatch, decremented when the response oneshot completes).
    inflight: AtomicUsize,
    /// Monotonic worker-id counter used by the autoscaler when spawning
    /// additional workers; the field is also used as the running worker count
    /// for telemetry.
    next_worker_id: AtomicUsize,
    config: RedisDirectMultiplexerConfig,
    connection: RedisConnection,
    endpoint_uuid: Option<String>,
    /// Cached owned endpoint-uuid string used for metric labels.
    /// Constructed once at multiplexer creation; the hot path borrows
    /// it via `label_views()` instead of allocating per dispatch.
    cached_endpoint_label: String,
    /// Cached owned organization-uuid string used for metric labels.
    cached_org_label: String,
    /// Cached fast-telemetry series handles for hot-path multiplexer metrics.
    multiplex_series: OnceLock<ProxyMultiplexSeries>,
    /// How the autoscaler should spawn newly added workers — must match
    /// the mode used for the initial workers so the runtime affinity is
    /// preserved as the pool grows.
    spawn_mode: SpawnMode,
}

impl RedisDirectMultiplexerInner {
    /// Borrow the cached label slice in the form the metrics API expects.
    /// Returns owned-key arrays of `(&str, &str)` pointing into
    /// `cached_endpoint_label` and `ENDPOINT_KIND_LABEL`. No allocation.
    #[inline]
    fn label_views(&self) -> [(&str, &str); 3] {
        [
            ("org_uuid", self.cached_org_label.as_str()),
            ("endpoint_uuid", self.cached_endpoint_label.as_str()),
            ("endpoint_kind", ENDPOINT_KIND_LABEL),
        ]
    }

    #[inline]
    fn multiplex_series(&self) -> Option<&ProxyMultiplexSeries> {
        if let Some(series) = self.multiplex_series.get() {
            return Some(series);
        }

        let metrics = metrics_handle()?;
        let labels = self.label_views();
        Some(self.multiplex_series.get_or_init(|| metrics.proxy().multiplex_series(&labels)))
    }
}

impl RedisDirectMultiplexer {
    /// Construct a multiplexer whose worker tasks (and autoscaler) run on
    /// the global multi-threaded tokio runtime. Backwards-compatible default.
    pub fn new(connection: RedisConnection, org_uuid: String, endpoint_uuid: Option<String>, config: RedisDirectMultiplexerConfig) -> Self {
        Self::new_with_spawn_mode(connection, org_uuid, endpoint_uuid, config, SpawnMode::Global)
    }

    /// Construct a multiplexer whose worker tasks (and autoscaler) run on
    /// the *current* `LocalSet` via `tokio::task::spawn_local`. Must be
    /// called from inside a `LocalSet` (i.e. from a shard runtime). Used
    /// by the per-shard multiplexer registry: each shard thread builds
    /// its own multiplexer for an endpoint, and all of that endpoint's
    /// worker / autoscaler tasks stay pinned to the shard thread — no
    /// cross-thread wakeups on the request hot path.
    pub fn new_local(
        connection: RedisConnection,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        config: RedisDirectMultiplexerConfig,
    ) -> Self {
        Self::new_with_spawn_mode(connection, org_uuid, endpoint_uuid, config, SpawnMode::Local)
    }

    fn new_with_spawn_mode(
        connection: RedisConnection,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        config: RedisDirectMultiplexerConfig,
        spawn_mode: SpawnMode,
    ) -> Self {
        let cached_endpoint_label = endpoint_multiplexer_label(endpoint_uuid.as_deref()).to_string();
        let cached_org_label = org_uuid;
        let initial_workers: Vec<DirectWorkerHandle> = (0..config.worker_count)
            .map(|worker_id| {
                DirectWorkerHandle::spawn(
                    worker_id,
                    connection.clone(),
                    cached_org_label.clone(),
                    endpoint_uuid.clone(),
                    config,
                    spawn_mode,
                )
            })
            .collect();

        let inner = Arc::new(RedisDirectMultiplexerInner {
            workers: ArcSwap::from_pointee(initial_workers),
            next_worker: AtomicUsize::new(0),
            inflight: AtomicUsize::new(0),
            next_worker_id: AtomicUsize::new(config.worker_count),
            config,
            connection,
            endpoint_uuid,
            cached_endpoint_label,
            cached_org_label,
            multiplex_series: OnceLock::new(),
            spawn_mode,
        });

        // Autoscaler runs while the multiplexer is alive. It exits cleanly
        // when the strong count reaches zero, since it holds only a Weak ref.
        if config.max_workers > config.worker_count {
            let weak_inner = Arc::downgrade(&inner);
            spawn_mode.spawn(autoscaler_loop(weak_inner));
        }

        Self { inner }
    }

    /// Current worker count. Useful for tests and metrics emission.
    pub fn worker_count(&self) -> usize {
        self.inner.workers.load().len()
    }

    /// Current sum of in-flight requests across all workers.
    pub fn inflight(&self) -> usize {
        self.inner.inflight.load(Ordering::Relaxed)
    }

    /// Stable label for this multiplexer's endpoint. Used as the key
    /// into the per-shard `SHARD_MULTIPLEXERS` registry — every
    /// multiplexer pointing at the same backend endpoint shares this
    /// string.
    pub fn endpoint_label(&self) -> &str {
        &self.inner.cached_endpoint_label
    }

    /// Backend connection config (host/port/auth/etc) this multiplexer
    /// was constructed with. Exposed so the direct-proxy bridge mode
    /// can open a dedicated 1:1 client→backend connection without
    /// going through the multiplexer's worker pool.
    pub fn connection_config(&self) -> &RedisConnection {
        &self.inner.connection
    }

    /// Endpoint UUID this multiplexer was constructed with. Mirrors
    /// the Option<String> the multiplexer was created from; cheap to
    /// clone since it's already optional+owned.
    pub fn endpoint_uuid(&self) -> Option<String> {
        self.inner.endpoint_uuid.clone()
    }

    /// Construct a fresh `Local`-mode multiplexer that mirrors this
    /// one's connection config, endpoint id, and runtime config — but
    /// with `worker_count` and `max_workers` divided by `divisor` so
    /// the *total* backend connection budget across all shards
    /// matches the global multiplexer's configured budget instead of
    /// being multiplied by the shard count.
    ///
    /// Example: global config `worker_count=32, max_workers=32` and
    /// `divisor=4` (a 4-shard topology) produces a per-shard
    /// multiplexer with `worker_count=8, max_workers=8`. Aggregated
    /// across the 4 shards that's still 32 workers — same backend
    /// fan-out as the unsharded build, but spread across shard
    /// threads. Each value is floored at 1 so that very large shard
    /// counts (e.g. 24 shards with `worker_count=8`) still get at
    /// least one worker per shard rather than rounding down to zero.
    ///
    /// `pub(crate)` because misuse panics: must be called from inside
    /// a `LocalSet` (so `tokio::task::spawn_local` is valid) and on a
    /// multiplexer whose endpoint label is non-empty (so the per-shard
    /// registry doesn't collide with another labelless multiplexer).
    /// Both debug-asserted.
    pub(crate) fn local_clone(&self, divisor: usize) -> Self {
        debug_assert!(
            ep_core::runtime::is_shard_runtime(),
            "RedisDirectMultiplexer::local_clone must be called from a shard runtime thread; \
             tokio::task::spawn_local will panic outside a LocalSet",
        );
        debug_assert!(
            !self.inner.cached_endpoint_label.is_empty(),
            "RedisDirectMultiplexer::local_clone requires a non-empty endpoint label; \
             the per-shard registry keys on this string and would collide with other \
             labelless multiplexers, sending commands to the wrong backend",
        );
        let d = divisor.max(1);
        let mut config = self.inner.config;
        config.worker_count = (config.worker_count / d).max(1);
        config.max_workers = (config.max_workers / d).max(1);
        // The autoscaler can never grow past `max_workers`, so make
        // sure `worker_count <= max_workers` post-division.
        if config.worker_count > config.max_workers {
            config.worker_count = config.max_workers;
        }
        Self::new_local(
            self.inner.connection.clone(),
            self.inner.cached_org_label.clone(),
            self.inner.endpoint_uuid.clone(),
            config,
        )
    }

    /// Dispatch a request to a worker and return an in-flight handle that
    /// awaits the response separately. The processor uses this to overlap
    /// the next chunk's parse + dispatch with the prior chunk's response
    /// wait — the borrow-checker barrier on `&mut RedisClient` was the
    /// worker-side equivalent; this is the processor-side equivalent.
    ///
    /// Inflight accounting and `multi_total` recording move to
    /// `MultiplexInflight::await_response` so the metric still spans the
    /// full request lifetime. The existing `send()` API is preserved as
    /// a thin wrapper so callers that don't want pipelining keep working.
    pub async fn dispatch(&self, command_bytes: Bytes, command_count: usize) -> ResultEP<MultiplexInflight> {
        let total_start = Instant::now();

        // Increment in-flight; the matching decrement lives in
        // `MultiplexInflight::Drop` so cancellation paths (caller drops
        // the handle without awaiting) still balance the counter.
        self.inner.inflight.fetch_add(1, Ordering::Relaxed);

        let workers = self.inner.workers.load();
        let worker_count = workers.len();
        if worker_count == 0 {
            self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
            if let Some(series) = self.inner.multiplex_series() {
                series.record_dispatch_failure();
            }
            return Err(EpError::request("redis direct multiplexer has no workers"));
        }

        let start_index = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        let response_slot = Arc::new(ResponseSlot::new());
        let bus_send_start = Instant::now();
        let request = DirectRequest {
            command_bytes,
            command_count,
            response_target: Some(ResponseTarget::Slot(Arc::clone(&response_slot))),
            enqueued_at: bus_send_start,
            request_received_at: bus_send_start,
            // Slot path manages inflight via `MultiplexInflight::Drop`.
            sink_inflight_owner: None,
            completion_permit: None,
        };

        let mut pending_request = Some(request);
        for offset in 0..worker_count {
            let worker = &workers[(start_index + offset) % worker_count];
            if let Some(request_to_send) = pending_request.take() {
                match worker.sender.send(request_to_send).await {
                    Ok(()) => break,
                    Err(err) => pending_request = Some(err.0),
                }
            }
        }

        if pending_request.is_some() {
            self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
            if let Some(series) = self.inner.multiplex_series() {
                series.record_dispatch_failure();
            }
            return Err(EpError::request("redis direct multiplexer workers are unavailable"));
        }

        if let Some(series) = self.inner.multiplex_series() {
            series.record_bus_send(elapsed_us(bus_send_start));
            // Visibility into batch sizes; pipeline_wait scales with this.
            series.record_dispatch_command_count(command_count.min(u64::MAX as usize) as u64);
        }

        drop(workers);

        Ok(MultiplexInflight {
            response_slot,
            total_start,
            inner: Arc::clone(&self.inner),
            decremented: false,
        })
    }

    /// Dispatch + await in one step. Equivalent to the prior monolithic
    /// `send()` for callers that don't pipeline.
    pub async fn send(&self, command_bytes: Bytes, command_count: usize) -> ResultEP<(Bytes, u64)> {
        let inflight = self.dispatch(command_bytes, command_count).await?;
        inflight.await_response().await
    }

    /// Fire-and-forget dispatch: enqueue the request to a worker, then
    /// return immediately. The worker reader delivers the response
    /// directly to `sink` instead of an awaiter — no slot, no oneshot,
    /// no scheduler wakeup of the caller's task on the response path.
    ///
    /// `request_received_at` is the bridge-side timestamp threaded
    /// through to the response sender so end_to_end latency stays
    /// accurate (matches the `BytesQueueSender::send_with_request_received_at`
    /// contract).
    pub async fn dispatch_to_sink(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: Arc<dyn DispatchResponseSink>,
        request_received_at: Instant,
    ) -> ResultEP<()> {
        // Increment in-flight; matched in the worker reader after the
        // sink delivery (see PendingFifoEntry handling).
        self.inner.inflight.fetch_add(1, Ordering::Relaxed);

        let workers = self.inner.workers.load();
        let worker_count = workers.len();
        if worker_count == 0 {
            self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
            if let Some(series) = self.inner.multiplex_series() {
                series.record_dispatch_failure();
            }
            sink.deliver(
                Err(EpError::request("redis direct multiplexer has no workers")),
                command_count,
                request_received_at,
                0,
            );
            return Err(EpError::request("redis direct multiplexer has no workers"));
        }

        let start_index = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        let bus_send_start = Instant::now();
        let request = DirectRequest {
            command_bytes,
            command_count,
            response_target: Some(ResponseTarget::Sink(sink)),
            enqueued_at: bus_send_start,
            request_received_at,
            sink_inflight_owner: Some(Arc::clone(&self.inner)),
            completion_permit: None,
        };

        let mut pending_request = Some(request);
        for offset in 0..worker_count {
            let worker = &workers[(start_index + offset) % worker_count];
            if let Some(request_to_send) = pending_request.take() {
                match worker.sender.send(request_to_send).await {
                    Ok(()) => break,
                    Err(err) => pending_request = Some(err.0),
                }
            }
        }

        if let Some(req) = pending_request {
            self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
            if let Some(series) = self.inner.multiplex_series() {
                series.record_dispatch_failure();
            }
            // Deliver the failure via the sink so the caller's bridge
            // sees an error response in the correct order. Use take()
            // to move the field out without violating the Drop impl.
            let mut req = req;
            if let Some(target) = req.response_target.take()
                && let ResponseTarget::Sink(sink) = target
            {
                sink.deliver(
                    Err(EpError::request("redis direct multiplexer workers are unavailable")),
                    command_count,
                    request_received_at,
                    0,
                );
            }
            return Err(EpError::request("redis direct multiplexer workers are unavailable"));
        }

        if let Some(series) = self.inner.multiplex_series() {
            series.record_bus_send(elapsed_us(bus_send_start));
            series.record_dispatch_command_count(command_count.min(u64::MAX as usize) as u64);
        }

        drop(workers);
        Ok(())
    }

    /// Non-blocking sink dispatch. Returns as soon as the request is
    /// enqueued to a worker, or returns an error if every worker queue is
    /// currently full/unavailable. On enqueue failure the sink is not called.
    ///
    /// This is intended for best-effort side paths such as mirroring where
    /// the primary request must not inherit mirror backpressure.
    pub fn try_dispatch_to_sink(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: Arc<dyn DispatchResponseSink>,
        request_received_at: Instant,
    ) -> ResultEP<()> {
        self.try_dispatch_to_sink_with_optional_completion_permit(command_bytes, command_count, sink, request_received_at, None)
    }

    /// Non-blocking sink dispatch with a completion permit released when
    /// the worker drains or fails the response.
    pub fn try_dispatch_to_sink_with_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: Arc<dyn DispatchResponseSink>,
        request_received_at: Instant,
        completion_permit: OwnedSemaphorePermit,
    ) -> ResultEP<()> {
        self.try_dispatch_to_sink_with_optional_completion_permit(
            command_bytes,
            command_count,
            sink,
            request_received_at,
            Some(completion_permit),
        )
    }

    /// Non-blocking dispatch to a process-lifetime sink. Used by shard-local
    /// mirror targets so the hot path does not clone an `Arc` for each mirror
    /// request while still letting the worker drain the mirror response.
    pub fn try_dispatch_to_static_sink_with_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: &'static dyn DispatchResponseSink,
        request_received_at: Instant,
        completion_permit: OwnedSemaphorePermit,
    ) -> ResultEP<()> {
        self.try_dispatch_with_response_target_and_optional_completion_permit(
            command_bytes,
            command_count,
            ResponseTarget::StaticSink(sink),
            request_received_at,
            Some(completion_permit),
        )
    }

    /// Non-blocking dispatch to a process-lifetime sink that does not need
    /// successful response bytes. The worker reader drains the matching RESP
    /// frames to preserve Redis ordering, then delivers `Ok(Bytes::new())`.
    pub fn try_dispatch_to_static_discard_sink_with_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: &'static dyn DispatchResponseSink,
        request_received_at: Instant,
        completion_permit: OwnedSemaphorePermit,
    ) -> ResultEP<()> {
        self.try_dispatch_with_response_target_and_optional_completion_permit(
            command_bytes,
            command_count,
            ResponseTarget::StaticDiscardSink(sink),
            request_received_at,
            Some(completion_permit),
        )
    }

    fn try_dispatch_to_sink_with_optional_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: Arc<dyn DispatchResponseSink>,
        request_received_at: Instant,
        completion_permit: Option<OwnedSemaphorePermit>,
    ) -> ResultEP<()> {
        self.try_dispatch_with_response_target_and_optional_completion_permit(
            command_bytes,
            command_count,
            ResponseTarget::Sink(sink),
            request_received_at,
            completion_permit,
        )
    }

    fn try_dispatch_with_response_target_and_optional_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        response_target: ResponseTarget,
        request_received_at: Instant,
        completion_permit: Option<OwnedSemaphorePermit>,
    ) -> ResultEP<()> {
        self.inner.inflight.fetch_add(1, Ordering::Relaxed);

        let workers = self.inner.workers.load();
        let worker_count = workers.len();
        if worker_count == 0 {
            self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
            if let Some(series) = self.inner.multiplex_series() {
                series.record_dispatch_failure();
            }
            return Err(EpError::request("redis direct multiplexer has no workers"));
        }

        let start_index = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        let bus_send_start = Instant::now();
        let request = DirectRequest {
            command_bytes,
            command_count,
            response_target: Some(response_target),
            enqueued_at: bus_send_start,
            request_received_at,
            sink_inflight_owner: Some(Arc::clone(&self.inner)),
            completion_permit,
        };

        let mut pending_request = Some(request);
        for offset in 0..worker_count {
            let worker = &workers[(start_index + offset) % worker_count];
            if let Some(request_to_send) = pending_request.take() {
                match worker.sender.try_send(request_to_send) {
                    Ok(()) => {
                        if let Some(series) = self.inner.multiplex_series() {
                            series.record_bus_send(elapsed_us(bus_send_start));
                            series.record_dispatch_command_count(command_count.min(u64::MAX as usize) as u64);
                        }
                        drop(workers);
                        return Ok(());
                    }
                    Err(mpsc::error::TrySendError::Full(request)) | Err(mpsc::error::TrySendError::Closed(request)) => {
                        pending_request = Some(request);
                    }
                }
            }
        }

        drop(pending_request);
        if let Some(series) = self.inner.multiplex_series() {
            series.record_dispatch_failure();
        }
        Err(EpError::request("redis direct multiplexer workers are unavailable"))
    }
}

/// Handle to an in-flight multiplexer dispatch. `dispatch()` returns this
/// after the bus-send phase completes; the caller can hold many of these
/// concurrently to pipeline against the workers, then await their
/// responses in submission order to preserve per-connection RESP order.
///
/// Cancellation safety: dropping the handle without awaiting decrements
/// the inflight counter (in `Drop`) and lets the worker write the
/// response into a oneshot whose receiver is gone — same behavior as the
/// monolithic `send()` future being dropped mid-await.
pub struct MultiplexInflight {
    response_slot: Arc<ResponseSlot>,
    total_start: Instant,
    inner: Arc<RedisDirectMultiplexerInner>,
    /// Set true after `await_response` decrements the counter so `Drop`
    /// doesn't decrement again.
    decremented: bool,
}

impl MultiplexInflight {
    pub async fn await_response(mut self) -> ResultEP<(Bytes, u64)> {
        let fut = ResponseSlotFuture { slot: Arc::clone(&self.response_slot) };
        let (result, fulfilled_at) = fut.await;

        // Slot-wakeup latency: from the producer stamping `fulfilled_at`
        // just before storing into the slot to this future being polled
        // and reading the value. Isolates tokio scheduler delay on the
        // response path so it shows up directly instead of as residual
        // in `multi_total - sum_of_phases`.
        let oneshot_delivery_us = fulfilled_at.elapsed().as_micros().min(u64::MAX as u128) as u64;

        if let Some(series) = self.inner.multiplex_series() {
            series.record_oneshot_delivery(oneshot_delivery_us);
            series.record_total(elapsed_us(self.total_start));
        }

        // Decrement now (rather than relying on Drop) so the inflight
        // gauge reflects "still waiting for backend" exclusively.
        self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
        self.decremented = true;

        result
    }
}

impl Drop for MultiplexInflight {
    fn drop(&mut self) {
        if !self.decremented {
            self.inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Autoscaler control loop. Wakes on a fixed cadence, checks in-flight
/// load against the configured scale-up threshold, and appends a new
/// worker when the threshold is sustained.
///
/// Exit conditions:
/// - The `Weak<Inner>` upgrade fails (multiplexer dropped).
/// - Worker count has reached `max_workers` AND the threshold is below the
///   trigger (no further work to do; wait for next tick anyway).
async fn autoscaler_loop(weak_inner: std::sync::Weak<RedisDirectMultiplexerInner>) {
    let interval_ms = match weak_inner.upgrade() {
        Some(inner) => inner.config.scale_interval_ms,
        None => return,
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first immediate tick so we don't scale right at startup.
    ticker.tick().await;

    let ctx = LogContext::default().with_feature("redis_multiplexer_autoscaler");

    loop {
        ticker.tick().await;
        let Some(inner) = weak_inner.upgrade() else {
            return;
        };

        let current = inner.workers.load();
        let worker_count = current.len();
        let inflight = inner.inflight.load(Ordering::Relaxed);

        // Update gauges every tick so the dashboard reflects current state
        // even when no scale event fires.
        let metrics = metrics_handle();
        let endpoint_label_owned = endpoint_multiplexer_label(inner.endpoint_uuid.as_deref()).to_string();
        let labels = metric_labels(&inner.cached_org_label, &endpoint_label_owned);
        if let Some(m) = metrics.as_deref() {
            m.proxy().set_multiplex_workers(worker_count as i64, &labels);
            m.proxy().set_multiplex_inflight(inflight as i64, &labels);
        }

        if worker_count >= inner.config.max_workers {
            continue;
        }

        let avg_inflight = inflight / worker_count.max(1);
        let trigger_inflight =
            (inner.config.max_inflight_per_worker.saturating_mul(inner.config.scale_up_threshold_percent as usize)) / 100;

        if avg_inflight < trigger_inflight {
            continue;
        }

        let next_id = inner.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let new_worker = DirectWorkerHandle::spawn(
            next_id,
            inner.connection.clone(),
            inner.cached_org_label.clone(),
            inner.endpoint_uuid.clone(),
            inner.config,
            inner.spawn_mode,
        );

        let mut next_workers: Vec<DirectWorkerHandle> = (**current).clone();
        next_workers.push(new_worker);
        let new_count = next_workers.len();
        inner.workers.store(Arc::new(next_workers));

        if let Some(m) = metrics.as_deref() {
            m.proxy().record_multiplex_scale_up(&labels);
            m.proxy().set_multiplex_workers(new_count as i64, &labels);
        }

        log_info!(
            ctx.clone(),
            "Redis multiplexer scaled up",
            audience = LogAudience::Internal,
            from_workers = worker_count,
            to_workers = new_count,
            inflight = inflight,
            avg_inflight_per_worker = avg_inflight,
            trigger_inflight_per_worker = trigger_inflight,
            max_inflight_per_worker = inner.config.max_inflight_per_worker,
            max_workers = inner.config.max_workers
        );

        if new_count >= inner.config.max_workers {
            log_warn!(
                ctx.clone(),
                "Redis multiplexer reached max worker count",
                audience = LogAudience::Internal,
                max_workers = inner.config.max_workers
            );
        }
    }
}

#[derive(Clone)]
struct DirectWorkerHandle {
    sender: mpsc::Sender<DirectRequest>,
}

impl DirectWorkerHandle {
    fn spawn(
        worker_id: usize,
        connection: RedisConnection,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        config: RedisDirectMultiplexerConfig,
        spawn_mode: SpawnMode,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity_per_worker);
        spawn_mode.spawn(async move {
            DirectWorker::new(connection, org_uuid, endpoint_uuid, config, receiver, worker_id, spawn_mode).run().await;
        });
        Self { sender }
    }
}

/// Response payload handed back through the slot. The `fulfilled_at`
/// timestamp is stamped by the producer (reader on success, writer on
/// dispatch failure, etc.) immediately before storing into the slot, so
/// the consumer can compute slot wakeup latency.
type WorkerResponse = (Result<(Bytes, u64), EpError>, Instant);

/// Where the worker delivers the response. Two modes:
/// - `Slot`: dispatcher awaits on a `MultiplexInflight` — the worker
///   fulfills the slot and the awaiter polls it.
/// - `Sink`: dispatcher registers a destination at dispatch time and
///   never awaits — the worker pushes the response directly into the
///   sink (e.g., the bridge's response queue), avoiding a cross-thread
///   wakeup of the dispatcher.
/// - `StaticSink`: same delivery semantics as `Sink`, but the sink is a
///   process-lifetime shard-local mirror target. This avoids per-request
///   `Arc` cloning on best-effort mirror dispatch.
/// - `StaticDiscardSink`: process-lifetime sink whose success path only needs
///   drain/latency/error accounting, not backend response bytes.
enum ResponseTarget {
    Slot(Arc<ResponseSlot>),
    Sink(Arc<dyn DispatchResponseSink>),
    StaticSink(&'static dyn DispatchResponseSink),
    StaticDiscardSink(&'static dyn DispatchResponseSink),
}

impl ResponseTarget {
    fn discards_success_bytes(&self) -> bool {
        matches!(self, ResponseTarget::StaticDiscardSink(_))
    }

    /// Deliver a response via whichever destination is registered.
    /// `network_latency_us` is the backend wire round-trip from the
    /// worker's `started_at` to read-done; threaded through to slot
    /// callers (as the second tuple element of `(Bytes, u64)`) and to
    /// sink implementations for telemetry.
    fn deliver(&self, result: Result<Bytes, EpError>, command_count: usize, request_received_at: Instant, network_latency_us: u64) {
        match self {
            ResponseTarget::Slot(slot) => {
                let payload: WorkerResponse = (result.map(|bytes| (bytes, network_latency_us)), Instant::now());
                slot.fulfill(payload);
            }
            ResponseTarget::Sink(sink) => {
                sink.deliver(result, command_count, request_received_at, network_latency_us);
            }
            ResponseTarget::StaticSink(sink) => {
                sink.deliver(result, command_count, request_received_at, network_latency_us);
            }
            ResponseTarget::StaticDiscardSink(sink) => {
                sink.deliver(result, command_count, request_received_at, network_latency_us);
            }
        }
    }

    /// On drop without delivery, signal the awaiter (slot path only —
    /// sink path's caller doesn't await, so there's nothing to cancel).
    fn cancel_if_unfulfilled(&self) {
        if let ResponseTarget::Slot(slot) = self {
            slot.cancel_if_unfulfilled();
        }
    }
}

struct DirectRequest {
    command_bytes: Bytes,
    command_count: usize,
    /// `Option` so the writer can `.take()` it when handing off
    /// responsibility to a `PendingFifoEntry`. Drop sees `None` and
    /// skips cancellation; callers that successfully fulfill or
    /// transfer always leave the field empty.
    response_target: Option<ResponseTarget>,
    /// Wall-clock at which the request was placed into the worker's mpsc
    /// channel. Used to compute the worker-pickup phase metric.
    enqueued_at: Instant,
    /// Bridge-side timestamp from when the bytes were first observed,
    /// threaded through so the worker can record end_to_end latency on
    /// the sink path (slot path uses started_at instead).
    request_received_at: Instant,
    /// Reference to the multiplexer's inner state — used to decrement
    /// `inflight` in `Drop` on the sink path. The slot path manages
    /// inflight via `MultiplexInflight::Drop`, so the sink path owns
    /// this responsibility itself. `Some` only for sink-path requests.
    sink_inflight_owner: Option<Arc<RedisDirectMultiplexerInner>>,
    /// Optional per-request resource guard released when the response
    /// path drains or fails. Used by best-effort mirror dispatch to hold
    /// a semaphore permit without allocating a unique sink object.
    completion_permit: Option<OwnedSemaphorePermit>,
}

impl Drop for DirectRequest {
    fn drop(&mut self) {
        if let Some(target) = self.response_target.take() {
            target.cancel_if_unfulfilled();
        }
        // Sink-path inflight is decremented here (slot-path decrements
        // via `MultiplexInflight::Drop`).
        if let Some(inner) = self.sink_inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
        drop(self.completion_permit.take());
    }
}

/// FIFO entry that flows from the writer task to the reader task. The
/// writer stamps `started_at` (write begin) and `written_at` (write
/// end), pushes the entry onto the channel, and moves on to the next
/// request. The reader pops the head, reads `command_count` RESP frames
/// off the wire, and fulfills `response_tx` — emitting `pipeline_wait`
/// (written_at → read_done) and returning the network-latency total
/// (started_at → read_done) for downstream metrics.
struct PendingFifoEntry {
    command_count: usize,
    started_at: Instant,
    written_at: Instant,
    request_received_at: Instant,
    response_target: Option<ResponseTarget>,
    /// Sink-path inflight ownership. Same semantics as on `DirectRequest`.
    sink_inflight_owner: Option<Arc<RedisDirectMultiplexerInner>>,
    /// Optional per-request resource guard. Dropped with this FIFO entry
    /// after response delivery or failure.
    completion_permit: Option<OwnedSemaphorePermit>,
}

impl Drop for PendingFifoEntry {
    fn drop(&mut self) {
        if let Some(target) = self.response_target.take() {
            target.cancel_if_unfulfilled();
        }
        if let Some(inner) = self.sink_inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
        drop(self.completion_permit.take());
    }
}

/// Reason a writer session terminated. The supervisor uses this to decide
/// between a clean exit (request channel closed) and a reconnect (any I/O
/// failure on the writer side or a reader-task disappearance).
enum WriterSessionExit {
    /// The request mpsc was closed by the multiplexer — graceful shutdown.
    ChannelClosed,
    /// Wire write failed; both halves are unusable, supervisor reconnects.
    WriteFailed(EpError),
    /// Reader task hung up (its end of the FIFO closed); reconnect.
    ReaderDied,
}

struct DirectWorker {
    connection: RedisConnection,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    config: RedisDirectMultiplexerConfig,
    receiver: mpsc::Receiver<DirectRequest>,
    #[allow(dead_code)]
    worker_id: usize,
    /// Inherited from the multiplexer so the per-session reader task is
    /// spawned with the same affinity (Global → tokio::spawn,
    /// Local → spawn_local on the shard's LocalSet).
    spawn_mode: SpawnMode,
}

impl DirectWorker {
    fn new(
        connection: RedisConnection,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        config: RedisDirectMultiplexerConfig,
        receiver: mpsc::Receiver<DirectRequest>,
        worker_id: usize,
        spawn_mode: SpawnMode,
    ) -> Self {
        Self {
            connection,
            org_uuid,
            endpoint_uuid,
            config,
            receiver,
            worker_id,
            spawn_mode,
        }
    }

    /// Worker supervisor. Owns the request mpsc receiver across reconnects;
    /// each iteration of the outer loop opens a fresh backend connection,
    /// splits it into reader/writer halves, spawns the reader task, and
    /// runs the writer loop in this task. On any I/O failure, both halves
    /// drop and the loop reconnects — preserving the prior behavior of
    /// "worker stays alive across disconnects" without using `&mut self.client`
    /// to serialize writes against reads.
    async fn run(mut self) {
        let endpoint_label_owned = endpoint_multiplexer_label(self.endpoint_uuid.as_deref()).to_string();
        let org_label_owned = self.org_uuid.clone();

        loop {
            // 1. Connect. On failure, brief sleep and retry; the request
            //    channel close path is detected via `recv()` returning
            //    `None` inside `run_writer_session` once we're back in the
            //    happy path.
            let client =
                match RedisClient::connect_with_org_endpoint(&self.connection, self.org_uuid.clone(), self.endpoint_uuid.clone()).await {
                    Ok(c) => c,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        if self.receiver.is_closed() {
                            return;
                        }
                        continue;
                    }
                };

            // 2. Split the client into independent halves. The bounded FIFO
            //    capacity caps in-flight commands per worker — once the
            //    reader is `max_inflight_per_worker` behind, the writer
            //    backpressures on `fifo_tx.send().await` until the reader
            //    drains.
            let (mut writer, reader) = client.into_split();
            let (fifo_tx, fifo_rx) = mpsc::channel::<PendingFifoEntry>(self.config.max_inflight_per_worker.max(1));

            // 3. Spawn the reader task. It owns the read half + framing
            //    buffer; on any read error it fails its current entry,
            //    drains the rest of the FIFO with the same error, and exits.
            //    Spawn affinity follows the worker's `spawn_mode` so a
            //    Local-mode worker keeps its reader on the same shard
            //    thread.
            let reader_label = endpoint_label_owned.clone();
            let reader_org_label = org_label_owned.clone();
            let reader_fut = reader_loop(reader, fifo_rx, reader_org_label, reader_label);
            let reader_handle = match self.spawn_mode {
                SpawnMode::Global => tokio::spawn(reader_fut),
                SpawnMode::Local => tokio::task::spawn_local(reader_fut),
            };

            // 4. Run the writer loop in this task. Returns when the
            //    request channel closes (graceful) or write fails / reader
            //    disappears (reconnect).
            let exit =
                run_writer_session(&mut self.receiver, &mut writer, &fifo_tx, self.config, &org_label_owned, &endpoint_label_owned).await;

            // 5. Drop the writer's FIFO sender so the reader sees its end
            //    closed and exits gracefully if it isn't already on its
            //    way out from a read failure.
            drop(fifo_tx);
            let _ = reader_handle.await;

            match exit {
                WriterSessionExit::ChannelClosed => return,
                WriterSessionExit::WriteFailed(error) => {
                    log_warn!(
                        LogContext::default().with_feature("redis_multiplexer"),
                        "Redis multiplexer writer session ended on write failure, reconnecting",
                        audience = LogAudience::Internal,
                        endpoint = endpoint_label_owned.clone(),
                        error = error.to_string()
                    );
                }
                WriterSessionExit::ReaderDied => {
                    log_warn!(
                        LogContext::default().with_feature("redis_multiplexer"),
                        "Redis multiplexer reader task disappeared, reconnecting",
                        audience = LogAudience::Internal,
                        endpoint = endpoint_label_owned.clone()
                    );
                }
            }
        }
    }
}

/// Drive the write side of one connection's lifetime. Pulls requests from
/// the worker's mpsc, writes each to the wire, and pushes a `PendingFifoEntry`
/// onto the bounded FIFO that the reader task drains. The writer never
/// awaits a response; that's the entire point of the split.
async fn run_writer_session(
    receiver: &mut mpsc::Receiver<DirectRequest>,
    writer: &mut RedisClientWriter,
    fifo_tx: &mpsc::Sender<PendingFifoEntry>,
    config: RedisDirectMultiplexerConfig,
    org_label: &str,
    endpoint_label: &str,
) -> WriterSessionExit {
    let multiplex_series = multiplex_series_for_endpoint_label(org_label, endpoint_label);
    let write_burst = config.write_burst.max(1);

    // Reusable batch buffer: holds up to `write_burst` requests drained
    // from the mpsc each iteration. Pre-allocated so the hot path doesn't
    // allocate per write cycle.
    let mut batch: Vec<DirectRequest> = Vec::with_capacity(write_burst);

    loop {
        // Block for the first request — every iteration must have at least
        // one. After this, we *opportunistically* drain more from the mpsc
        // without yielding so a single TCP write can carry the full burst.
        // This matters especially on single-thread shard runtimes where the
        // writer task gets descheduled between iterations: without batching,
        // each request becomes its own `write()` syscall and redis sees a
        // gappy arrival pattern, wasting backend CPU on per-event-loop
        // bookkeeping. With batching, the writer drains its accumulated
        // backlog in one syscall whenever it gets to run.
        let first = match receiver.recv().await {
            Some(r) => r,
            None => return WriterSessionExit::ChannelClosed,
        };
        batch.clear();
        batch.push(first);
        while batch.len() < write_burst {
            match receiver.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }

        // Worker-pickup phase: per-request enqueue → writer dequeues.
        // Recorded once per request, not once per batch.
        if let Some(series) = multiplex_series.as_ref() {
            for r in &batch {
                series.record_worker_pickup(elapsed_us(r.enqueued_at));
            }
        }

        // Concatenate all command bytes from the batch into a single buffer
        // for one TCP write. Per-request FIFO entries are still pushed
        // individually below so response routing is preserved.
        let total_bytes: usize = batch.iter().map(|r| r.command_bytes.len()).sum();
        let mut combined = bytes::BytesMut::with_capacity(total_bytes);
        for r in &batch {
            combined.extend_from_slice(&r.command_bytes);
        }
        let combined = combined.freeze();

        let started_at = Instant::now();
        if let Err(error) = writer.write_command_raw_no_response(&combined).await {
            // Deliver the error via every registered destination in the
            // batch — none of them got a backend wire round-trip, so
            // network_latency_us is 0 for all.
            for mut r in batch.drain(..) {
                if let Some(target) = r.response_target.take() {
                    target.deliver(Err(error.clone()), r.command_count, r.request_received_at, 0);
                }
            }
            return WriterSessionExit::WriteFailed(error);
        }
        let written_at = Instant::now();

        if let Some(series) = multiplex_series.as_ref() {
            series.record_write(written_at.duration_since(started_at).as_micros().min(u64::MAX as u128) as u64);
        }

        // One PendingFifoEntry per request. All entries from this batch
        // share the same `started_at`/`written_at` (they went out in the
        // same TCP write); each keeps its own `request_received_at`,
        // `response_target`, and `sink_inflight_owner`. Order matters:
        // redis returns responses in the order requests were written,
        // and the FIFO preserves that ordering.
        for mut r in batch.drain(..) {
            let response_target = r.response_target.take().expect("response_target must be present until we take it");
            let entry = PendingFifoEntry {
                command_count: r.command_count,
                started_at,
                written_at,
                request_received_at: r.request_received_at,
                response_target: Some(response_target),
                sink_inflight_owner: r.sink_inflight_owner.take(),
                completion_permit: r.completion_permit.take(),
            };

            if let Err(send_err) = fifo_tx.send(entry).await {
                // Reader died mid-batch. Deliver an error to this entry
                // and any remaining requests still in `batch` (the drain
                // iterator hasn't consumed them yet — but we're inside a
                // for loop over a Drain, so `batch` is empty after we
                // exit; deliver via the returned entry only here).
                let mut returned_entry = send_err.0;
                if let Some(target) = returned_entry.response_target.take() {
                    target.deliver(
                        Err(EpError::request("redis multiplexer reader task unavailable")),
                        returned_entry.command_count,
                        returned_entry.request_received_at,
                        0,
                    );
                }
                return WriterSessionExit::ReaderDied;
            }
        }
    }
}

/// Drive the read side of one connection's lifetime. Pops the FIFO, reads
/// the matching number of RESP frames, fulfills the requestor's slot,
/// repeats. On any read error or FIFO close, drains remaining entries with
/// errors and exits — letting the supervisor reconnect.
async fn reader_loop(
    mut reader: RedisClientReader,
    mut fifo_rx: mpsc::Receiver<PendingFifoEntry>,
    org_label: String,
    endpoint_label: String,
) {
    let multiplex_series = multiplex_series_for_endpoint_label(&org_label, &endpoint_label);

    loop {
        let mut entry = match fifo_rx.recv().await {
            Some(e) => e,
            None => return, // Writer dropped its end — graceful shutdown.
        };

        let discards_success_bytes = entry.response_target.as_ref().is_some_and(ResponseTarget::discards_success_bytes);
        let response_result = if discards_success_bytes {
            reader.discard_response_group(entry.command_count).await.map(|()| Bytes::new())
        } else {
            reader.read_response_group_raw_bytes(entry.command_count).await
        };

        match response_result {
            Ok(response_bytes) => {
                let read_done = Instant::now();
                let pipeline_wait_us = read_done.duration_since(entry.written_at).as_micros().min(u64::MAX as u128) as u64;
                let network_latency_us = read_done.duration_since(entry.started_at).as_micros().min(u64::MAX as u128) as u64;
                if let Some(series) = multiplex_series.as_ref() {
                    series.record_pipeline_wait(pipeline_wait_us);
                }
                if let Some(target) = entry.response_target.take() {
                    target.deliver(Ok(response_bytes), entry.command_count, entry.request_received_at, network_latency_us);
                }
            }
            Err(error) => {
                if let Some(target) = entry.response_target.take() {
                    target.deliver(Err(error.clone()), entry.command_count, entry.request_received_at, 0);
                }
                while let Ok(mut remaining) = fifo_rx.try_recv() {
                    if let Some(target) = remaining.response_target.take() {
                        target.deliver(Err(error.clone()), remaining.command_count, remaining.request_received_at, 0);
                    }
                }
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-shard multiplexer registry.
//
// Each shard runtime thread owns a thread_local map of endpoint label →
// `RedisDirectMultiplexer`. The first dispatch from a shard for a given
// endpoint constructs a multiplexer there via `RedisDirectMultiplexer::new_local`
// (workers + autoscaler stay pinned to the shard's LocalSet); subsequent
// dispatches reuse it.
//
// Lifecycle: lazy on first lookup. Cleared on demand via `clear_shard_multiplexers`
// (used by Step 5's migration-update signal handling so a shard rebuilds
// its multiplexer against the new endpoint config).
// ---------------------------------------------------------------------------

use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static SHARD_MULTIPLEXERS: RefCell<HashMap<String, RedisDirectMultiplexer>> = RefCell::default();
}

/// Look up the current shard's multiplexer for `endpoint_label`, creating
/// one via `factory` on first call. Caller must invoke this from inside a
/// `LocalSet` (i.e. on a shard runtime thread) and the factory should
/// build the multiplexer with `RedisDirectMultiplexer::new_local`.
///
/// Returned multiplexer is `Clone` (cheap — Arc inside) so callers may
/// keep it for the duration of a request without holding the registry
/// borrow.
pub fn shard_multiplexer_or_init<F>(endpoint_label: &str, factory: F) -> RedisDirectMultiplexer
where
    F: FnOnce() -> RedisDirectMultiplexer,
{
    SHARD_MULTIPLEXERS.with(|cell| {
        if let Some(existing) = cell.borrow().get(endpoint_label) {
            return existing.clone();
        }
        let mux = factory();
        cell.borrow_mut().insert(endpoint_label.to_string(), mux.clone());
        mux
    })
}

/// Remove the multiplexer for `endpoint_label` from the current shard's
/// registry, if present. Used on migration / endpoint reconfiguration so
/// the next dispatch from this shard rebuilds against the new connection
/// config. Dropping the multiplexer's last clone tears down its workers.
pub fn shard_multiplexer_evict(endpoint_label: &str) -> Option<RedisDirectMultiplexer> {
    SHARD_MULTIPLEXERS.with(|cell| cell.borrow_mut().remove(endpoint_label))
}

/// Drop every multiplexer in the current shard's registry. Test helper
/// and bulk-invalidation path; production paths should prefer the more
/// targeted `shard_multiplexer_evict`.
pub fn clear_shard_multiplexers() {
    SHARD_MULTIPLEXERS.with(|cell| cell.borrow_mut().clear());
}

/// Number of multiplexers currently cached on the calling shard. Test /
/// telemetry helper.
pub fn shard_multiplexer_len() -> usize {
    SHARD_MULTIPLEXERS.with(|cell| cell.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use format::EdenUuid;
    use redis_protocol::resp2::decode::decode as decode_resp2;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_ping_server() -> Result<(RedisConnection, oneshot::Receiver<usize>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (accepted_tx, accepted_rx) = oneshot::channel();

        tokio::spawn(async move {
            let mut accepted_connections = 0usize;
            let mut accepted_sender = Some(accepted_tx);

            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                accepted_connections += 1;
                if let Some(sender) = accepted_sender.take() {
                    let _ = sender.send(accepted_connections);
                }

                tokio::spawn(async move {
                    let mut buffer = BytesMut::with_capacity(8192);
                    let mut read_buffer = [0u8; 4096];

                    loop {
                        let Ok(bytes_read) = stream.read(&mut read_buffer).await else {
                            break;
                        };
                        if bytes_read == 0 {
                            break;
                        }
                        buffer.extend_from_slice(&read_buffer[..bytes_read]);

                        let mut complete_frames = 0usize;
                        loop {
                            match decode_resp2(&buffer) {
                                Ok(Some((_, consumed))) => {
                                    complete_frames += 1;
                                    let _ = buffer.split_to(consumed);
                                }
                                Ok(None) => break,
                                Err(_) => break,
                            }
                        }

                        if complete_frames > 0 {
                            let mut response = Vec::with_capacity(complete_frames * 7);
                            for _ in 0..complete_frames {
                                response.extend_from_slice(b"+PONG\r\n");
                            }
                            if stream.write_all(&response).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        });

        Ok((
            RedisConnection {
                host: addr.ip().to_string(),
                port: Some(addr.port()),
                protocol_version: Some(2),
                ..Default::default()
            },
            accepted_rx,
        ))
    }

    #[tokio::test]
    async fn multiplexed_worker_reuses_single_backend_connection_for_concurrent_requests() {
        let (connection, accepted_rx) = spawn_ping_server().await.expect("server should start");
        let multiplexer = RedisDirectMultiplexer::new(
            connection,
            "test-org".to_string(),
            None,
            RedisDirectMultiplexerConfig {
                worker_count: 1,
                max_workers: 1,
                max_inflight_per_worker: 128,
                queue_capacity_per_worker: 128,
                write_burst: 32,
                scale_interval_ms: 100,
                scale_up_threshold_percent: 75,
            },
        );

        let request = Bytes::from_static(b"*1\r\n$4\r\nPING\r\n");
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let multiplexer = multiplexer.clone();
            let request = request.clone();
            tasks.push(tokio::spawn(async move { multiplexer.send(request, 1).await.expect("request should succeed") }));
        }

        for task in tasks {
            let (response, _) = task.await.expect("task should complete");
            assert_eq!(response, Bytes::from_static(b"+PONG\r\n"));
        }

        let accepted_connections = accepted_rx.await.expect("should report accepted connections");
        assert_eq!(accepted_connections, 1);
    }

    #[tokio::test]
    async fn multiplexed_worker_preserves_pipeline_response_grouping() {
        let (connection, _accepted_rx) = spawn_ping_server().await.expect("server should start");
        let multiplexer = RedisDirectMultiplexer::new(
            connection,
            "test-org".to_string(),
            None,
            RedisDirectMultiplexerConfig {
                worker_count: 1,
                max_workers: 1,
                max_inflight_per_worker: 128,
                queue_capacity_per_worker: 128,
                write_burst: 32,
                scale_interval_ms: 100,
                scale_up_threshold_percent: 75,
            },
        );

        let (response, _) = multiplexer
            .send(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"), 2)
            .await
            .expect("pipelined request should succeed");

        assert_eq!(response, Bytes::from_static(b"+PONG\r\n+PONG\r\n"));
    }

    struct TestSink {
        tx: std::sync::Mutex<Option<oneshot::Sender<Result<Bytes, EpError>>>>,
    }

    impl DispatchResponseSink for TestSink {
        fn deliver(
            &self,
            response: Result<Bytes, EpError>,
            _command_count: usize,
            _request_received_at: Instant,
            _network_latency_us: u64,
        ) {
            if let Some(tx) = self.tx.lock().expect("test sink mutex should not be poisoned").take() {
                let _ = tx.send(response);
            }
        }
    }

    #[tokio::test]
    async fn try_dispatch_to_sink_enqueues_without_awaiting_response() {
        let (connection, _accepted_rx) = spawn_ping_server().await.expect("server should start");
        let multiplexer = RedisDirectMultiplexer::new(
            connection,
            "test-org".to_string(),
            None,
            RedisDirectMultiplexerConfig {
                worker_count: 1,
                max_workers: 1,
                max_inflight_per_worker: 128,
                queue_capacity_per_worker: 128,
                write_burst: 32,
                scale_interval_ms: 100,
                scale_up_threshold_percent: 75,
            },
        );
        let (tx, rx) = oneshot::channel();
        let sink = Arc::new(TestSink { tx: std::sync::Mutex::new(Some(tx)) });

        multiplexer
            .try_dispatch_to_sink(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"), 1, sink, Instant::now())
            .expect("try dispatch should enqueue when worker queue has capacity");

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("sink should receive drained response")
            .expect("sink sender should remain open")
            .expect("backend response should be successful");
        assert_eq!(delivered, Bytes::from_static(b"+PONG\r\n"));
    }

    #[tokio::test]
    async fn try_dispatch_to_static_sink_enqueues_without_arc_sink() {
        let (connection, _accepted_rx) = spawn_ping_server().await.expect("server should start");
        let multiplexer = RedisDirectMultiplexer::new(
            connection,
            "test-org".to_string(),
            None,
            RedisDirectMultiplexerConfig {
                worker_count: 1,
                max_workers: 1,
                max_inflight_per_worker: 128,
                queue_capacity_per_worker: 128,
                write_burst: 32,
                scale_interval_ms: 100,
                scale_up_threshold_percent: 75,
            },
        );
        let (tx, rx) = oneshot::channel();
        let sink = Box::leak(Box::new(TestSink { tx: std::sync::Mutex::new(Some(tx)) }));
        let permit = Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned().expect("test semaphore should have a permit");

        multiplexer
            .try_dispatch_to_static_sink_with_completion_permit(
                Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
                1,
                sink,
                Instant::now(),
                permit,
            )
            .expect("try dispatch should enqueue when worker queue has capacity");

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("static sink should receive drained response")
            .expect("static sink sender should remain open")
            .expect("backend response should be successful");
        assert_eq!(delivered, Bytes::from_static(b"+PONG\r\n"));
    }

    fn small_config() -> RedisDirectMultiplexerConfig {
        RedisDirectMultiplexerConfig {
            worker_count: 1,
            max_workers: 1,
            max_inflight_per_worker: 8,
            queue_capacity_per_worker: 8,
            write_burst: 4,
            scale_interval_ms: 100,
            scale_up_threshold_percent: 75,
        }
    }

    #[test]
    fn endpoint_multiplexer_label_preserves_existing_inputs() {
        assert_eq!(endpoint_multiplexer_label(None), "");
        assert_eq!(endpoint_multiplexer_label(Some("endpoint:test")), "endpoint:test");
    }

    #[test]
    fn endpoint_multiplexer_label_for_uuid_uses_tagged_endpoint_uuid() {
        let endpoint_uuid = EndpointUuid::new_uuid();
        let label = endpoint_multiplexer_label_for_uuid(&endpoint_uuid);

        assert_eq!(label, endpoint_uuid.to_string());
        assert_ne!(label, endpoint_uuid.uuid().to_string());
    }

    #[tokio::test]
    async fn shard_multiplexer_caches_factory_result() {
        let (connection, _accepted_rx) = spawn_ping_server().await.expect("server should start");
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                clear_shard_multiplexers();
                let endpoint = "test-endpoint-cache";
                let factory_calls = Arc::new(AtomicUsize::new(0));

                let conn1 = connection.clone();
                let calls1 = Arc::clone(&factory_calls);
                let mux1 = shard_multiplexer_or_init(endpoint, move || {
                    calls1.fetch_add(1, Ordering::SeqCst);
                    RedisDirectMultiplexer::new_local(conn1, "test-org".to_string(), None, small_config())
                });

                let conn2 = connection.clone();
                let calls2 = Arc::clone(&factory_calls);
                let mux2 = shard_multiplexer_or_init(endpoint, move || {
                    calls2.fetch_add(1, Ordering::SeqCst);
                    RedisDirectMultiplexer::new_local(conn2, "test-org".to_string(), None, small_config())
                });

                assert_eq!(factory_calls.load(Ordering::SeqCst), 1, "factory must be called only on first lookup");
                assert_eq!(shard_multiplexer_len(), 1);
                assert!(Arc::ptr_eq(&mux1.inner, &mux2.inner), "second lookup must return the cached multiplexer");

                clear_shard_multiplexers();
            })
            .await;
    }

    #[tokio::test]
    async fn shard_multiplexer_evict_forces_factory_to_run_again() {
        let (connection, _accepted_rx) = spawn_ping_server().await.expect("server should start");
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                clear_shard_multiplexers();
                let endpoint = "test-endpoint-evict";
                let factory_calls = Arc::new(AtomicUsize::new(0));

                let conn1 = connection.clone();
                let calls1 = Arc::clone(&factory_calls);
                let _mux1 = shard_multiplexer_or_init(endpoint, move || {
                    calls1.fetch_add(1, Ordering::SeqCst);
                    RedisDirectMultiplexer::new_local(conn1, "test-org".to_string(), None, small_config())
                });

                let evicted = shard_multiplexer_evict(endpoint);
                assert!(evicted.is_some(), "evict returns the previously-cached multiplexer");
                assert_eq!(shard_multiplexer_len(), 0);

                let conn2 = connection.clone();
                let calls2 = Arc::clone(&factory_calls);
                let _mux2 = shard_multiplexer_or_init(endpoint, move || {
                    calls2.fetch_add(1, Ordering::SeqCst);
                    RedisDirectMultiplexer::new_local(conn2, "test-org".to_string(), None, small_config())
                });

                assert_eq!(factory_calls.load(Ordering::SeqCst), 2, "factory must run again after evict");
                clear_shard_multiplexers();
            })
            .await;
    }

    #[tokio::test]
    async fn new_local_dispatches_request_via_local_workers() {
        let (connection, _accepted_rx) = spawn_ping_server().await.expect("server should start");
        let local = tokio::task::LocalSet::new();
        let response = local
            .run_until(async move {
                let multiplexer = RedisDirectMultiplexer::new_local(connection, "test-org".to_string(), None, small_config());
                multiplexer.send(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"), 1).await.expect("local-mode request should succeed").0
            })
            .await;
        assert_eq!(response, Bytes::from_static(b"+PONG\r\n"));
    }
}
