//! Direct request multiplexer for PostgreSQL backend connections.
//!
//! This mirrors the Redis direct multiplexer shape for the PostgreSQL
//! interlay hot path. Each worker owns one backend connection split into
//! independent writer and reader halves. The writer batches safe request
//! groups onto the socket, while the reader drains complete response groups
//! in FIFO order until ReadyForQuery. Pipelining is only used by callers for
//! request groups that are safe to share on a direct backend connection.

use crate::client::{PostgresClient, PostgresClientReader, PostgresClientWriter};
use crate::url::PostgresConnectionParsed;
use bytes::{Bytes, BytesMut};
use error::{EpError, ResultEP};
use postgres_wire::stmt_cache::{self, ClientStmtMap, ResponseSlot};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

const DEFAULT_WORKER_COUNT: usize = 16;
const DEFAULT_MAX_WORKERS: usize = 64;
const DEFAULT_QUEUE_CAPACITY_PER_WORKER: usize = 1024;
const DEFAULT_SCALE_INTERVAL_MS: u64 = 100;
const DEFAULT_SCALE_UP_THRESHOLD_PER_WORKER: usize = 2;
const DEFAULT_MAX_INFLIGHT_PER_WORKER: usize = 32;
const DEFAULT_WRITE_BURST: usize = 32;

const ENV_WORKER_COUNT: &str = "EDEN_POSTGRES_MULTIPLEXED_CONNECTIONS";
const ENV_MAX_WORKERS: &str = "EDEN_POSTGRES_MULTIPLEXED_MAX_WORKERS";
const ENV_QUEUE_CAPACITY_PER_WORKER: &str = "EDEN_POSTGRES_MULTIPLEXED_QUEUE_CAPACITY_PER_CONNECTION";
const ENV_SCALE_INTERVAL_MS: &str = "EDEN_POSTGRES_MULTIPLEXED_SCALE_INTERVAL_MS";
const ENV_SCALE_UP_THRESHOLD_PER_WORKER: &str = "EDEN_POSTGRES_MULTIPLEXED_SCALE_UP_THRESHOLD_PER_WORKER";
const ENV_MAX_INFLIGHT_PER_WORKER: &str = "EDEN_POSTGRES_MULTIPLEXED_MAX_INFLIGHT_PER_CONNECTION";
const ENV_WRITE_BURST: &str = "EDEN_POSTGRES_MULTIPLEXED_WRITE_BURST";

type SlotResult = Result<(Bytes, u64), EpError>;

/// Destination for responses produced by multiplexer workers.
pub trait PostgresDispatchResponseSink: Send + Sync + 'static {
    fn deliver(&self, response: Result<Bytes, EpError>, request_received_at: Instant, network_latency_us: u64);
}

#[derive(Debug, Clone, Copy)]
pub struct PostgresDirectMultiplexerConfig {
    pub worker_count: usize,
    pub max_workers: usize,
    pub queue_capacity_per_worker: usize,
    pub scale_interval_ms: u64,
    pub scale_up_threshold_per_worker: usize,
    pub max_inflight_per_worker: usize,
    pub write_burst: usize,
}

impl Default for PostgresDirectMultiplexerConfig {
    fn default() -> Self {
        Self {
            worker_count: DEFAULT_WORKER_COUNT,
            max_workers: DEFAULT_MAX_WORKERS,
            queue_capacity_per_worker: DEFAULT_QUEUE_CAPACITY_PER_WORKER,
            scale_interval_ms: DEFAULT_SCALE_INTERVAL_MS,
            scale_up_threshold_per_worker: DEFAULT_SCALE_UP_THRESHOLD_PER_WORKER,
            max_inflight_per_worker: DEFAULT_MAX_INFLIGHT_PER_WORKER,
            write_burst: DEFAULT_WRITE_BURST,
        }
    }
}

impl PostgresDirectMultiplexerConfig {
    pub fn normalized(mut self) -> Self {
        self.worker_count = self.worker_count.max(1);
        self.max_workers = self.max_workers.max(self.worker_count);
        self.queue_capacity_per_worker = self.queue_capacity_per_worker.max(1);
        self.scale_interval_ms = self.scale_interval_ms.max(1);
        self.scale_up_threshold_per_worker = self.scale_up_threshold_per_worker.max(1);
        self.max_inflight_per_worker = self.max_inflight_per_worker.max(1);
        self.write_burst = self.write_burst.max(1).min(self.max_inflight_per_worker);
        self
    }

    pub fn from_env() -> Self {
        Self {
            worker_count: env_usize(ENV_WORKER_COUNT, DEFAULT_WORKER_COUNT),
            max_workers: env_usize(ENV_MAX_WORKERS, DEFAULT_MAX_WORKERS),
            queue_capacity_per_worker: env_usize(ENV_QUEUE_CAPACITY_PER_WORKER, DEFAULT_QUEUE_CAPACITY_PER_WORKER),
            scale_interval_ms: env_u64(ENV_SCALE_INTERVAL_MS, DEFAULT_SCALE_INTERVAL_MS),
            scale_up_threshold_per_worker: env_usize(ENV_SCALE_UP_THRESHOLD_PER_WORKER, DEFAULT_SCALE_UP_THRESHOLD_PER_WORKER),
            max_inflight_per_worker: env_usize(ENV_MAX_INFLIGHT_PER_WORKER, DEFAULT_MAX_INFLIGHT_PER_WORKER),
            write_burst: env_usize(ENV_WRITE_BURST, DEFAULT_WRITE_BURST),
        }
        .normalized()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|value| value.trim().parse::<usize>().ok()).unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|value| value.trim().parse::<u64>().ok()).unwrap_or(default)
}

#[derive(Clone)]
pub struct PostgresDirectMultiplexer {
    inner: Arc<PostgresDirectMultiplexerInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpawnMode {
    Global,
    Local,
}

impl SpawnMode {
    fn spawn<F>(self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
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

struct PostgresDirectMultiplexerInner {
    workers: RwLock<Vec<DirectWorkerHandle>>,
    next_worker: AtomicUsize,
    inflight: AtomicUsize,
    next_worker_id: AtomicUsize,
    config: PostgresDirectMultiplexerConfig,
    connection: PostgresConnectionParsed,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    endpoint_label: String,
    spawn_mode: SpawnMode,
}

impl PostgresDirectMultiplexer {
    pub fn new(
        connection: PostgresConnectionParsed,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        config: PostgresDirectMultiplexerConfig,
    ) -> Self {
        let endpoint_label = endpoint_uuid.clone().unwrap_or_default();
        Self::new_with_spawn_mode(connection, org_uuid, endpoint_uuid, endpoint_label, config, SpawnMode::Global)
    }

    pub fn new_with_registry_label(
        connection: PostgresConnectionParsed,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        registry_label: String,
        config: PostgresDirectMultiplexerConfig,
    ) -> Self {
        Self::new_with_spawn_mode(connection, org_uuid, endpoint_uuid, registry_label, config, SpawnMode::Global)
    }

    pub fn new_local(
        connection: PostgresConnectionParsed,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        registry_label: String,
        config: PostgresDirectMultiplexerConfig,
    ) -> Self {
        Self::new_with_spawn_mode(connection, org_uuid, endpoint_uuid, registry_label, config, SpawnMode::Local)
    }

    fn new_with_spawn_mode(
        connection: PostgresConnectionParsed,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        endpoint_label: String,
        config: PostgresDirectMultiplexerConfig,
        spawn_mode: SpawnMode,
    ) -> Self {
        let config = config.normalized();
        let initial_workers = (0..config.worker_count)
            .map(|worker_id| {
                DirectWorkerHandle::spawn(worker_id, connection.clone(), org_uuid.clone(), endpoint_uuid.clone(), config, spawn_mode)
            })
            .collect();

        let inner = Arc::new(PostgresDirectMultiplexerInner {
            workers: RwLock::new(initial_workers),
            next_worker: AtomicUsize::new(0),
            inflight: AtomicUsize::new(0),
            next_worker_id: AtomicUsize::new(config.worker_count),
            config,
            connection,
            org_uuid,
            endpoint_uuid,
            endpoint_label,
            spawn_mode,
        });

        spawn_mode.spawn(autoscaler_loop(Arc::downgrade(&inner)));
        Self { inner }
    }

    pub fn endpoint_label(&self) -> &str {
        &self.inner.endpoint_label
    }

    pub fn local_clone(&self, divisor: usize) -> Self {
        debug_assert!(
            ep_core::runtime::is_shard_runtime(),
            "PostgresDirectMultiplexer::local_clone must be called from a shard runtime thread"
        );
        debug_assert!(
            !self.inner.endpoint_label.is_empty(),
            "PostgresDirectMultiplexer::local_clone requires an endpoint label for shard-local registry lookup"
        );
        let divisor = divisor.max(1);
        let mut config = self.inner.config;
        config.worker_count = (config.worker_count / divisor).max(1);
        config.max_workers = (config.max_workers / divisor).max(1);
        if config.worker_count > config.max_workers {
            config.worker_count = config.max_workers;
        }
        Self::new_local(
            self.inner.connection.clone(),
            self.inner.org_uuid.clone(),
            self.inner.endpoint_uuid.clone(),
            self.inner.endpoint_label.clone(),
            config,
        )
    }

    pub async fn send(&self, bytes: Bytes) -> ResultEP<(Bytes, u64)> {
        let (tx, rx) = oneshot::channel();
        let request = DirectRequest {
            payload: Some(DirectRequestPayload::Raw(bytes)),
            response_target: Some(ResponseTarget::Slot(tx)),
            request_received_at: Instant::now(),
            inflight_owner: Some(Arc::clone(&self.inner)),
        };
        self.dispatch_request(request).await?;
        rx.await.map_err(|_| EpError::request("postgres multiplexer request canceled before fulfillment"))?
    }

    pub async fn send_prepared(&self, raw_batch: Bytes, client_stmt_map: ClientStmtMap) -> ResultEP<(Bytes, u64)> {
        let (tx, rx) = oneshot::channel();
        let request = DirectRequest {
            payload: Some(DirectRequestPayload::Prepared { raw_batch, client_stmt_map }),
            response_target: Some(ResponseTarget::Slot(tx)),
            request_received_at: Instant::now(),
            inflight_owner: Some(Arc::clone(&self.inner)),
        };
        self.dispatch_request(request).await?;
        rx.await.map_err(|_| EpError::request("postgres multiplexer prepared request canceled before fulfillment"))?
    }

    pub async fn dispatch_to_sink(
        &self,
        bytes: Bytes,
        sink: Arc<dyn PostgresDispatchResponseSink>,
        request_received_at: Instant,
    ) -> ResultEP<()> {
        let request = DirectRequest {
            payload: Some(DirectRequestPayload::Raw(bytes)),
            response_target: Some(ResponseTarget::Sink(sink)),
            request_received_at,
            inflight_owner: Some(Arc::clone(&self.inner)),
        };
        self.dispatch_request(request).await
    }

    async fn dispatch_request(&self, request: DirectRequest) -> ResultEP<()> {
        self.inner.inflight.fetch_add(1, Ordering::Relaxed);
        let workers = self.inner.workers.read().expect("postgres multiplexer workers lock poisoned").clone();
        let worker_count = workers.len();
        if worker_count == 0 {
            request.complete(Err(EpError::request("postgres direct multiplexer has no workers")), 0);
            return Err(EpError::request("postgres direct multiplexer has no workers"));
        }

        let start_index = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        let mut pending = Some(request);
        let mut fallback_worker_index = None;
        for offset in 0..worker_count {
            let worker_index = (start_index + offset) % worker_count;
            let worker = &workers[worker_index];
            if let Some(request_to_send) = pending.take() {
                match worker.sender.try_send(request_to_send) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(request)) => {
                        fallback_worker_index.get_or_insert(worker_index);
                        pending = Some(request);
                    }
                    Err(TrySendError::Closed(request)) => pending = Some(request),
                }
            }
        }

        if let Some(request_to_send) = pending {
            if let Some(worker_index) = fallback_worker_index {
                let worker = &workers[worker_index];
                match worker.sender.send(request_to_send).await {
                    Ok(()) => return Ok(()),
                    Err(err) => {
                        let request = err.0;
                        request.complete(Err(EpError::request("postgres direct multiplexer workers are unavailable")), 0);
                    }
                }
            } else {
                request_to_send.complete(Err(EpError::request("postgres direct multiplexer workers are unavailable")), 0);
            }
        }
        Err(EpError::request("postgres direct multiplexer workers are unavailable"))
    }
}

async fn autoscaler_loop(weak_inner: std::sync::Weak<PostgresDirectMultiplexerInner>) {
    let interval_ms = match weak_inner.upgrade() {
        Some(inner) => inner.config.scale_interval_ms,
        None => return,
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let Some(inner) = weak_inner.upgrade() else {
            return;
        };
        let worker_count = inner.workers.read().expect("postgres multiplexer workers lock poisoned").len();
        if worker_count >= inner.config.max_workers {
            continue;
        }
        let inflight = inner.inflight.load(Ordering::Relaxed);
        let avg_inflight = inflight / worker_count.max(1);
        if avg_inflight < inner.config.scale_up_threshold_per_worker {
            continue;
        }

        let worker_id = inner.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let worker = DirectWorkerHandle::spawn(
            worker_id,
            inner.connection.clone(),
            inner.org_uuid.clone(),
            inner.endpoint_uuid.clone(),
            inner.config,
            inner.spawn_mode,
        );
        inner.workers.write().expect("postgres multiplexer workers lock poisoned").push(worker);
    }
}

#[derive(Clone)]
struct DirectWorkerHandle {
    sender: mpsc::Sender<DirectRequest>,
}

impl DirectWorkerHandle {
    fn spawn(
        worker_id: usize,
        connection: PostgresConnectionParsed,
        org_uuid: String,
        endpoint_uuid: Option<String>,
        config: PostgresDirectMultiplexerConfig,
        spawn_mode: SpawnMode,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity_per_worker);
        spawn_mode.spawn(async move {
            DirectWorker {
                worker_id,
                connection,
                org_uuid,
                endpoint_uuid,
                config,
                receiver,
                spawn_mode,
            }
            .run()
            .await;
        });
        Self { sender }
    }
}

enum ResponseTarget {
    Slot(oneshot::Sender<SlotResult>),
    Sink(Arc<dyn PostgresDispatchResponseSink>),
}

struct DirectRequest {
    payload: Option<DirectRequestPayload>,
    response_target: Option<ResponseTarget>,
    request_received_at: Instant,
    inflight_owner: Option<Arc<PostgresDirectMultiplexerInner>>,
}

enum DirectRequestPayload {
    Raw(Bytes),
    Prepared { raw_batch: Bytes, client_stmt_map: ClientStmtMap },
}

impl DirectRequest {
    fn complete(mut self, result: Result<Bytes, EpError>, network_latency_us: u64) {
        if let Some(target) = self.response_target.take() {
            match target {
                ResponseTarget::Slot(tx) => {
                    let _ = tx.send(result.map(|bytes| (bytes, network_latency_us)));
                }
                ResponseTarget::Sink(sink) => {
                    sink.deliver(result, self.request_received_at, network_latency_us);
                }
            }
        }
        if let Some(inner) = self.inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn into_ready(mut self, backend_id: stmt_cache::BackendId, combined: &mut BytesMut) -> ReadyRequest {
        let payload = self.payload.take().expect("postgres multiplexer request payload must be present before write");

        let response_slots = match payload {
            DirectRequestPayload::Raw(bytes) => {
                combined.extend_from_slice(&bytes);
                None
            }
            DirectRequestPayload::Prepared { raw_batch, mut client_stmt_map } => {
                let rewritten = stmt_cache::rewrite_batch(&raw_batch, &mut client_stmt_map, backend_id);
                combined.extend_from_slice(&rewritten.backend_bytes);
                Some(rewritten.response_slots().to_vec())
            }
        };

        ReadyRequest {
            response_target: self.response_target.take(),
            request_received_at: self.request_received_at,
            inflight_owner: self.inflight_owner.take(),
            response_slots,
        }
    }
}

impl Drop for DirectRequest {
    fn drop(&mut self) {
        if let Some(target) = self.response_target.take() {
            match target {
                ResponseTarget::Slot(tx) => {
                    let _ = tx.send(Err(EpError::request("postgres multiplexer request dropped before fulfillment")));
                }
                ResponseTarget::Sink(sink) => {
                    sink.deliver(
                        Err(EpError::request("postgres multiplexer request dropped before fulfillment")),
                        self.request_received_at,
                        0,
                    );
                }
            }
        }
        if let Some(inner) = self.inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct ReadyRequest {
    response_target: Option<ResponseTarget>,
    request_received_at: Instant,
    inflight_owner: Option<Arc<PostgresDirectMultiplexerInner>>,
    response_slots: Option<Vec<ResponseSlot>>,
}

impl ReadyRequest {
    fn complete(mut self, result: Result<Bytes, EpError>, network_latency_us: u64) {
        if let Some(target) = self.response_target.take() {
            match target {
                ResponseTarget::Slot(tx) => {
                    let _ = tx.send(result.map(|bytes| (bytes, network_latency_us)));
                }
                ResponseTarget::Sink(sink) => {
                    sink.deliver(result, self.request_received_at, network_latency_us);
                }
            }
        }
        if let Some(inner) = self.inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for ReadyRequest {
    fn drop(&mut self) {
        if let Some(target) = self.response_target.take() {
            match target {
                ResponseTarget::Slot(tx) => {
                    let _ = tx.send(Err(EpError::request("postgres multiplexer request dropped before write completion")));
                }
                ResponseTarget::Sink(sink) => {
                    sink.deliver(
                        Err(EpError::request("postgres multiplexer request dropped before write completion")),
                        self.request_received_at,
                        0,
                    );
                }
            }
        }
        if let Some(inner) = self.inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct DirectWorker {
    #[allow(dead_code)]
    worker_id: usize,
    connection: PostgresConnectionParsed,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    config: PostgresDirectMultiplexerConfig,
    receiver: mpsc::Receiver<DirectRequest>,
    spawn_mode: SpawnMode,
}

impl DirectWorker {
    async fn run(mut self) {
        loop {
            let client = match self.connect().await {
                Ok(client) => client,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if self.receiver.is_closed() {
                        return;
                    }
                    continue;
                }
            };

            let backend_id = client.backend_key_data().unwrap_or((0, 0));
            let (mut writer, reader) = client.into_split();
            let (fifo_tx, fifo_rx) = mpsc::channel::<PendingFifoEntry>(self.config.max_inflight_per_worker);
            let reader_fut = reader_loop(reader, fifo_rx);
            let reader_handle = match self.spawn_mode {
                SpawnMode::Global => tokio::spawn(reader_fut),
                SpawnMode::Local => tokio::task::spawn_local(reader_fut),
            };

            let exit = run_writer_session(&mut self.receiver, &mut writer, &fifo_tx, self.config, backend_id).await;
            drop(fifo_tx);
            let _ = reader_handle.await;

            match exit {
                WriterSessionExit::ChannelClosed => return,
                WriterSessionExit::WriteFailed | WriterSessionExit::ReaderDied => continue,
            }
        }
    }

    async fn connect(&self) -> ResultEP<PostgresClient> {
        PostgresClient::connect_with_org_endpoint(&self.connection, self.org_uuid.clone(), self.endpoint_uuid.clone()).await
    }
}

struct PendingFifoEntry {
    started_at: Instant,
    response_target: Option<ResponseTarget>,
    request_received_at: Instant,
    inflight_owner: Option<Arc<PostgresDirectMultiplexerInner>>,
    response_slots: Option<Vec<ResponseSlot>>,
}

impl PendingFifoEntry {
    fn complete(mut self, result: Result<Bytes, EpError>, network_latency_us: u64) {
        if let Some(target) = self.response_target.take() {
            match target {
                ResponseTarget::Slot(tx) => {
                    let _ = tx.send(result.map(|bytes| (bytes, network_latency_us)));
                }
                ResponseTarget::Sink(sink) => {
                    sink.deliver(result, self.request_received_at, network_latency_us);
                }
            }
        }
        if let Some(inner) = self.inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for PendingFifoEntry {
    fn drop(&mut self) {
        if let Some(target) = self.response_target.take() {
            match target {
                ResponseTarget::Slot(tx) => {
                    let _ = tx.send(Err(EpError::request("postgres multiplexer request dropped before response delivery")));
                }
                ResponseTarget::Sink(sink) => {
                    sink.deliver(
                        Err(EpError::request("postgres multiplexer request dropped before response delivery")),
                        self.request_received_at,
                        0,
                    );
                }
            }
        }
        if let Some(inner) = self.inflight_owner.take() {
            inner.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

enum WriterSessionExit {
    ChannelClosed,
    WriteFailed,
    ReaderDied,
}

async fn run_writer_session(
    receiver: &mut mpsc::Receiver<DirectRequest>,
    writer: &mut PostgresClientWriter,
    fifo_tx: &mpsc::Sender<PendingFifoEntry>,
    config: PostgresDirectMultiplexerConfig,
    backend_id: stmt_cache::BackendId,
) -> WriterSessionExit {
    let write_burst = config.write_burst.max(1);
    let mut batch: Vec<DirectRequest> = Vec::with_capacity(write_burst);
    let mut ready_batch: Vec<ReadyRequest> = Vec::with_capacity(write_burst);

    loop {
        let first = match receiver.recv().await {
            Some(request) => request,
            None => return WriterSessionExit::ChannelClosed,
        };
        batch.clear();
        batch.push(first);
        while batch.len() < write_burst {
            match receiver.try_recv() {
                Ok(request) => batch.push(request),
                Err(_) => break,
            }
        }

        let permits = match fifo_tx.reserve_many(batch.len()).await {
            Ok(permits) => permits,
            Err(_) => {
                for request in batch.drain(..) {
                    request.complete(Err(EpError::request("postgres multiplexer reader task unavailable")), 0);
                }
                return WriterSessionExit::ReaderDied;
            }
        };

        let estimated_bytes: usize = batch
            .iter()
            .map(|request| match request.payload.as_ref() {
                Some(DirectRequestPayload::Raw(bytes)) => bytes.len(),
                Some(DirectRequestPayload::Prepared { raw_batch, .. }) => raw_batch.len(),
                None => 0,
            })
            .sum();
        let mut combined = BytesMut::with_capacity(estimated_bytes);
        ready_batch.clear();
        for request in batch.drain(..) {
            ready_batch.push(request.into_ready(backend_id, &mut combined));
        }
        let combined = combined.freeze();

        let started_at = Instant::now();
        if let Err(error) = writer.write_query_raw_no_response(&combined).await {
            for request in ready_batch.drain(..) {
                request.complete(Err(error.clone()), 0);
            }
            return WriterSessionExit::WriteFailed;
        }

        let mut permits = permits;
        for mut request in ready_batch.drain(..) {
            let entry = PendingFifoEntry {
                started_at,
                response_target: request.response_target.take(),
                request_received_at: request.request_received_at,
                inflight_owner: request.inflight_owner.take(),
                response_slots: request.response_slots.take(),
            };
            let permit = permits.next().expect("reserved one FIFO permit per postgres request");
            permit.send(entry);
        }
    }
}

async fn reader_loop(mut reader: PostgresClientReader, mut fifo_rx: mpsc::Receiver<PendingFifoEntry>) {
    loop {
        let entry = match fifo_rx.recv().await {
            Some(entry) => entry,
            None => return,
        };

        match reader.read_response_group_raw_bytes().await {
            Ok(mut response) => {
                if let Some(slots) = entry.response_slots.as_deref() {
                    response = stmt_cache::merge_responses(&response, slots);
                }
                let read_done = Instant::now();
                let network_latency_us = read_done.duration_since(entry.started_at).as_micros().min(u64::MAX as u128) as u64;
                entry.complete(Ok(response), network_latency_us);
            }
            Err(error) => {
                entry.complete(Err(error.clone()), 0);
                while let Ok(remaining) = fifo_rx.try_recv() {
                    remaining.complete(Err(error.clone()), 0);
                }
                return;
            }
        }
    }
}

thread_local! {
    static SHARD_MULTIPLEXERS: RefCell<HashMap<String, PostgresDirectMultiplexer>> = RefCell::default();
}

pub fn shard_multiplexer_or_init<F>(endpoint_label: &str, factory: F) -> PostgresDirectMultiplexer
where
    F: FnOnce() -> PostgresDirectMultiplexer,
{
    SHARD_MULTIPLEXERS.with(|multiplexers| {
        let mut multiplexers = multiplexers.borrow_mut();
        if let Some(existing) = multiplexers.get(endpoint_label) {
            return existing.clone();
        }
        let multiplexer = factory();
        multiplexers.insert(endpoint_label.to_string(), multiplexer.clone());
        multiplexer
    })
}

pub fn shard_multiplexer_evict(endpoint_label: &str) -> Option<PostgresDirectMultiplexer> {
    SHARD_MULTIPLEXERS.with(|multiplexers| multiplexers.borrow_mut().remove(endpoint_label))
}

pub fn clear_shard_multiplexers() {
    SHARD_MULTIPLEXERS.with(|multiplexers| multiplexers.borrow_mut().clear());
}

pub fn shard_multiplexer_len() -> usize {
    SHARD_MULTIPLEXERS.with(|multiplexers| multiplexers.borrow().len())
}

pub fn pick_multiplexer_for_dispatch(global: &PostgresDirectMultiplexer) -> PostgresDirectMultiplexer {
    if ep_core::runtime::is_shard_runtime() {
        let divisor = ep_core::runtime::shard_count_or(1);
        let global_for_factory = global.clone();
        shard_multiplexer_or_init(global.endpoint_label(), move || global_for_factory.local_clone(divisor))
    } else {
        global.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::SslMode;

    fn parsed_connection() -> PostgresConnectionParsed {
        PostgresConnectionParsed {
            host: "127.0.0.1".to_string(),
            port: 15432,
            user: "postgres".to_string(),
            password: None,
            database: "postgres".to_string(),
            sslmode: SslMode::Disable,
            application_name: None,
        }
    }

    #[test]
    fn config_default_is_normalized() {
        let config = PostgresDirectMultiplexerConfig::default();
        assert!(config.worker_count >= 1);
        assert!(config.max_workers >= config.worker_count);
        assert!(config.queue_capacity_per_worker >= 1);
        assert!(config.scale_interval_ms >= 1);
        assert!(config.scale_up_threshold_per_worker >= 1);
        assert!(config.max_inflight_per_worker >= 1);
        assert!(config.write_burst >= 1);
        assert!(config.write_burst <= config.max_inflight_per_worker);
    }

    #[test]
    fn config_normalization_clamps_write_burst_to_inflight_limit() {
        let config = PostgresDirectMultiplexerConfig {
            worker_count: 0,
            max_workers: 0,
            queue_capacity_per_worker: 0,
            scale_interval_ms: 0,
            scale_up_threshold_per_worker: 0,
            max_inflight_per_worker: 4,
            write_burst: 64,
        }
        .normalized();

        assert_eq!(config.worker_count, 1);
        assert_eq!(config.max_workers, 1);
        assert_eq!(config.queue_capacity_per_worker, 1);
        assert_eq!(config.scale_interval_ms, 1);
        assert_eq!(config.scale_up_threshold_per_worker, 1);
        assert_eq!(config.max_inflight_per_worker, 4);
        assert_eq!(config.write_burst, 4);
    }

    #[tokio::test]
    async fn custom_registry_label_is_used_for_shard_lookup() {
        let multiplexer = PostgresDirectMultiplexer::new_with_registry_label(
            parsed_connection(),
            "org".to_string(),
            Some("endpoint-uuid".to_string()),
            "endpoint-uuid:write".to_string(),
            PostgresDirectMultiplexerConfig::default(),
        );

        assert_eq!(multiplexer.endpoint_label(), "endpoint-uuid:write");
    }

    #[tokio::test]
    async fn shard_registry_caches_factory_result() {
        clear_shard_multiplexers();
        let config = PostgresDirectMultiplexerConfig {
            worker_count: 1,
            max_workers: 1,
            queue_capacity_per_worker: 1,
            scale_interval_ms: 10_000,
            scale_up_threshold_per_worker: 1,
            max_inflight_per_worker: 1,
            write_burst: 1,
        };
        let endpoint = "endpoint:test";
        let mux1 = shard_multiplexer_or_init(endpoint, || {
            PostgresDirectMultiplexer::new(parsed_connection(), "org".to_string(), Some(endpoint.to_string()), config)
        });
        let mux2 = shard_multiplexer_or_init(endpoint, || {
            PostgresDirectMultiplexer::new(parsed_connection(), "org".to_string(), Some("other".to_string()), config)
        });

        assert_eq!(shard_multiplexer_len(), 1);
        assert!(Arc::ptr_eq(&mux1.inner, &mux2.inner));
        clear_shard_multiplexers();
    }
}
