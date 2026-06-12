#![cfg_attr(test, allow(clippy::unwrap_used))]
pub mod client;
#[allow(dead_code)] // Alternative implementation not yet integrated
mod client_new;
pub mod codec;
pub mod config;
pub mod connection;
pub mod multiplex;
pub mod pool;
#[cfg(all(test, feature = "infra-tests"))]
pub mod test_utils;

pub use client::*;
pub use codec::*;
pub use config::*;
pub use connection::*;
pub use multiplex::*;
pub use pool::*;

use bytes::Bytes;
use deadpool::managed::{Object, Pool, PoolError};
use ep_core::pool::PoisonGuard;
use error::{EpError, ResultEP};
use redis::Pipeline;

#[doc(hidden)]
pub mod validation {
    use bytes::BytesMut;
    use std::io;
    use std::time::Duration;
    use tokio::time::Instant;

    pub fn empty_read_budget_for_validation() -> u32 {
        crate::client::empty_read_budget()
    }

    pub async fn timed_duplex_read_for_validation(
        close_peer: bool,
        poll_interval: Duration,
        prefilled_bytes: usize,
    ) -> io::Result<(usize, Duration)> {
        let (mut client_side, peer_side) = tokio::io::duplex(1024);
        let mut buffer = BytesMut::with_capacity(prefilled_bytes.max(64));
        if prefilled_bytes > 0 {
            buffer.extend(std::iter::repeat_n(0u8, prefilled_bytes));
        }

        let peer_task = if close_peer {
            drop(peer_side);
            None
        } else {
            Some(tokio::spawn(async move {
                tokio::time::sleep(poll_interval + poll_interval).await;
                drop(peer_side);
            }))
        };

        let start = Instant::now();
        let read = crate::codec::read_buf_with_poll_interval(&mut client_side, &mut buffer, poll_interval).await?;
        let elapsed = start.elapsed();

        if let Some(task) = peer_task {
            let _ = task.await;
        }

        Ok((read, elapsed))
    }
}

#[derive(Clone)]
pub struct RedisAsync {
    pool: Pool<RedisConnectionManager>,
    /// Multiplexer used only by the legacy Redis processor fallback.
    /// The primary proxy hot path opens lanes through
    /// `eden_gateway::direct_pool` using `connection_config` instead.
    direct_multiplexer: Option<RedisDirectMultiplexer>,
    connection_config: RedisConnection,
    endpoint_uuid: Option<String>,
    max_retries: u32,
    multi_key_execution: MultiKeyExecution,
    _pool_status_poller: Option<telemetry::PoolStatusPollerHandle>,
}

impl RedisAsync {
    pub fn new(
        pool: Pool<RedisConnectionManager>,
        direct_multiplexer: Option<RedisDirectMultiplexer>,
        connection_config: RedisConnection,
        endpoint_uuid: Option<String>,
        max_retries: u32,
        multi_key_execution: MultiKeyExecution,
        pool_status_poller: Option<telemetry::PoolStatusPollerHandle>,
    ) -> Self {
        Self {
            pool,
            direct_multiplexer,
            connection_config,
            endpoint_uuid,
            max_retries,
            multi_key_execution,
            _pool_status_poller: pool_status_poller,
        }
    }

    pub async fn get(&self) -> Result<Object<RedisConnectionManager>, PoolError<EpError>> {
        self.pool.get().await
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    pub fn multi_key_execution(&self) -> MultiKeyExecution {
        self.multi_key_execution
    }

    /// Backend connection config this `RedisAsync` was built around.
    pub fn connection_config(&self) -> &RedisConnection {
        &self.connection_config
    }

    /// Endpoint UUID associated with this connection. Used by direct-proxy
    /// mode when it opens dedicated backend connections.
    pub fn endpoint_uuid(&self) -> Option<String> {
        self.endpoint_uuid.clone()
    }

    /// Send raw Redis bytes from the legacy processor fallback.
    pub async fn send_raw_bytes_multiplexed(&self, command_bytes: Bytes) -> ResultEP<(Bytes, u64)> {
        let Some(global) = &self.direct_multiplexer else {
            return self.send_raw_bytes_via_pool(command_bytes).await;
        };

        let command_count = RedisClient::count_pipeline_commands(&command_bytes)?;
        let mux = pick_multiplexer_for_dispatch(global);
        mux.send(command_bytes, command_count).await
    }

    /// Send raw Redis bytes when the caller already parsed the command count.
    pub async fn send_raw_bytes_multiplexed_with_command_count(
        &self,
        command_bytes: Bytes,
        command_count: usize,
    ) -> ResultEP<(Bytes, u64)> {
        if let Some(global) = &self.direct_multiplexer {
            let mux = pick_multiplexer_for_dispatch(global);
            return mux.send(command_bytes, command_count).await;
        }

        self.send_raw_bytes_via_pool(command_bytes).await
    }

    /// Fire-and-forget dispatch from the legacy processor fallback.
    /// Returns when the request has been enqueued to a worker; the
    /// worker delivers the response directly to `sink`. Falls back to
    /// the synchronous pool path (with await) if the multiplexer isn't
    /// configured; in that case, the sink is invoked inline once the
    /// pool returns.
    pub async fn dispatch_raw_bytes_multiplexed_to_sink(
        &self,
        command_bytes: Bytes,
        sink: std::sync::Arc<dyn crate::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
    ) -> ResultEP<()> {
        let command_count = RedisClient::count_pipeline_commands(&command_bytes)?;
        self.dispatch_raw_bytes_multiplexed_to_sink_with_command_count(command_bytes, command_count, sink, request_received_at)
            .await
    }

    /// Fire-and-forget dispatch when the caller already parsed the command count.
    pub async fn dispatch_raw_bytes_multiplexed_to_sink_with_command_count(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: std::sync::Arc<dyn crate::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
    ) -> ResultEP<()> {
        if let Some(global) = &self.direct_multiplexer {
            let mux = pick_multiplexer_for_dispatch(global);
            return mux.dispatch_to_sink(command_bytes, command_count, sink, request_received_at).await;
        }

        // No multiplexer: do a blocking pool roundtrip and deliver.
        let result = self.send_raw_bytes_via_pool(command_bytes).await;
        match result {
            Ok((bytes, latency)) => {
                sink.deliver(Ok(bytes), command_count, request_received_at, latency);
                Ok(())
            }
            Err(e) => {
                sink.deliver(Err(e.clone()), command_count, request_received_at, 0);
                Err(e)
            }
        }
    }

    /// Best-effort sink dispatch. Returns immediately after enqueueing to a
    /// direct multiplexer worker and never waits for queue capacity. The sink
    /// is called only if the command was accepted by a worker.
    pub fn try_dispatch_raw_bytes_multiplexed_to_sink(
        &self,
        command_bytes: Bytes,
        sink: std::sync::Arc<dyn crate::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
    ) -> ResultEP<()> {
        let command_count = RedisClient::count_pipeline_commands(&command_bytes)?;
        self.try_dispatch_raw_bytes_multiplexed_to_sink_with_command_count(command_bytes, command_count, sink, request_received_at)
    }

    /// Best-effort sink dispatch when the caller already parsed the command count.
    pub fn try_dispatch_raw_bytes_multiplexed_to_sink_with_command_count(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: std::sync::Arc<dyn crate::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
    ) -> ResultEP<()> {
        if let Some(global) = &self.direct_multiplexer {
            let mux = pick_multiplexer_for_dispatch(global);
            return mux.try_dispatch_to_sink(command_bytes, command_count, sink, request_received_at);
        }

        Err(EpError::request("redis direct multiplexer is unavailable"))
    }

    /// Best-effort sink dispatch with a permit released on response drain/failure.
    pub fn try_dispatch_raw_bytes_multiplexed_to_sink_with_command_count_and_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: std::sync::Arc<dyn crate::multiplex::DispatchResponseSink>,
        request_received_at: std::time::Instant,
        completion_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> ResultEP<()> {
        if let Some(global) = &self.direct_multiplexer {
            let mux = pick_multiplexer_for_dispatch(global);
            return mux.try_dispatch_to_sink_with_completion_permit(
                command_bytes,
                command_count,
                sink,
                request_received_at,
                completion_permit,
            );
        }

        Err(EpError::request("redis direct multiplexer is unavailable"))
    }

    /// Best-effort dispatch to a process-lifetime sink. Mirror targets use
    /// this to keep response draining on the shard-local multiplexer without
    /// cloning a sink `Arc` per request.
    pub fn try_dispatch_raw_bytes_multiplexed_to_static_sink_with_command_count_and_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: &'static dyn crate::multiplex::DispatchResponseSink,
        request_received_at: std::time::Instant,
        completion_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> ResultEP<()> {
        if let Some(global) = &self.direct_multiplexer {
            let mux = pick_multiplexer_for_dispatch(global);
            return mux.try_dispatch_to_static_sink_with_completion_permit(
                command_bytes,
                command_count,
                sink,
                request_received_at,
                completion_permit,
            );
        }

        Err(EpError::request("redis direct multiplexer is unavailable"))
    }

    /// Best-effort dispatch to a process-lifetime sink that does not need
    /// successful response bytes. Used by mirroring to drain Redis responses
    /// without materializing payloads that will be discarded.
    pub fn try_dispatch_raw_bytes_multiplexed_to_static_discard_sink_with_command_count_and_completion_permit(
        &self,
        command_bytes: Bytes,
        command_count: usize,
        sink: &'static dyn crate::multiplex::DispatchResponseSink,
        request_received_at: std::time::Instant,
        completion_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> ResultEP<()> {
        if let Some(global) = &self.direct_multiplexer {
            let mux = pick_multiplexer_for_dispatch(global);
            return mux.try_dispatch_to_static_discard_sink_with_completion_permit(
                command_bytes,
                command_count,
                sink,
                request_received_at,
                completion_permit,
            );
        }

        Err(EpError::request("redis direct multiplexer is unavailable"))
    }

    async fn send_raw_bytes_via_pool(&self, command_bytes: Bytes) -> ResultEP<(Bytes, u64)> {
        let max_retries = self.max_retries();
        let mut last_err: Option<EpError> = None;
        let started_at = std::time::Instant::now();

        for attempt in 0..=max_retries {
            if attempt > 0 && started_at.elapsed().as_secs() >= 8 {
                break;
            }

            let client = self.get().await.map_err(EpError::request)?;
            let mut guard = PoisonGuard::new(client);

            match guard.send_command_raw(&command_bytes).await {
                Ok((response, network_latency_us)) => {
                    guard.disarm();
                    return Ok((response.to_bytes(), network_latency_us));
                }
                Err(error) if is_retryable_redis_raw_error(&error) && attempt < max_retries => {
                    last_err = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_err.unwrap_or_else(|| EpError::request("redis raw bytes send: no attempts executed")))
    }
}

fn is_retryable_redis_raw_error(error: &EpError) -> bool {
    matches!(error, EpError::Io(_) | EpError::Connect(_) | EpError::Request(_))
}

/// Choose the multiplexer to dispatch through:
///   - On a shard runtime thread: look up (or lazy-init) the per-shard
///     multiplexer for `global.endpoint_label()` from this thread's
///     `SHARD_MULTIPLEXERS` registry. The shard-local multiplexer's
///     workers are spawned via `spawn_local`, so the request → backend
///     write → response read → sink delivery chain stays on the shard
///     thread. The lazy-init factory uses `local_clone()` to mirror the
///     global multiplexer's connection + worker config.
///   - On any other thread (actix workers, the main proxy_runtime,
///     tests without a LocalSet): fall back to the global multiplexer
///     so behavior is unchanged outside the shard runtimes.
///
/// Returns an owned `RedisDirectMultiplexer` (cheap — Arc inside) so
/// the caller doesn't have to keep the registry borrow open.
fn pick_multiplexer_for_dispatch(global: &RedisDirectMultiplexer) -> RedisDirectMultiplexer {
    if ep_core::runtime::is_shard_runtime() {
        // Total backend connection budget for this endpoint stays
        // anchored to the global multiplexer's `worker_count` /
        // `max_workers`. The per-shard local multiplexer takes a
        // proportional slice — `global / shard_count` — so the
        // aggregated worker pool across all shards matches the
        // unsharded build's fan-out instead of multiplying it.
        let divisor = ep_core::runtime::shard_count_or(1);
        let global_for_factory = global.clone();
        crate::multiplex::shard_multiplexer_or_init(global.endpoint_label(), move || global_for_factory.local_clone(divisor))
    } else {
        global.clone()
    }
}

pub type RedisTx = Pipeline;

pub trait ToRedisPool {
    fn to_redis_pool(self) -> RedisAsync;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, Instant};

    static TEST_ENDPOINT_ID: AtomicUsize = AtomicUsize::new(0);

    fn test_endpoint() -> String {
        let id = TEST_ENDPOINT_ID.fetch_add(1, Ordering::Relaxed);
        format!("redis-poller-test-{id}")
    }

    fn redis_endpoint_in_use_count(endpoint_uuid: &str) -> Option<i64> {
        telemetry::connection_tracker::connection_state()
            .snapshot_endpoint_in_use()
            .into_iter()
            .find(|(db_type, uuid, _)| *db_type == "redis" && uuid == endpoint_uuid)
            .map(|(_, _, count)| count)
    }

    async fn wait_for_redis_endpoint_in_use(endpoint_uuid: &str, expected: Option<i64>) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if redis_endpoint_in_use_count(endpoint_uuid) == expected {
                return;
            }

            assert!(Instant::now() < deadline, "timed out waiting for Redis endpoint in-use count {expected:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn redis_async_keeps_backend_metadata_without_multiplexer() {
        let connection_config = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: Some(6380),
            ..Default::default()
        };
        let endpoint_uuid = Some("endpoint-123".to_string());
        let manager = RedisConnectionManager::new(connection_config.clone());
        let pool = Pool::builder(manager).max_size(1).build().expect("pool should build");

        let redis = RedisAsync::new(pool, None, connection_config.clone(), endpoint_uuid.clone(), 0, MultiKeyExecution::Native, None);

        assert_eq!(redis.connection_config().host, connection_config.host);
        assert_eq!(redis.connection_config().port, connection_config.port);
        assert_eq!(redis.endpoint_uuid(), endpoint_uuid);
    }

    #[tokio::test]
    async fn redis_async_drops_pool_status_poller_when_last_clone_drops() {
        let connection_config = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: Some(6380),
            ..Default::default()
        };
        let endpoint_uuid = test_endpoint();
        let poll_count = Arc::new(AtomicUsize::new(0));
        let poll_count_for_closure = poll_count.clone();
        let poller = telemetry::spawn_pool_status_poller(
            "redis",
            telemetry::labels::SYSTEM_ORG_UUID,
            Some(endpoint_uuid.clone()),
            Duration::from_millis(10),
            move || {
                poll_count_for_closure.fetch_add(1, Ordering::Relaxed);
                Some((2, 0))
            },
        );
        let manager = RedisConnectionManager::new(connection_config.clone());
        let pool = Pool::builder(manager).max_size(1).build().expect("pool should build");
        let redis = RedisAsync::new(
            pool,
            None,
            connection_config,
            Some(endpoint_uuid.clone()),
            0,
            MultiKeyExecution::Native,
            Some(poller),
        );
        let redis_clone = redis.clone();

        wait_for_redis_endpoint_in_use(&endpoint_uuid, Some(2)).await;
        drop(redis);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(redis_endpoint_in_use_count(&endpoint_uuid), Some(2));

        drop(redis_clone);
        wait_for_redis_endpoint_in_use(&endpoint_uuid, None).await;

        tokio::time::sleep(Duration::from_millis(30)).await;
        let count_after_drop = poll_count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(poll_count.load(Ordering::Relaxed), count_after_drop);
    }
}
