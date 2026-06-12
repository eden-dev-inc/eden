//! Proxy metrics using fast-telemetry for high-performance counting.
//!
//! Target: <100μs latency for proxy operations.
//!
//! ## Hot Path Usage (Recommended)
//!
//! For the wire protocol processor, use `ProxySeries` for zero-allocation recording:
//!
//! ```ignore
//! // At connection setup:
//! let series = metrics.proxy().series_for_organization(&org_uuid, &interlay_uuid, &endpoint_uuid, "redis");
//!
//! // In hot path (per-request batch):
//! series.record_batch(ProxyBatchRecord {
//!     duration_us,
//!     comparable_duration_us,
//!     endpoint_duration_us,
//!     overhead_us,
//!     bytes_read,
//!     bytes_written,
//!     command_count,
//! });
//! ```

use fast_telemetry::{
    DynamicCounter, DynamicCounterSeries, DynamicDistribution, DynamicDistributionSeries, DynamicGaugeI64, DynamicGaugeI64Series,
    DynamicLabelSet, ExportMetrics,
};
use format::{EdenUuid, EndpointUuid, InterlayUuid, OrganizationUuid};

use crate::labels::{LABEL_ORG_UUID, LABEL_TRAFFIC_CLASS, SYSTEM_ORG_UUID, TRAFFIC_CLASS_EXTERNAL};

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;
const PROXY_DIST_MAX_SERIES: usize = 2000;

fn external_traffic_labels<'a>(labels: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let needs_org = !labels.iter().any(|(key, value)| (*key == LABEL_ORG_UUID || *key == "organization_uuid") && !value.is_empty());
    let needs_traffic_class = !labels.iter().any(|(key, _)| *key == LABEL_TRAFFIC_CLASS);
    let mut out = Vec::with_capacity(labels.len() + usize::from(needs_org) + usize::from(needs_traffic_class));
    out.extend_from_slice(labels);
    if needs_org {
        out.push((LABEL_ORG_UUID, SYSTEM_ORG_UUID));
    }
    if needs_traffic_class {
        out.push((LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL));
    }
    out
}

/// Fast-telemetry metrics for interlay proxy operations.
///
/// All metrics support dynamic labels (`interlay_uuid`, `endpoint_uuid`, `endpoint_kind`, etc.)
/// for per-interlay observability.
#[derive(ExportMetrics)]
#[metric_prefix = "gateway"]
#[otlp]
#[clickhouse]
pub struct ProxyMetrics {
    /// Proxy requests (one per batch of data received over the wire)
    #[help = "Total proxy requests (one per batch of data received)"]
    requests_total: DynamicCounter,

    /// Redis commands processed (individual commands within requests)
    #[help = "Total Redis commands processed"]
    redis_commands_total: DynamicCounter,

    /// Proxy request latency (microseconds, p99 <100μs target)
    #[help = "Proxy request latency in microseconds (target: <100μs)"]
    request_duration_microseconds: DynamicDistribution,

    /// Proxy request latency for batches with comparable endpoint/overhead timing.
    #[help = "Proxy request latency in microseconds for batches with comparable endpoint timing"]
    comparable_request_duration_microseconds: DynamicDistribution,

    /// Command-equivalent latency for batched gateway traffic.
    #[help = "Gateway command-equivalent latency in microseconds, normalized from batched wire traffic"]
    redis_command_duration_microseconds: DynamicDistribution,

    /// Endpoint dispatch duration that is directly comparable to proxy request duration.
    #[help = "Endpoint dispatch latency in microseconds, labeled by interlay and endpoint"]
    endpoint_duration_microseconds: DynamicDistribution,

    /// Command-equivalent endpoint dispatch latency for batched gateway traffic.
    #[help = "Backend endpoint command-equivalent latency in microseconds, normalized from batched wire traffic"]
    redis_command_endpoint_duration_microseconds: DynamicDistribution,

    /// Proxy overhead after subtracting backend dispatch time.
    #[help = "Proxy overhead latency in microseconds, labeled by interlay and endpoint"]
    overhead_duration_microseconds: DynamicDistribution,

    /// Command-equivalent gateway overhead for batched gateway traffic.
    #[help = "Gateway command-equivalent overhead in microseconds, normalized from batched wire traffic"]
    redis_command_overhead_duration_microseconds: DynamicDistribution,

    /// Network latency to backend (Redis)
    #[help = "Network latency to backend in microseconds"]
    network_latency_microseconds: DynamicDistribution,

    /// Time spent waiting in the bridge request queue before protocol processing starts.
    #[help = "Bridge request queue wait latency in microseconds"]
    bridge_request_queue_microseconds: DynamicDistribution,

    /// Time spent waiting in the bridge response queue before writing to the client.
    #[help = "Bridge response queue wait latency in microseconds"]
    bridge_response_queue_microseconds: DynamicDistribution,

    /// Time spent writing proxy responses back to the client socket.
    #[help = "Bridge client write latency in microseconds"]
    bridge_client_write_microseconds: DynamicDistribution,

    /// Time spent parsing wire protocol frames.
    #[help = "Proxy wire protocol parse latency in microseconds"]
    parse_duration_microseconds: DynamicDistribution,

    /// Time spent decoding or scanning wire protocol frames before command materialization.
    #[help = "Proxy wire protocol decode latency in microseconds"]
    parse_decode_duration_microseconds: DynamicDistribution,

    /// Time spent evaluating parse-path gates before protocol decoding begins.
    #[help = "Proxy parse gate latency in microseconds"]
    parse_gate_duration_microseconds: DynamicDistribution,

    /// Time spent materializing parsed command structures from decoded frames.
    #[help = "Proxy command materialization latency in microseconds"]
    parse_materialize_duration_microseconds: DynamicDistribution,

    /// Time spent assembling request bytes for backend dispatch after parse.
    #[help = "Proxy parsed request byte assembly latency in microseconds"]
    parse_copy_duration_microseconds: DynamicDistribution,

    /// Residual parse time not otherwise attributed to decode/materialize/copy.
    #[help = "Proxy parse bookkeeping latency in microseconds"]
    parse_bookkeeping_duration_microseconds: DynamicDistribution,

    /// Time spent applying command policy and resolving route state before backend dispatch.
    #[help = "Proxy policy and routing latency in microseconds"]
    policy_routing_duration_microseconds: DynamicDistribution,

    /// Time spent waiting for a reusable backend connection/pool checkout.
    #[help = "Proxy backend pool wait latency in microseconds"]
    backend_pool_wait_microseconds: DynamicDistribution,

    /// Time spent recording analytics/audit samples after backend execution.
    #[help = "Proxy analytics recording latency in microseconds"]
    analytics_record_duration_microseconds: DynamicDistribution,

    /// Time spent encoding or assembling response bytes before queueing to the bridge.
    #[help = "Proxy response encoding latency in microseconds"]
    response_encode_duration_microseconds: DynamicDistribution,

    /// Time spent waiting on migration coordination locks or write queues.
    #[help = "Proxy migration coordination wait latency in microseconds"]
    migration_wait_duration_microseconds: DynamicDistribution,

    /// Direct migration route decisions made by the gateway.
    #[help = "Direct proxy migration route decisions"]
    direct_migration_routes_total: DynamicCounter,

    /// Time spent executing one direct migration route decision.
    #[help = "Direct proxy migration route execution latency in microseconds"]
    direct_migration_route_duration_microseconds: DynamicDistribution,

    /// Request chunks accepted by the socket bridge and queued for protocol processing.
    #[help = "Proxy bridge request chunks accepted"]
    bridge_request_chunks_total: DynamicCounter,

    /// Response chunks dequeued by the socket bridge for client writes.
    #[help = "Proxy bridge response chunks dequeued"]
    bridge_response_chunks_total: DynamicCounter,

    /// Bridge enqueue failures, labeled by queue and reason.
    #[help = "Proxy bridge enqueue failures"]
    bridge_enqueue_rejections_total: DynamicCounter,

    /// Active proxy connections (gauge - current connection count)
    #[help = "Active proxy connections"]
    active_connections: DynamicGaugeI64,

    /// Bytes read from proxy clients
    #[help = "Proxy bytes read from clients"]
    bytes_read_total: DynamicCounter,

    /// Bytes written to proxy clients
    #[help = "Proxy bytes written to clients"]
    bytes_written_total: DynamicCounter,

    /// Proxy errors
    #[help = "Proxy command errors"]
    errors_total: DynamicCounter,

    /// Proxy connection failures
    #[help = "Proxy connection failures"]
    connection_failures_total: DynamicCounter,

    /// Time the request waits in the multiplexer's bus channel before a worker
    /// accepts it (multiplex.send entry → channel send returns Ok).
    #[help = "Multiplexer bus channel send latency in microseconds"]
    multiplex_bus_send_microseconds: DynamicDistribution,

    /// Time between channel acceptance and the worker beginning dispatch
    /// (channel receive in the worker's run loop).
    #[help = "Multiplexer worker pickup latency in microseconds"]
    multiplex_worker_pickup_microseconds: DynamicDistribution,

    /// Time the worker spends writing the request bytes to the backend TCP
    /// socket.
    #[help = "Multiplexer worker write latency in microseconds"]
    multiplex_write_microseconds: DynamicDistribution,

    /// Time from worker write completion to response read completion. Captures
    /// Redis pipeline depth on the worker connection.
    #[help = "Multiplexer worker pipeline-wait latency in microseconds"]
    multiplex_pipeline_wait_microseconds: DynamicDistribution,

    /// Total wall-clock time spent inside multiplex.send (entry → caller
    /// receives the response oneshot).
    #[help = "Multiplexer total dispatch latency in microseconds"]
    multiplex_total_microseconds: DynamicDistribution,

    /// Time from the reader task fulfilling the response oneshot to the
    /// caller's `await_response` waking up and reading the value.
    /// Isolates tokio scheduler wakeup latency on the response path so
    /// it shows up directly rather than as a residual in
    /// `multi_total - sum_of_phases`.
    #[help = "Multiplexer oneshot delivery latency in microseconds (worker sends → caller wakes)"]
    multiplex_oneshot_delivery_microseconds: DynamicDistribution,

    /// Distribution of command counts per multiplexer.dispatch call.
    /// `pipeline_wait` scales with batch size (Redis processes them
    /// serially on one connection), so this metric is needed to
    /// interpret `pipeline_wait` shifts across architectures.
    #[help = "Number of RESP commands per multiplexer dispatch call"]
    multiplex_dispatch_command_count: DynamicDistribution,

    /// Time from the bridge first observing bytes from the client socket
    /// (after `read_buf` returns) to the moment the multiplexer.dispatch
    /// call is entered for those bytes' commands. Covers bridge parse,
    /// bridge→processor handoff, and processor pre-dispatch work.
    /// Replaces the 0-valued `bridge_request_queue` in the bridge-parse
    /// architecture by anchoring at byte arrival rather than mpsc send.
    #[help = "Time from bridge byte arrival to multiplexer dispatch entry"]
    bridge_recv_to_dispatch_microseconds: DynamicDistribution,

    /// Current multiplexer worker count (gauge, sampled by autoscaler).
    #[help = "Active multiplexer worker connections"]
    multiplex_workers: DynamicGaugeI64,

    /// Sum of in-flight requests across all multiplexer workers (gauge).
    #[help = "In-flight multiplexer requests"]
    multiplex_inflight: DynamicGaugeI64,

    /// Counter, +1 each time the autoscaler appends a worker.
    #[help = "Multiplexer autoscaler scale-up events"]
    multiplex_scale_up_total: DynamicCounter,

    /// Counter, +1 when multiplex.send fails because every worker channel
    /// rejected the request (pool fully unavailable).
    #[help = "Multiplexer dispatch failures (all workers rejected)"]
    multiplex_dispatch_failures_total: DynamicCounter,

    /// Time spent in the bridge's RESP parse loop after a kernel read
    /// returns — i.e., the cost of running `parse_command_view_meta` on the
    /// bytes received from the client socket before they are shipped to the
    /// processor as a `RedisIngressBatch`.
    #[help = "Bridge-side RESP parse latency in microseconds"]
    bridge_parse_microseconds: DynamicDistribution,

    /// Full end-to-end proxy-induced latency for a batch, measured from the
    /// moment the bridge finishes receiving bytes from the client socket
    /// until the bridge finishes writing the corresponding response back to
    /// the client. Captures every internal hop: bridge parse, processor
    /// queue wait, processor work, multiplexer dispatch, backend round-trip,
    /// response queue, and bridge write.
    #[help = "Full proxy-induced end-to-end latency per batch in microseconds"]
    end_to_end_microseconds: DynamicDistribution,

    /// Command-equivalent end-to-end proxy-induced latency for pipelined
    /// Redis batches. Recorded alongside `end_to_end_microseconds` so
    /// dashboards can distinguish per-command latency from full batch drain.
    #[help = "Command-equivalent proxy-induced end-to-end latency in microseconds"]
    redis_command_end_to_end_microseconds: DynamicDistribution,

    /// Number of connections currently dispatched to each thread-per-core
    /// shard runtime (label: `shard_id`). Mirrors the per-router inflight
    /// counter that backs two-choice load balancing — exposing it as a
    /// metric lets us verify that `assign_shards` + `pick_shorter` are
    /// distributing connections rather than crowding one shard.
    #[help = "Connections currently dispatched to each proxy shard runtime"]
    shard_connections_active: DynamicGaugeI64,

    /// In-flight per-batch requests across all connections on a shard
    /// (label: `shard_id`). Distinct from `shard_connections_active`:
    /// counts request batches inside each connection. Currently 0 (per-
    /// batch dispatch hasn't landed); the metric is wired now so the
    /// dashboards / alerting are ready when per-batch routing lands.
    #[help = "In-flight request batches per proxy shard runtime"]
    shard_requests_inflight: DynamicGaugeI64,

    /// Counter, +1 each time a `ShardRouter::dispatch` fails (the shard's
    /// inbox mpsc was closed before the work could be enqueued). Labels:
    /// `shard_id`, `reason`. Expected to be ~zero in steady state — non-
    /// zero values mean a shard runtime exited unexpectedly.
    #[help = "ShardRouter dispatch failures by shard and reason"]
    shard_dispatch_failures_total: DynamicCounter,

    /// Counter, +1 each time `spawn_on_current_runtime` routes through
    /// `tokio::task::spawn_local` (label: `shard_id`). Tracks the rate at
    /// which per-connection helper tasks are being pinned to each shard.
    /// Useful for catching regressions where hot-path work accidentally
    /// falls back to `tokio::spawn` (i.e., zero growth on a busy shard).
    #[help = "Tasks spawned via spawn_local on each shard runtime"]
    shard_local_tasks_spawned_total: DynamicCounter,

    /// Number of backend Redis connections ("lanes") currently open in
    /// the direct-proxy lane pool, per (shard, interlay). Compared
    /// against the configured target (`EDEN_DIRECT_POOL_SIZE_PER_SHARD`)
    /// to spot pools that haven't grown to capacity vs. ones that are
    /// fully saturated. Labels: `shard_id`, `interlay_uuid`.
    #[help = "Open backend lanes in the direct-proxy lane pool per shard/interlay"]
    lane_pool_lanes_open: DynamicGaugeI64,

    /// Time a client task spent parked on a lane-pool waiter queue
    /// before acquiring a free lane. Zero when the pool isn't
    /// saturated; rising values mean the pool size is the throughput
    /// bottleneck. Labels: `shard_id`, `interlay_uuid`.
    #[help = "Wait time for a free direct-proxy lane in microseconds"]
    lane_pool_acquire_wait_microseconds: DynamicDistribution,

    /// Number of client tasks currently parked waiting for a free
    /// lane on a given (shard, interlay) pool. Labels: `shard_id`,
    /// `interlay_uuid`.
    #[help = "Client tasks currently waiting for a free lane"]
    lane_pool_waiters: DynamicGaugeI64,

    /// Lane-pool initialization lifecycle events. Labels: `shard_id`,
    /// `interlay_uuid`, `result`, `reason`.
    #[help = "Direct lane-pool initialization lifecycle events"]
    lane_pool_init_events_total: DynamicCounter,

    /// Wall-clock time spent initializing direct lane-pool backend
    /// connections. Labels: `shard_id`, `interlay_uuid`, `result`,
    /// `reason`.
    #[help = "Direct lane-pool initialization duration in microseconds"]
    lane_pool_init_duration_microseconds: DynamicDistribution,

    /// Wall-clock time concurrent callers spent waiting for an in-flight
    /// lane-pool initialization attempt. Labels: `shard_id`,
    /// `interlay_uuid`, `result`, `reason`.
    #[help = "Direct lane-pool initialization waiter duration in microseconds"]
    lane_pool_init_wait_microseconds: DynamicDistribution,

    /// Direct Redis client connections that entered the safe lane-pool
    /// mode. Labels: `interlay_uuid`, `endpoint_uuid`, `endpoint_kind`.
    #[help = "Direct Redis client connections that entered safe lane-pool mode"]
    direct_safe_connections_total: DynamicCounter,

    /// Direct Redis client connections promoted to unsafe pinned mode.
    /// Labels: `interlay_uuid`, `endpoint_uuid`, `endpoint_kind`,
    /// `reason`.
    #[help = "Direct Redis client connections promoted to unsafe pinned mode"]
    direct_unsafe_connections_total: DynamicCounter,

    /// Direct Redis client connections currently in safe lane-pool mode.
    /// Labels: `interlay_uuid`, `endpoint_uuid`, `endpoint_kind`.
    #[help = "Direct Redis client connections currently in safe lane-pool mode"]
    direct_safe_connections_active: DynamicGaugeI64,

    /// Direct Redis client connections currently in unsafe pinned mode.
    /// Labels: `interlay_uuid`, `endpoint_uuid`, `endpoint_kind`,
    /// `reason`.
    #[help = "Direct Redis client connections currently in unsafe pinned mode"]
    direct_unsafe_connections_active: DynamicGaugeI64,

    /// Direct Redis shard-state update dispatch or acknowledgement
    /// failures. Migration updates label by `interlay_uuid`,
    /// `shard_id`, and `reason`; lifecycle cleanup labels by
    /// `operation` (`interlay_clear`, `interlay_retire`, or
    /// `endpoint_evict`), object UUID, `shard_id`, `cleanup_reason`,
    /// and `reason`.
    #[help = "Direct Redis shard-state update dispatch failures"]
    direct_state_update_dispatch_failures_total: DynamicCounter,

    /// Mirror endpoint dispatch attempts.
    /// Labels: `interlay_uuid`, `primary_endpoint_uuid`,
    /// `mirror_endpoint_uuid`, `endpoint_kind`, `req_type`.
    #[help = "Total mirror endpoint dispatch attempts"]
    mirror_requests_total: DynamicCounter,

    /// Mirror endpoint dispatch latency.
    /// Labels match `mirror_requests_total`.
    #[help = "Mirror endpoint dispatch latency in microseconds"]
    mirror_latency_microseconds: DynamicDistribution,

    /// Mirror endpoint dispatch errors.
    /// Labels add `reason`.
    #[help = "Total mirror endpoint dispatch errors"]
    mirror_errors_total: DynamicCounter,

    /// Mirror dispatch skips.
    /// Labels add `reason`.
    #[help = "Total mirror dispatch skips"]
    mirror_skipped_total: DynamicCounter,

    /// Mirror response divergence observations.
    /// Labels add `reason`.
    #[help = "Total mirror response divergence observations"]
    mirror_divergence_total: DynamicCounter,
}

impl ProxyMetrics {
    /// Create new ProxyMetrics with fast-telemetry counters.
    pub fn new() -> Self {
        Self {
            requests_total: DynamicCounter::new(SHARD_COUNT),
            redis_commands_total: DynamicCounter::new(SHARD_COUNT),
            request_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            comparable_request_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            redis_command_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            endpoint_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            redis_command_endpoint_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            overhead_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            redis_command_overhead_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            network_latency_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            bridge_request_queue_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            bridge_response_queue_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            bridge_client_write_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            parse_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            parse_decode_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            parse_gate_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            parse_materialize_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            parse_copy_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            parse_bookkeeping_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            policy_routing_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            backend_pool_wait_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            analytics_record_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            response_encode_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            migration_wait_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            direct_migration_routes_total: DynamicCounter::new(SHARD_COUNT),
            direct_migration_route_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            bridge_request_chunks_total: DynamicCounter::new(SHARD_COUNT),
            bridge_response_chunks_total: DynamicCounter::new(SHARD_COUNT),
            bridge_enqueue_rejections_total: DynamicCounter::new(SHARD_COUNT),
            active_connections: DynamicGaugeI64::new(SHARD_COUNT),
            bytes_read_total: DynamicCounter::new(SHARD_COUNT),
            bytes_written_total: DynamicCounter::new(SHARD_COUNT),
            errors_total: DynamicCounter::new(SHARD_COUNT),
            connection_failures_total: DynamicCounter::new(SHARD_COUNT),
            multiplex_bus_send_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_worker_pickup_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_write_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_pipeline_wait_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_total_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_oneshot_delivery_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_dispatch_command_count: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            bridge_recv_to_dispatch_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            multiplex_workers: DynamicGaugeI64::new(SHARD_COUNT),
            multiplex_inflight: DynamicGaugeI64::new(SHARD_COUNT),
            multiplex_scale_up_total: DynamicCounter::new(SHARD_COUNT),
            multiplex_dispatch_failures_total: DynamicCounter::new(SHARD_COUNT),
            bridge_parse_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            end_to_end_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            redis_command_end_to_end_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            shard_connections_active: DynamicGaugeI64::new(SHARD_COUNT),
            shard_requests_inflight: DynamicGaugeI64::new(SHARD_COUNT),
            shard_dispatch_failures_total: DynamicCounter::new(SHARD_COUNT),
            shard_local_tasks_spawned_total: DynamicCounter::new(SHARD_COUNT),
            lane_pool_lanes_open: DynamicGaugeI64::new(SHARD_COUNT),
            lane_pool_acquire_wait_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            lane_pool_waiters: DynamicGaugeI64::new(SHARD_COUNT),
            lane_pool_init_events_total: DynamicCounter::new(SHARD_COUNT),
            lane_pool_init_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            lane_pool_init_wait_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            direct_safe_connections_total: DynamicCounter::new(SHARD_COUNT),
            direct_unsafe_connections_total: DynamicCounter::new(SHARD_COUNT),
            direct_safe_connections_active: DynamicGaugeI64::new(SHARD_COUNT),
            direct_unsafe_connections_active: DynamicGaugeI64::new(SHARD_COUNT),
            direct_state_update_dispatch_failures_total: DynamicCounter::new(SHARD_COUNT),
            mirror_requests_total: DynamicCounter::new(SHARD_COUNT),
            mirror_latency_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, PROXY_DIST_MAX_SERIES),
            mirror_errors_total: DynamicCounter::new(SHARD_COUNT),
            mirror_skipped_total: DynamicCounter::new(SHARD_COUNT),
            mirror_divergence_total: DynamicCounter::new(SHARD_COUNT),
        }
    }

    // === Hot Path Series Handles ===
    //
    // For hot paths, resolve a series handle once and use it for repeated updates.
    // This avoids per-call label canonicalization and map lookups.

    /// Get a reusable series handle for requests counter.
    ///
    /// Hot path optimization: resolve once, increment via handle.
    /// ```ignore
    /// let series = metrics.requests_series(&[("interlay_uuid", id), ("endpoint_uuid", ep)]);
    /// series.inc();
    /// ```
    pub fn requests_series(&self, labels: &[(&str, &str)]) -> DynamicCounterSeries {
        let labels = external_traffic_labels(labels);
        self.requests_total.series(&labels)
    }

    /// Get a reusable series handle for commands counter.
    pub fn commands_series(&self, labels: &[(&str, &str)]) -> DynamicCounterSeries {
        let labels = external_traffic_labels(labels);
        self.redis_commands_total.series(&labels)
    }

    /// Get a reusable series handle for request duration distribution.
    pub fn duration_series(&self, labels: &[(&str, &str)]) -> DynamicDistributionSeries {
        let labels = external_traffic_labels(labels);
        self.request_duration_microseconds.series(&labels)
    }

    /// Get a reusable series handle for comparable request duration distribution.
    pub fn comparable_duration_series(&self, labels: &[(&str, &str)]) -> DynamicDistributionSeries {
        let labels = external_traffic_labels(labels);
        self.comparable_request_duration_microseconds.series(&labels)
    }

    /// Get a reusable series handle for comparable endpoint dispatch duration distribution.
    pub fn endpoint_duration_series(&self, labels: &[(&str, &str)]) -> DynamicDistributionSeries {
        let labels = external_traffic_labels(labels);
        self.endpoint_duration_microseconds.series(&labels)
    }

    /// Get a reusable series handle for proxy overhead duration distribution.
    pub fn overhead_duration_series(&self, labels: &[(&str, &str)]) -> DynamicDistributionSeries {
        let labels = external_traffic_labels(labels);
        self.overhead_duration_microseconds.series(&labels)
    }

    /// Get a reusable series handle for bytes read counter.
    pub fn bytes_read_series(&self, labels: &[(&str, &str)]) -> DynamicCounterSeries {
        let labels = external_traffic_labels(labels);
        self.bytes_read_total.series(&labels)
    }

    /// Get a reusable series handle for bytes written counter.
    pub fn bytes_written_series(&self, labels: &[(&str, &str)]) -> DynamicCounterSeries {
        let labels = external_traffic_labels(labels);
        self.bytes_written_total.series(&labels)
    }

    /// Create cached series handles for a Redis mirror target.
    pub fn mirror_series(&self, labels: &[(&str, &str)], upstream_error_labels: &[(&str, &str)]) -> ProxyMirrorSeries {
        let labels = external_traffic_labels(labels);
        let upstream_error_labels = external_traffic_labels(upstream_error_labels);
        ProxyMirrorSeries {
            requests: self.mirror_requests_total.series(&labels),
            latency: self.mirror_latency_microseconds.series(&labels),
            upstream_errors: self.mirror_errors_total.series(&upstream_error_labels),
        }
    }

    /// Create cached series handles for Redis multiplexer hot metrics.
    pub fn multiplex_series(&self, labels: &[(&str, &str)]) -> ProxyMultiplexSeries {
        let labels = external_traffic_labels(labels);
        ProxyMultiplexSeries {
            bus_send: self.multiplex_bus_send_microseconds.series(&labels),
            worker_pickup: self.multiplex_worker_pickup_microseconds.series(&labels),
            write: self.multiplex_write_microseconds.series(&labels),
            pipeline_wait: self.multiplex_pipeline_wait_microseconds.series(&labels),
            total: self.multiplex_total_microseconds.series(&labels),
            oneshot_delivery: self.multiplex_oneshot_delivery_microseconds.series(&labels),
            dispatch_command_count: self.multiplex_dispatch_command_count.series(&labels),
            dispatch_failures: self.multiplex_dispatch_failures_total.series(&labels),
        }
    }

    /// Create cached series handles for bridge metrics recorded on every
    /// request/response chunk.
    pub fn bridge_series(&self, labels: &[(&str, &str)]) -> ProxyBridgeSeries {
        let labels = external_traffic_labels(labels);
        ProxyBridgeSeries {
            request_chunks: self.bridge_request_chunks_total.series(&labels),
            request_queue: self.bridge_request_queue_microseconds.series(&labels),
            response_chunks: self.bridge_response_chunks_total.series(&labels),
            response_queue: self.bridge_response_queue_microseconds.series(&labels),
            client_write: self.bridge_client_write_microseconds.series(&labels),
            bridge_parse: self.bridge_parse_microseconds.series(&labels),
            end_to_end: self.end_to_end_microseconds.series(&labels),
            command_end_to_end: self.redis_command_end_to_end_microseconds.series(&labels),
        }
    }

    /// Create cached series handles for direct Redis lane-pool hot metrics.
    pub fn lane_pool_series(&self, labels: &[(&str, &str)]) -> ProxyLanePoolSeries {
        let labels = external_traffic_labels(labels);
        ProxyLanePoolSeries {
            lanes_open: self.lane_pool_lanes_open.series(&labels),
            acquire_wait: self.lane_pool_acquire_wait_microseconds.series(&labels),
            waiters: self.lane_pool_waiters.series(&labels),
        }
    }

    // === Public Recording Methods ===

    /// Record proxy request (one per batch of data received over the wire)
    #[inline]
    pub fn record_request(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.requests_total += 1 (DynamicCounter)");
        self.requests_total.inc(labels);
    }

    /// Record Redis commands processed
    #[inline]
    pub fn record_commands(&self, count: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.redis.commands_total += {} (DynamicCounter)", count);
        self.redis_commands_total.add(labels, count as isize);
    }

    /// Record proxy request duration in microseconds
    #[inline]
    pub fn record_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.request_duration_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.request_duration_microseconds.record(labels, duration_us);
    }

    /// Record comparable proxy request duration in microseconds.
    #[inline]
    pub fn record_comparable_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.comparable_request_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.comparable_request_duration_microseconds.record(labels, duration_us);
    }

    /// Record comparable endpoint dispatch duration in microseconds.
    #[inline]
    pub fn record_endpoint_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.endpoint_duration_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.endpoint_duration_microseconds.record(labels, duration_us);
    }

    /// Record proxy overhead duration in microseconds.
    #[inline]
    pub fn record_overhead_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.overhead_duration_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.overhead_duration_microseconds.record(labels, duration_us);
    }

    /// Record network latency to backend in microseconds
    #[inline]
    pub fn record_network_latency(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.network_latency_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.network_latency_microseconds.record(labels, duration_us);
    }

    /// Record time waiting in the bridge request queue before processor receive.
    #[inline]
    pub fn record_bridge_request_queue(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bridge_request_queue_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.bridge_request_queue_microseconds.record(labels, duration_us);
    }

    /// Record time waiting in the bridge response queue before client write.
    #[inline]
    pub fn record_bridge_response_queue(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.bridge_response_queue_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.bridge_response_queue_microseconds.record(labels, duration_us);
    }

    /// Record time writing a response chunk back to the client.
    #[inline]
    pub fn record_bridge_client_write(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bridge_client_write_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.bridge_client_write_microseconds.record(labels, duration_us);
    }

    /// Record wire protocol parse duration.
    #[inline]
    pub fn record_parse_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.parse_duration_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.parse_duration_microseconds.record(labels, duration_us);
    }

    /// Record wire protocol decode/scanning duration.
    #[inline]
    pub fn record_parse_decode_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.parse_decode_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.parse_decode_duration_microseconds.record(labels, duration_us);
    }

    /// Record parse gate duration before protocol decoding begins.
    #[inline]
    pub fn record_parse_gate_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.parse_gate_duration_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.parse_gate_duration_microseconds.record(labels, duration_us);
    }

    /// Record command materialization duration.
    #[inline]
    pub fn record_parse_materialize_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.parse_materialize_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.parse_materialize_duration_microseconds.record(labels, duration_us);
    }

    /// Record parsed request byte assembly duration.
    #[inline]
    pub fn record_parse_copy_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.parse_copy_duration_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.parse_copy_duration_microseconds.record(labels, duration_us);
    }

    /// Record residual parse bookkeeping duration.
    #[inline]
    pub fn record_parse_bookkeeping_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.parse_bookkeeping_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.parse_bookkeeping_duration_microseconds.record(labels, duration_us);
    }

    /// Record policy/routing duration before backend dispatch.
    #[inline]
    pub fn record_policy_routing_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.policy_routing_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.policy_routing_duration_microseconds.record(labels, duration_us);
    }

    /// Record backend pool/pinned-connection wait duration.
    #[inline]
    pub fn record_backend_pool_wait(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.backend_pool_wait_microseconds = {}μs (DynamicHistogram)", duration_us);
        self.backend_pool_wait_microseconds.record(labels, duration_us);
    }

    /// Record analytics/audit recording duration.
    #[inline]
    pub fn record_analytics_record_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.analytics_record_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.analytics_record_duration_microseconds.record(labels, duration_us);
    }

    /// Record response encoding/assembly duration.
    #[inline]
    pub fn record_response_encode_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.response_encode_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.response_encode_duration_microseconds.record(labels, duration_us);
    }

    /// Record migration coordination wait duration.
    #[inline]
    pub fn record_migration_wait_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.migration_wait_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.migration_wait_duration_microseconds.record(labels, duration_us);
    }

    /// Record a direct migration route decision.
    #[inline]
    pub fn record_direct_migration_route(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.direct_migration_routes_total += 1 (DynamicCounter)");
        self.direct_migration_routes_total.inc(labels);
    }

    /// Record direct migration route execution duration.
    #[inline]
    pub fn record_direct_migration_route_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] gateway.direct_migration_route_duration_microseconds = {}μs (DynamicHistogram)",
            duration_us
        );
        self.direct_migration_route_duration_microseconds.record(labels, duration_us);
    }

    /// Record bridge request chunk acceptance.
    #[inline]
    pub fn record_bridge_request_chunk(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bridge_request_chunks_total += 1 (DynamicCounter)");
        self.bridge_request_chunks_total.inc(labels);
    }

    /// Record bridge response chunk dequeue.
    #[inline]
    pub fn record_bridge_response_chunk(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bridge_response_chunks_total += 1 (DynamicCounter)");
        self.bridge_response_chunks_total.inc(labels);
    }

    /// Record bridge enqueue rejection.
    #[inline]
    pub fn record_bridge_enqueue_rejection(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bridge_enqueue_rejections_total += 1 (DynamicCounter)");
        self.bridge_enqueue_rejections_total.inc(labels);
    }

    /// Record bytes read from proxy clients
    #[inline]
    pub fn record_bytes_read(&self, bytes: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bytes_read_total += {} bytes (DynamicCounter)", bytes);
        self.bytes_read_total.add(labels, bytes as isize);
    }

    /// Record bytes written to proxy clients
    #[inline]
    pub fn record_bytes_written(&self, bytes: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.bytes_written_total += {} bytes (DynamicCounter)", bytes);
        self.bytes_written_total.add(labels, bytes as isize);
    }

    /// Record proxy error
    #[inline]
    pub fn record_error(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.errors_total += 1 (DynamicCounter)");
        self.errors_total.inc(labels);
    }

    /// Record proxy connection failure
    #[inline]
    pub fn record_connection_failure(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] gateway.connection_failures_total += 1 (DynamicCounter)");
        self.connection_failures_total.inc(labels);
    }

    /// Record bus-channel send latency for a multiplexer dispatch.
    #[inline]
    pub fn record_multiplex_bus_send(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.multiplex_bus_send_microseconds.record(labels, duration_us);
    }

    /// Record worker-pickup latency (channel accepted → worker dispatch entry).
    #[inline]
    pub fn record_multiplex_worker_pickup(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.multiplex_worker_pickup_microseconds.record(labels, duration_us);
    }

    /// Record write latency at the worker (TCP write of the request bytes).
    #[inline]
    pub fn record_multiplex_write(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.multiplex_write_microseconds.record(labels, duration_us);
    }

    /// Record pipeline-wait latency (write done → response read done).
    #[inline]
    pub fn record_multiplex_pipeline_wait(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.multiplex_pipeline_wait_microseconds.record(labels, duration_us);
    }

    /// Record total multiplex.send latency.
    #[inline]
    pub fn record_multiplex_total(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.multiplex_total_microseconds.record(labels, duration_us);
    }

    /// Record oneshot delivery latency (worker fulfilled response_tx →
    /// caller's await_response woke up). Surfaces the scheduler wakeup
    /// component that would otherwise appear only as residual in
    /// `multi_total - sum_of_phases`.
    #[inline]
    pub fn record_multiplex_oneshot_delivery(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.multiplex_oneshot_delivery_microseconds.record(labels, duration_us);
    }

    /// Record the number of RESP commands in a multiplexer dispatch call.
    /// Recorded as microseconds for consistency with the distribution
    /// surface; the value is the raw command count, not a duration.
    #[inline]
    pub fn record_multiplex_dispatch_command_count(&self, count: u64, labels: &[(&str, &str)]) {
        self.multiplex_dispatch_command_count.record(labels, count);
    }

    /// Record time from bridge byte arrival (read_buf return) to
    /// multiplexer dispatch entry.
    #[inline]
    pub fn record_bridge_recv_to_dispatch(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.bridge_recv_to_dispatch_microseconds.record(labels, duration_us);
    }

    /// Set the current multiplexer worker count gauge.
    #[inline]
    pub fn set_multiplex_workers(&self, count: i64, labels: &[(&str, &str)]) {
        self.multiplex_workers.set(labels, count);
    }

    /// Set the current multiplexer in-flight count gauge.
    #[inline]
    pub fn set_multiplex_inflight(&self, count: i64, labels: &[(&str, &str)]) {
        self.multiplex_inflight.set(labels, count);
    }

    /// Increment the autoscaler scale-up counter.
    #[inline]
    pub fn record_multiplex_scale_up(&self, labels: &[(&str, &str)]) {
        self.multiplex_scale_up_total.inc(labels);
    }

    /// Increment the dispatch-failures counter.
    #[inline]
    pub fn record_multiplex_dispatch_failure(&self, labels: &[(&str, &str)]) {
        self.multiplex_dispatch_failures_total.inc(labels);
    }

    /// Set the active-connections gauge for a shard.
    #[inline]
    pub fn set_shard_connections_active(&self, count: i64, labels: &[(&str, &str)]) {
        self.shard_connections_active.set(labels, count);
    }

    /// Set the in-flight per-batch requests gauge for a shard.
    #[inline]
    pub fn set_shard_requests_inflight(&self, count: i64, labels: &[(&str, &str)]) {
        self.shard_requests_inflight.set(labels, count);
    }

    /// Increment the shard dispatch-failure counter (one per failed
    /// `ShardRouter::dispatch`). `labels` should include `shard_id` and a
    /// `reason` like `"shard_closed"`.
    #[inline]
    pub fn record_shard_dispatch_failure(&self, labels: &[(&str, &str)]) {
        self.shard_dispatch_failures_total.inc(labels);
    }

    /// Increment the spawn_local counter on a shard. Called from
    /// `eden_gateway_core::runtime::spawn_on_current_runtime` whenever it
    /// routes through `tokio::task::spawn_local` (i.e., when the calling
    /// thread is a marked shard runtime thread).
    #[inline]
    pub fn record_shard_local_task_spawned(&self, labels: &[(&str, &str)]) {
        self.shard_local_tasks_spawned_total.inc(labels);
    }

    /// Set the open-lanes gauge for the direct-proxy lane pool on a
    /// given (`shard_id`, `interlay_uuid`). Capacity is implied by
    /// `EDEN_DIRECT_POOL_SIZE_PER_SHARD`; rising values trace lane
    /// growth, plateaus mean the pool has reached target.
    #[inline]
    pub fn set_lane_pool_lanes_open(&self, count: i64, labels: &[(&str, &str)]) {
        self.lane_pool_lanes_open.set(labels, count);
    }

    /// Record how long a client task waited for a free lane (only
    /// emitted when the wait was non-zero).
    #[inline]
    pub fn record_lane_pool_acquire_wait(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.lane_pool_acquire_wait_microseconds.record(labels, duration_us);
    }

    /// Set the gauge of client tasks currently parked on the lane
    /// pool's waiter queue. Non-zero values indicate the pool is
    /// saturated and the per-shard target is too small.
    #[inline]
    pub fn set_lane_pool_waiters(&self, count: i64, labels: &[(&str, &str)]) {
        self.lane_pool_waiters.set(labels, count);
    }

    /// Increment a direct lane-pool initialization lifecycle event.
    /// Labels should include stable `result` and `reason` values.
    #[inline]
    pub fn record_lane_pool_init_event(&self, labels: &[(&str, &str)]) {
        self.lane_pool_init_events_total.inc(labels);
    }

    /// Record the wall-clock duration of a direct lane-pool
    /// initialization attempt.
    #[inline]
    pub fn record_lane_pool_init_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.lane_pool_init_duration_microseconds.record(labels, duration_us);
    }

    /// Record the time a caller waited on an already-running direct
    /// lane-pool initialization attempt.
    #[inline]
    pub fn record_lane_pool_init_wait_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.lane_pool_init_wait_microseconds.record(labels, duration_us);
    }

    /// Increment the counter for a direct Redis connection entering the
    /// safe lane-pool path.
    #[inline]
    pub fn record_direct_safe_connection(&self, labels: &[(&str, &str)]) {
        self.direct_safe_connections_total.inc(labels);
    }

    /// Increment the counter for a direct Redis connection promoted to
    /// unsafe pinned mode. Labels should include a stable `reason`.
    #[inline]
    pub fn record_direct_unsafe_connection(&self, labels: &[(&str, &str)]) {
        self.direct_unsafe_connections_total.inc(labels);
    }

    /// Update the current number of direct Redis connections in safe
    /// lane-pool mode.
    #[inline]
    pub fn update_direct_safe_connections(&self, delta: i64, labels: &[(&str, &str)]) {
        self.direct_safe_connections_active.add(labels, delta);
    }

    /// Update the current number of direct Redis connections in unsafe
    /// pinned mode. Labels should include a stable `reason`.
    #[inline]
    pub fn update_direct_unsafe_connections(&self, delta: i64, labels: &[(&str, &str)]) {
        self.direct_unsafe_connections_active.add(labels, delta);
    }

    /// Increment the counter for a failed direct-state update broadcast
    /// to a shard.
    #[inline]
    pub fn record_direct_state_update_dispatch_failure(&self, labels: &[(&str, &str)]) {
        self.direct_state_update_dispatch_failures_total.inc(labels);
    }

    /// Increment a mirror dispatch attempt.
    #[inline]
    pub fn record_mirror_request(&self, labels: &[(&str, &str)]) {
        self.mirror_requests_total.inc(labels);
    }

    /// Record mirror dispatch latency.
    #[inline]
    pub fn record_mirror_latency(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.mirror_latency_microseconds.record(labels, duration_us);
    }

    /// Increment a mirror dispatch error.
    #[inline]
    pub fn record_mirror_error(&self, labels: &[(&str, &str)]) {
        self.mirror_errors_total.inc(labels);
    }

    /// Increment a mirror skip.
    #[inline]
    pub fn record_mirror_skip(&self, labels: &[(&str, &str)]) {
        self.mirror_skipped_total.inc(labels);
    }

    /// Increment a mirror response divergence observation.
    #[inline]
    pub fn record_mirror_divergence(&self, labels: &[(&str, &str)]) {
        self.mirror_divergence_total.inc(labels);
    }

    /// Record bridge-side RESP parse latency.
    #[inline]
    pub fn record_bridge_parse_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.bridge_parse_microseconds.record(labels, duration_us);
    }

    /// Record full end-to-end proxy-induced latency for a batch.
    #[inline]
    pub fn record_end_to_end_duration(&self, duration_us: u64, labels: &[(&str, &str)]) {
        self.end_to_end_microseconds.record(labels, duration_us);
    }

    /// Record command-equivalent end-to-end proxy-induced latency.
    #[inline]
    pub fn record_command_end_to_end_duration(&self, duration_us: u64, command_count: u64, labels: &[(&str, &str)]) {
        if let Some(command_duration_us) = command_equivalent_duration_us(duration_us, command_count) {
            self.redis_command_end_to_end_microseconds.record(labels, command_duration_us);
        }
    }

    // === Snapshot Methods ===

    pub fn get_requests_total(&self) -> u64 {
        self.requests_total.sum_all() as u64
    }

    pub fn get_commands_total(&self) -> u64 {
        self.redis_commands_total.sum_all() as u64
    }

    pub fn get_active_connections(&self) -> i64 {
        self.active_connections.sum_all()
    }

    pub fn get_bytes_read_total(&self) -> u64 {
        self.bytes_read_total.sum_all() as u64
    }

    pub fn get_bytes_written_total(&self) -> u64 {
        self.bytes_written_total.sum_all() as u64
    }

    pub fn get_errors_total(&self) -> u64 {
        self.errors_total.sum_all() as u64
    }

    pub fn get_connection_failures_total(&self) -> u64 {
        self.connection_failures_total.sum_all() as u64
    }

    /// Snapshot current active proxy connections grouped by dynamic labels.
    pub fn snapshot_active_connections(&self) -> Vec<(DynamicLabelSet, i64)> {
        self.active_connections.snapshot()
    }

    /// Evict stale series from all proxy metrics.
    ///
    /// Series that haven't been accessed for `max_staleness` cycles are removed.
    /// Returns total number of series evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        // Counters
        evicted += self.requests_total.evict_stale(max_staleness);
        evicted += self.redis_commands_total.evict_stale(max_staleness);
        evicted += self.bytes_read_total.evict_stale(max_staleness);
        evicted += self.bytes_written_total.evict_stale(max_staleness);
        evicted += self.errors_total.evict_stale(max_staleness);
        evicted += self.connection_failures_total.evict_stale(max_staleness);
        evicted += self.bridge_request_chunks_total.evict_stale(max_staleness);
        evicted += self.bridge_response_chunks_total.evict_stale(max_staleness);
        evicted += self.bridge_enqueue_rejections_total.evict_stale(max_staleness);
        evicted += self.direct_migration_routes_total.evict_stale(max_staleness);
        evicted += self.lane_pool_init_events_total.evict_stale(max_staleness);
        evicted += self.direct_safe_connections_total.evict_stale(max_staleness);
        evicted += self.direct_unsafe_connections_total.evict_stale(max_staleness);
        evicted += self.direct_state_update_dispatch_failures_total.evict_stale(max_staleness);
        evicted += self.mirror_requests_total.evict_stale(max_staleness);
        evicted += self.mirror_errors_total.evict_stale(max_staleness);
        evicted += self.mirror_skipped_total.evict_stale(max_staleness);
        evicted += self.mirror_divergence_total.evict_stale(max_staleness);
        // Gauges
        evicted += self.active_connections.evict_stale(max_staleness);
        evicted += self.direct_safe_connections_active.evict_stale(max_staleness);
        evicted += self.direct_unsafe_connections_active.evict_stale(max_staleness);
        // Distributions
        evicted += self.request_duration_microseconds.evict_stale(max_staleness);
        evicted += self.comparable_request_duration_microseconds.evict_stale(max_staleness);
        evicted += self.redis_command_duration_microseconds.evict_stale(max_staleness);
        evicted += self.endpoint_duration_microseconds.evict_stale(max_staleness);
        evicted += self.redis_command_endpoint_duration_microseconds.evict_stale(max_staleness);
        evicted += self.overhead_duration_microseconds.evict_stale(max_staleness);
        evicted += self.redis_command_overhead_duration_microseconds.evict_stale(max_staleness);
        evicted += self.network_latency_microseconds.evict_stale(max_staleness);
        evicted += self.bridge_request_queue_microseconds.evict_stale(max_staleness);
        evicted += self.bridge_response_queue_microseconds.evict_stale(max_staleness);
        evicted += self.bridge_client_write_microseconds.evict_stale(max_staleness);
        evicted += self.parse_duration_microseconds.evict_stale(max_staleness);
        evicted += self.parse_decode_duration_microseconds.evict_stale(max_staleness);
        evicted += self.parse_gate_duration_microseconds.evict_stale(max_staleness);
        evicted += self.parse_materialize_duration_microseconds.evict_stale(max_staleness);
        evicted += self.parse_copy_duration_microseconds.evict_stale(max_staleness);
        evicted += self.parse_bookkeeping_duration_microseconds.evict_stale(max_staleness);
        evicted += self.policy_routing_duration_microseconds.evict_stale(max_staleness);
        evicted += self.backend_pool_wait_microseconds.evict_stale(max_staleness);
        evicted += self.analytics_record_duration_microseconds.evict_stale(max_staleness);
        evicted += self.response_encode_duration_microseconds.evict_stale(max_staleness);
        evicted += self.migration_wait_duration_microseconds.evict_stale(max_staleness);
        evicted += self.direct_migration_route_duration_microseconds.evict_stale(max_staleness);
        evicted += self.lane_pool_init_duration_microseconds.evict_stale(max_staleness);
        evicted += self.lane_pool_init_wait_microseconds.evict_stale(max_staleness);
        evicted += self.mirror_latency_microseconds.evict_stale(max_staleness);
        evicted += self.end_to_end_microseconds.evict_stale(max_staleness);
        evicted += self.redis_command_end_to_end_microseconds.evict_stale(max_staleness);
        evicted
    }

    /// Get current cardinality (number of unique label sets) across all metrics.
    pub fn cardinality(&self) -> usize {
        self.series_cardinality()
    }

    /// Get total series cardinality across all dynamic proxy metrics.
    pub fn series_cardinality(&self) -> usize {
        self.requests_total.cardinality()
            + self.redis_commands_total.cardinality()
            + self.bytes_read_total.cardinality()
            + self.bytes_written_total.cardinality()
            + self.errors_total.cardinality()
            + self.connection_failures_total.cardinality()
            + self.bridge_request_chunks_total.cardinality()
            + self.bridge_response_chunks_total.cardinality()
            + self.bridge_enqueue_rejections_total.cardinality()
            + self.direct_migration_routes_total.cardinality()
            + self.lane_pool_init_events_total.cardinality()
            + self.active_connections.cardinality()
            + self.direct_safe_connections_total.cardinality()
            + self.direct_unsafe_connections_total.cardinality()
            + self.direct_state_update_dispatch_failures_total.cardinality()
            + self.mirror_requests_total.cardinality()
            + self.mirror_errors_total.cardinality()
            + self.mirror_skipped_total.cardinality()
            + self.mirror_divergence_total.cardinality()
            + self.direct_safe_connections_active.cardinality()
            + self.direct_unsafe_connections_active.cardinality()
            + self.request_duration_microseconds.cardinality()
            + self.comparable_request_duration_microseconds.cardinality()
            + self.redis_command_duration_microseconds.cardinality()
            + self.endpoint_duration_microseconds.cardinality()
            + self.redis_command_endpoint_duration_microseconds.cardinality()
            + self.overhead_duration_microseconds.cardinality()
            + self.redis_command_overhead_duration_microseconds.cardinality()
            + self.network_latency_microseconds.cardinality()
            + self.bridge_request_queue_microseconds.cardinality()
            + self.bridge_response_queue_microseconds.cardinality()
            + self.bridge_client_write_microseconds.cardinality()
            + self.parse_duration_microseconds.cardinality()
            + self.parse_decode_duration_microseconds.cardinality()
            + self.parse_gate_duration_microseconds.cardinality()
            + self.parse_materialize_duration_microseconds.cardinality()
            + self.parse_copy_duration_microseconds.cardinality()
            + self.parse_bookkeeping_duration_microseconds.cardinality()
            + self.policy_routing_duration_microseconds.cardinality()
            + self.backend_pool_wait_microseconds.cardinality()
            + self.analytics_record_duration_microseconds.cardinality()
            + self.response_encode_duration_microseconds.cardinality()
            + self.migration_wait_duration_microseconds.cardinality()
            + self.direct_migration_route_duration_microseconds.cardinality()
            + self.lane_pool_init_duration_microseconds.cardinality()
            + self.lane_pool_init_wait_microseconds.cardinality()
            + self.mirror_latency_microseconds.cardinality()
            + self.end_to_end_microseconds.cardinality()
            + self.redis_command_end_to_end_microseconds.cardinality()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_organization_uuid() -> OrganizationUuid {
        OrganizationUuid::new(Uuid::parse_str("91903041-3c42-44b0-a1fe-516cf7feb344").expect("valid organization uuid"))
    }

    fn test_interlay_uuid() -> InterlayUuid {
        InterlayUuid::new(Uuid::parse_str("dcc53c60-ba0d-4837-8be8-e1fe631ce17b").expect("valid interlay uuid"))
    }

    fn test_endpoint_uuid() -> EndpointUuid {
        EndpointUuid::new(Uuid::parse_str("507b72cf-740b-4f0f-8733-9249da081965").expect("valid endpoint uuid"))
    }

    #[test]
    fn dogstatsd_delta_export_includes_org_scoped_proxy_metrics() {
        let metrics = ProxyMetrics::new();
        let labels = [
            ("org_uuid", "91903041-3c42-44b0-a1fe-516cf7feb344"),
            ("interlay_uuid", "dcc53c60-ba0d-4837-8be8-e1fe631ce17b"),
            ("endpoint_uuid", "507b72cf-740b-4f0f-8733-9249da081965"),
            ("endpoint_kind", "redis"),
        ];
        let mut state = ProxyMetricsDogStatsDState::new();

        let active = metrics.active_connections_series(&labels);
        active.inc();
        let org_uuid = test_organization_uuid();
        let interlay_uuid = test_interlay_uuid();
        let endpoint_uuid = test_endpoint_uuid();
        let series = metrics.series_for_organization(&org_uuid, &interlay_uuid, &endpoint_uuid, "redis");
        series.record_batch(ProxyBatchRecord {
            duration_us: 97,
            comparable_duration_us: Some(97),
            endpoint_duration_us: Some(67),
            overhead_us: Some(30),
            bytes_read: 128,
            bytes_written: 256,
            command_count: 4,
        });

        let mut output = String::new();
        metrics.export_dogstatsd_delta(&mut output, &[("service", "eden")], &mut state);

        assert!(output.contains("gateway.requests_total:1|c"));
        assert!(output.contains("gateway.redis_commands_total:4|c"));
        assert!(output.contains("gateway.bytes_read_total:128|c"));
        assert!(output.contains("gateway.bytes_written_total:256|c"));
        assert!(output.contains("gateway.active_connections:1|g"));
        assert!(output.contains("gateway.request_duration_microseconds:"));
        assert!(output.contains("|d"));
        assert!(output.contains("org_uuid:91903041-3c42-44b0-a1fe-516cf7feb344"));
        assert!(output.contains("interlay_uuid:dcc53c60-ba0d-4837-8be8-e1fe631ce17b"));
        assert!(output.contains("endpoint_uuid:507b72cf-740b-4f0f-8733-9249da081965"));
        assert!(!output.contains("org_uuid:org:"));
        assert!(!output.contains("interlay_uuid:interlay:"));
        assert!(!output.contains("endpoint_uuid:endpoint:"));
        assert!(output.contains("endpoint_kind:redis"));
    }

    #[cfg(feature = "clickhouse")]
    #[test]
    fn clickhouse_export_includes_org_scoped_gateway_request_metrics() {
        let metrics = ProxyMetrics::new();
        let org_uuid = test_organization_uuid();
        let interlay_uuid = test_interlay_uuid();
        let endpoint_uuid = test_endpoint_uuid();
        let series = metrics.series_for_organization(&org_uuid, &interlay_uuid, &endpoint_uuid, "redis");
        series.record_batch(ProxyBatchRecord {
            duration_us: 97,
            comparable_duration_us: Some(97),
            endpoint_duration_us: Some(67),
            overhead_us: Some(30),
            bytes_read: 128,
            bytes_written: 256,
            command_count: 4,
        });
        let org_label = org_uuid.uuid().to_string();
        let interlay_label = interlay_uuid.uuid().to_string();
        let endpoint_label = endpoint_uuid.uuid().to_string();
        let bridge_series = metrics.bridge_series(&[
            ("org_uuid", org_label.as_str()),
            ("interlay_uuid", interlay_label.as_str()),
            ("endpoint_uuid", endpoint_label.as_str()),
            ("endpoint_kind", "redis"),
        ]);
        bridge_series.record_end_to_end(120, 4);

        let mut batches = crate::metrics::ClickHouseMetricGroupBatch::new("eden", "node-1");
        metrics.export_clickhouse(&mut batches.proxy, 123);
        let sum_names = batches.proxy.sums.iter().map(|row| row.MetricName.as_str()).collect::<Vec<_>>();
        let histogram_names = batches.proxy.exp_histograms.iter().map(|row| row.MetricName.as_str()).collect::<Vec<_>>();
        let expected_org_uuid = org_uuid.uuid().to_string();

        assert!(sum_names.contains(&"gateway_requests_total") || sum_names.contains(&"gateway.requests_total"));
        assert!(sum_names.contains(&"gateway_redis_commands_total") || sum_names.contains(&"gateway.redis_commands_total"));
        assert!(sum_names.contains(&"gateway_bytes_read_total") || sum_names.contains(&"gateway.bytes_read_total"));
        assert!(sum_names.contains(&"gateway_bytes_written_total") || sum_names.contains(&"gateway.bytes_written_total"));
        assert!(
            histogram_names.contains(&"gateway_request_duration_microseconds")
                || histogram_names.contains(&"gateway.request_duration_microseconds")
        );
        assert!(
            histogram_names.contains(&"gateway_redis_command_duration_microseconds")
                || histogram_names.contains(&"gateway.redis_command_duration_microseconds")
        );
        assert!(
            histogram_names.contains(&"gateway_redis_command_endpoint_duration_microseconds")
                || histogram_names.contains(&"gateway.redis_command_endpoint_duration_microseconds")
        );
        assert!(
            histogram_names.contains(&"gateway_redis_command_overhead_duration_microseconds")
                || histogram_names.contains(&"gateway.redis_command_overhead_duration_microseconds")
        );
        assert!(
            histogram_names.contains(&"gateway_redis_command_end_to_end_microseconds")
                || histogram_names.contains(&"gateway.redis_command_end_to_end_microseconds")
        );
        assert!(batches.proxy.sums.iter().any(|row| row.Attributes.get("org_uuid").is_some_and(|value| value == &expected_org_uuid)));
    }

    #[test]
    fn proxy_batch_records_command_equivalent_timing_for_pipelined_commands() {
        let metrics = ProxyMetrics::new();
        let org_uuid = test_organization_uuid();
        let interlay_uuid = test_interlay_uuid();
        let endpoint_uuid = test_endpoint_uuid();
        let labels = [
            ("org_uuid", org_uuid.uuid().to_string()),
            ("interlay_uuid", interlay_uuid.uuid().to_string()),
            ("endpoint_uuid", endpoint_uuid.uuid().to_string()),
            ("endpoint_kind", "redis".to_string()),
            (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL.to_string()),
        ];
        let borrowed_labels = labels.iter().map(|(key, value)| (*key, value.as_str())).collect::<Vec<_>>();
        let series = metrics.series_for_organization(&org_uuid, &interlay_uuid, &endpoint_uuid, "redis");

        series.record_batch(ProxyBatchRecord {
            duration_us: 101,
            comparable_duration_us: Some(101),
            endpoint_duration_us: Some(76),
            overhead_us: Some(25),
            bytes_read: 2048,
            bytes_written: 4096,
            command_count: 10,
        });

        assert_eq!(metrics.request_duration_microseconds.count(&borrowed_labels), 1);
        assert_eq!(metrics.request_duration_microseconds.sum(&borrowed_labels), 101);
        assert_eq!(metrics.redis_command_duration_microseconds.count(&borrowed_labels), 1);
        assert_eq!(metrics.redis_command_duration_microseconds.sum(&borrowed_labels), 11);
        assert_eq!(metrics.redis_command_endpoint_duration_microseconds.sum(&borrowed_labels), 8);
        assert_eq!(metrics.redis_command_overhead_duration_microseconds.sum(&borrowed_labels), 3);
        assert_eq!(metrics.redis_commands_total.get(&borrowed_labels), 10);
    }

    #[test]
    fn dogstatsd_weighted_distribution_export_keeps_datadog_sample_rate() {
        let metrics = ProxyMetrics::new();
        let mut state = ProxyMetricsDogStatsDState::new();
        let org_uuid = test_organization_uuid();
        let interlay_uuid = test_interlay_uuid();
        let endpoint_uuid = test_endpoint_uuid();
        let series = metrics.series_for_organization(&org_uuid, &interlay_uuid, &endpoint_uuid, "redis");

        for _ in 0..16 {
            series.record_batch(ProxyBatchRecord {
                duration_us: 97,
                comparable_duration_us: Some(97),
                endpoint_duration_us: Some(67),
                overhead_us: Some(30),
                bytes_read: 128,
                bytes_written: 256,
                command_count: 4,
            });
        }

        let mut output = String::new();
        metrics.export_dogstatsd_delta(&mut output, &[("service", "eden")], &mut state);

        assert!(output.contains("gateway.request_duration_microseconds:"));
        assert!(output.contains("|d|@"));
        assert!(output.contains("service:eden"));
    }

    #[test]
    fn bridge_series_records_hot_path_metrics_with_labels() {
        let metrics = ProxyMetrics::new();
        let labels = [
            ("org_uuid", "91903041-3c42-44b0-a1fe-516cf7feb344"),
            ("interlay_uuid", "dcc53c60-ba0d-4837-8be8-e1fe631ce17b"),
            ("endpoint_uuid", "507b72cf-740b-4f0f-8733-9249da081965"),
            ("endpoint_kind", "redis"),
        ];
        let stored_labels = external_traffic_labels(&labels);
        let mut state = ProxyMetricsDogStatsDState::new();

        let series = metrics.bridge_series(&labels);
        series.record_request_chunk();
        series.record_request_queue(13);
        series.record_response_chunk();
        series.record_response_queue(31);
        series.record_client_write(7);
        series.record_bridge_parse(11);
        series.record_end_to_end(73, 4);

        assert_eq!(metrics.bridge_request_chunks_total.get(&stored_labels), 1);
        assert_eq!(metrics.bridge_request_queue_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.bridge_response_chunks_total.get(&stored_labels), 1);
        assert_eq!(metrics.bridge_response_queue_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.bridge_client_write_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.bridge_parse_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.end_to_end_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.end_to_end_microseconds.sum(&stored_labels), 73);
        assert_eq!(metrics.redis_command_end_to_end_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.redis_command_end_to_end_microseconds.sum(&stored_labels), 19);

        let mut output = String::new();
        metrics.export_dogstatsd_delta(&mut output, &[("service", "eden")], &mut state);

        assert!(output.contains("gateway.bridge_request_chunks_total:1|c"));
        assert!(output.contains("gateway.bridge_request_queue_microseconds:"));
        assert!(output.contains("gateway.bridge_response_chunks_total:1|c"));
        assert!(output.contains("gateway.bridge_response_queue_microseconds:"));
        assert!(output.contains("gateway.bridge_client_write_microseconds:"));
        assert!(output.contains("gateway.bridge_parse_microseconds:"));
        assert!(output.contains("gateway.end_to_end_microseconds:"));
        assert!(output.contains("gateway.redis_command_end_to_end_microseconds:"));
        assert!(output.contains("org_uuid:91903041-3c42-44b0-a1fe-516cf7feb344"));
        assert!(output.contains("interlay_uuid:dcc53c60-ba0d-4837-8be8-e1fe631ce17b"));
        assert!(output.contains("endpoint_uuid:507b72cf-740b-4f0f-8733-9249da081965"));
        assert!(output.contains("endpoint_kind:redis"));
    }

    #[test]
    fn multiplex_series_records_hot_path_metrics_with_labels() {
        let metrics = ProxyMetrics::new();
        let labels = [
            ("org_uuid", "91903041-3c42-44b0-a1fe-516cf7feb344"),
            ("endpoint_uuid", "507b72cf-740b-4f0f-8733-9249da081965"),
            ("endpoint_kind", "redis"),
        ];
        let stored_labels = external_traffic_labels(&labels);
        let mut state = ProxyMetricsDogStatsDState::new();

        let series = metrics.multiplex_series(&labels);
        series.record_bus_send(5);
        series.record_worker_pickup(7);
        series.record_write(11);
        series.record_pipeline_wait(13);
        series.record_total(17);
        series.record_oneshot_delivery(19);
        series.record_dispatch_command_count(3);
        series.record_dispatch_failure();

        assert_eq!(metrics.multiplex_bus_send_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_worker_pickup_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_write_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_pipeline_wait_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_total_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_oneshot_delivery_microseconds.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_dispatch_command_count.count(&stored_labels), 1);
        assert_eq!(metrics.multiplex_dispatch_failures_total.get(&stored_labels), 1);

        let mut output = String::new();
        metrics.export_dogstatsd_delta(&mut output, &[("service", "eden")], &mut state);

        assert!(output.contains("gateway.multiplex_bus_send_microseconds:"));
        assert!(output.contains("gateway.multiplex_worker_pickup_microseconds:"));
        assert!(output.contains("gateway.multiplex_write_microseconds:"));
        assert!(output.contains("gateway.multiplex_pipeline_wait_microseconds:"));
        assert!(output.contains("gateway.multiplex_total_microseconds:"));
        assert!(output.contains("gateway.multiplex_oneshot_delivery_microseconds:"));
        assert!(output.contains("gateway.multiplex_dispatch_command_count:"));
        assert!(output.contains("gateway.multiplex_dispatch_failures_total:1|c"));
        assert!(output.contains("org_uuid:91903041-3c42-44b0-a1fe-516cf7feb344"));
        assert!(output.contains("endpoint_uuid:507b72cf-740b-4f0f-8733-9249da081965"));
        assert!(output.contains("endpoint_kind:redis"));
    }

    #[test]
    fn lane_pool_series_records_direct_pool_metrics_with_labels() {
        let metrics = ProxyMetrics::new();
        let labels = [
            ("org_uuid", "91903041-3c42-44b0-a1fe-516cf7feb344"),
            ("shard_id", "3"),
            ("interlay_uuid", "dcc53c60-ba0d-4837-8be8-e1fe631ce17b"),
        ];
        let stored_labels = external_traffic_labels(&labels);
        let mut state = ProxyMetricsDogStatsDState::new();

        let series = metrics.lane_pool_series(&labels);
        series.set_lanes_open(4);
        series.set_waiters(2);
        series.record_acquire_wait(19);

        assert_eq!(metrics.lane_pool_lanes_open.get(&stored_labels), 4);
        assert_eq!(metrics.lane_pool_waiters.get(&stored_labels), 2);
        assert_eq!(metrics.lane_pool_acquire_wait_microseconds.count(&stored_labels), 1);

        let mut output = String::new();
        metrics.export_dogstatsd_delta(&mut output, &[("service", "eden")], &mut state);

        assert!(output.contains("gateway.lane_pool_lanes_open:4|g"));
        assert!(output.contains("gateway.lane_pool_waiters:2|g"));
        assert!(output.contains("gateway.lane_pool_acquire_wait_microseconds:"));
        assert!(output.contains("org_uuid:91903041-3c42-44b0-a1fe-516cf7feb344"));
        assert!(output.contains("shard_id:3"));
        assert!(output.contains("interlay_uuid:dcc53c60-ba0d-4837-8be8-e1fe631ce17b"));
    }

    #[test]
    fn active_direct_connection_series_handle_prevents_stale_eviction() {
        let metrics = ProxyMetrics::new();
        let labels = [
            ("interlay_uuid", "dcc53c60-ba0d-4837-8be8-e1fe631ce17b"),
            ("endpoint_uuid", "507b72cf-740b-4f0f-8733-9249da081965"),
            ("endpoint_kind", "redis"),
        ];
        let stored_labels = external_traffic_labels(&labels);

        let active = metrics.direct_safe_connections_active_series(&labels);
        active.inc();

        fast_telemetry::advance_cycle();
        assert_eq!(metrics.evict_stale(0), 0);
        assert_eq!(metrics.direct_safe_connections_active.get(&stored_labels), 1);

        active.dec();
        drop(active);

        fast_telemetry::advance_cycle();
        assert_eq!(metrics.evict_stale(0), 1);
        assert_eq!(metrics.direct_safe_connections_active.get(&stored_labels), 0);
    }

    #[test]
    fn active_proxy_connection_series_handle_prevents_stale_eviction() {
        let metrics = ProxyMetrics::new();
        let labels = [("interlay_uuid", "dcc53c60-ba0d-4837-8be8-e1fe631ce17b")];
        let stored_labels = external_traffic_labels(&labels);

        let active = metrics.active_connections_series(&labels);
        active.inc();

        fast_telemetry::advance_cycle();
        assert_eq!(metrics.evict_stale(0), 0);
        assert_eq!(metrics.active_connections.get(&stored_labels), 1);

        active.dec();
        drop(active);

        fast_telemetry::advance_cycle();
        assert_eq!(metrics.evict_stale(0), 1);
        assert_eq!(metrics.active_connections.get(&stored_labels), 0);
    }
}

impl Default for ProxyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A proxy request batch payload for hot path metric recording.
#[derive(Clone, Copy, Debug)]
pub struct ProxyBatchRecord {
    pub duration_us: u64,
    pub comparable_duration_us: Option<u64>,
    pub endpoint_duration_us: Option<u64>,
    pub overhead_us: Option<u64>,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub command_count: u64,
}

/// Cached i64 gauge series handle for connection-scoped gauges.
///
/// Holding this handle keeps the dynamic series protected from stale-series
/// eviction while the owning connection is still alive.
pub struct ProxyGaugeSeries {
    series: DynamicGaugeI64Series,
}

impl ProxyGaugeSeries {
    #[inline]
    pub fn inc(&self) {
        self.series.inc();
    }

    #[inline]
    pub fn dec(&self) {
        self.series.dec();
    }
}

/// Cached series handles for hot path proxy metrics.
///
/// Create once per connection with `ProxyMetrics::series_for_organization()` and use
/// the zero-allocation recording methods in the request loop.
///
/// ## Example
/// ```ignore
/// // At connection setup:
/// let series = metrics.proxy().series_for_organization(&org_uuid, &interlay_uuid, &endpoint_uuid, "redis");
///
/// // In hot path (per-request):
/// series.record_batch(ProxyBatchRecord {
///     duration_us,
///     comparable_duration_us,
///     endpoint_duration_us,
///     overhead_us,
///     bytes_read,
///     bytes_written,
///     command_count,
/// });
/// ```
///
/// Connection lifecycle is intentionally not part of this hot-path request
/// series. `gateway.active_connections` is owned by the interlay listener's
/// per-client guard and should be summed across interlay label sets.
pub struct ProxySeries {
    requests: DynamicCounterSeries,
    commands: DynamicCounterSeries,
    duration: DynamicDistributionSeries,
    comparable_duration: DynamicDistributionSeries,
    command_duration: DynamicDistributionSeries,
    endpoint_duration: DynamicDistributionSeries,
    command_endpoint_duration: DynamicDistributionSeries,
    overhead_duration: DynamicDistributionSeries,
    command_overhead_duration: DynamicDistributionSeries,
    bytes_read: DynamicCounterSeries,
    bytes_written: DynamicCounterSeries,
    errors: DynamicCounterSeries,
    bridge_request_queue: DynamicDistributionSeries,
    policy_routing_duration: DynamicDistributionSeries,
    response_encode_duration: DynamicDistributionSeries,
}

/// Cached series handles for mirror hot path metrics.
pub struct ProxyMirrorSeries {
    requests: DynamicCounterSeries,
    latency: DynamicDistributionSeries,
    upstream_errors: DynamicCounterSeries,
}

/// Cached series handles for Redis multiplexer hot path metrics.
#[derive(Clone)]
pub struct ProxyMultiplexSeries {
    bus_send: DynamicDistributionSeries,
    worker_pickup: DynamicDistributionSeries,
    write: DynamicDistributionSeries,
    pipeline_wait: DynamicDistributionSeries,
    total: DynamicDistributionSeries,
    oneshot_delivery: DynamicDistributionSeries,
    dispatch_command_count: DynamicDistributionSeries,
    dispatch_failures: DynamicCounterSeries,
}

/// Cached series handles for bridge hot path metrics.
pub struct ProxyBridgeSeries {
    request_chunks: DynamicCounterSeries,
    request_queue: DynamicDistributionSeries,
    response_chunks: DynamicCounterSeries,
    response_queue: DynamicDistributionSeries,
    client_write: DynamicDistributionSeries,
    bridge_parse: DynamicDistributionSeries,
    end_to_end: DynamicDistributionSeries,
    command_end_to_end: DynamicDistributionSeries,
}

/// Cached series handles for direct Redis lane-pool metrics.
pub struct ProxyLanePoolSeries {
    lanes_open: DynamicGaugeI64Series,
    acquire_wait: DynamicDistributionSeries,
    waiters: DynamicGaugeI64Series,
}

impl ProxyMirrorSeries {
    #[inline]
    pub fn record_request(&self) {
        self.requests.inc();
    }

    #[inline]
    pub fn record_latency(&self, duration_us: u64) {
        self.latency.record(duration_us);
    }

    #[inline]
    pub fn record_upstream_error(&self) {
        self.upstream_errors.inc();
    }
}

impl ProxyMultiplexSeries {
    #[inline]
    pub fn record_bus_send(&self, duration_us: u64) {
        self.bus_send.record(duration_us);
    }

    #[inline]
    pub fn record_worker_pickup(&self, duration_us: u64) {
        self.worker_pickup.record(duration_us);
    }

    #[inline]
    pub fn record_write(&self, duration_us: u64) {
        self.write.record(duration_us);
    }

    #[inline]
    pub fn record_pipeline_wait(&self, duration_us: u64) {
        self.pipeline_wait.record(duration_us);
    }

    #[inline]
    pub fn record_total(&self, duration_us: u64) {
        self.total.record(duration_us);
    }

    #[inline]
    pub fn record_oneshot_delivery(&self, duration_us: u64) {
        self.oneshot_delivery.record(duration_us);
    }

    #[inline]
    pub fn record_dispatch_command_count(&self, count: u64) {
        self.dispatch_command_count.record(count);
    }

    #[inline]
    pub fn record_dispatch_failure(&self) {
        self.dispatch_failures.inc();
    }
}

impl ProxyBridgeSeries {
    #[inline]
    pub fn record_request_chunk(&self) {
        self.request_chunks.inc();
    }

    #[inline]
    pub fn record_request_queue(&self, duration_us: u64) {
        self.request_queue.record(duration_us);
    }

    #[inline]
    pub fn record_response_chunk(&self) {
        self.response_chunks.inc();
    }

    #[inline]
    pub fn record_response_queue(&self, duration_us: u64) {
        self.response_queue.record(duration_us);
    }

    #[inline]
    pub fn record_client_write(&self, duration_us: u64) {
        self.client_write.record(duration_us);
    }

    #[inline]
    pub fn record_bridge_parse(&self, duration_us: u64) {
        self.bridge_parse.record(duration_us);
    }

    #[inline]
    pub fn record_end_to_end(&self, duration_us: u64, command_count: u64) {
        self.end_to_end.record(duration_us);
        if let Some(command_duration_us) = command_equivalent_duration_us(duration_us, command_count) {
            self.command_end_to_end.record(command_duration_us);
        }
    }
}

impl ProxyLanePoolSeries {
    #[inline]
    pub fn set_lanes_open(&self, count: i64) {
        self.lanes_open.set(count);
    }

    #[inline]
    pub fn record_acquire_wait(&self, duration_us: u64) {
        self.acquire_wait.record(duration_us);
    }

    #[inline]
    pub fn set_waiters(&self, count: i64) {
        self.waiters.set(count);
    }
}

#[inline]
fn command_equivalent_duration_us(duration_us: u64, command_count: u64) -> Option<u64> {
    if command_count == 0 {
        return None;
    }
    Some(duration_us.div_ceil(command_count))
}

impl ProxySeries {
    /// Record a complete proxy request batch (the hot path).
    ///
    /// This is the primary recording method for the wire protocol processor.
    /// All operations are zero-allocation after series creation.
    #[inline]
    pub fn record_batch(&self, batch: ProxyBatchRecord) {
        self.requests.inc();
        self.commands.add(batch.command_count as isize);
        self.duration.record(batch.duration_us);
        if let Some(comparable_duration_us) = batch.comparable_duration_us {
            self.comparable_duration.record(comparable_duration_us);
        }
        if let Some(command_duration_us) = command_equivalent_duration_us(batch.duration_us, batch.command_count) {
            self.command_duration.record(command_duration_us);
        }
        if let Some(endpoint_duration_us) = batch.endpoint_duration_us {
            self.endpoint_duration.record(endpoint_duration_us);
            if let Some(command_endpoint_duration_us) = command_equivalent_duration_us(endpoint_duration_us, batch.command_count) {
                self.command_endpoint_duration.record(command_endpoint_duration_us);
            }
        }
        if let Some(overhead_us) = batch.overhead_us {
            self.overhead_duration.record(overhead_us);
            if let Some(command_overhead_us) = command_equivalent_duration_us(overhead_us, batch.command_count) {
                self.command_overhead_duration.record(command_overhead_us);
            }
        }
        self.bytes_read.add(batch.bytes_read as isize);
        self.bytes_written.add(batch.bytes_written as isize);
    }

    /// Record a streamed/raw proxy request batch when precise latency
    /// segmentation is not available.
    ///
    /// Used by the direct Redis pinned fallback, which forwards bytes
    /// as a raw stream and therefore cannot safely pair each client read
    /// with an exact backend response timing segment.
    #[inline]
    pub fn record_streamed_request_batch(&self, bytes_read: u64, command_count: u64) {
        self.requests.inc();
        if command_count > 0 {
            self.commands.add(command_count as isize);
        }
        self.bytes_read.add(bytes_read as isize);
    }

    /// Record streamed/raw proxy response bytes when precise latency
    /// segmentation is not available.
    #[inline]
    pub fn record_streamed_response_bytes(&self, bytes_written: u64) {
        self.bytes_written.add(bytes_written as isize);
    }

    /// Record a proxy error.
    #[inline]
    pub fn record_error(&self) {
        self.errors.inc();
    }

    #[inline]
    pub fn record_bridge_request_queue(&self, duration_us: u64) {
        self.bridge_request_queue.record(duration_us);
    }

    #[inline]
    pub fn record_policy_routing_duration(&self, duration_us: u64) {
        self.policy_routing_duration.record(duration_us);
    }

    #[inline]
    pub fn record_response_encode_duration(&self, duration_us: u64) {
        self.response_encode_duration.record(duration_us);
    }
}

impl ProxyMetrics {
    /// Resolve the top-level active proxy-connection gauge series.
    ///
    /// Connection lifecycle guards should hold this handle for their lifetime so
    /// the stale-series sweeper cannot evict the active gauge while a
    /// long-lived proxy client session is still open.
    pub fn active_connections_series(&self, labels: &[(&str, &str)]) -> ProxyGaugeSeries {
        let labels = external_traffic_labels(labels);
        ProxyGaugeSeries { series: self.active_connections.series(&labels) }
    }

    /// Resolve the active safe direct-connection gauge series.
    ///
    /// Connection guards should hold this handle for their lifetime so the
    /// stale-series sweeper cannot evict the active gauge while a long-lived
    /// direct connection is still open.
    pub fn direct_safe_connections_active_series(&self, labels: &[(&str, &str)]) -> ProxyGaugeSeries {
        let labels = external_traffic_labels(labels);
        ProxyGaugeSeries { series: self.direct_safe_connections_active.series(&labels) }
    }

    /// Resolve the active unsafe direct-connection gauge series.
    ///
    /// Connection guards should hold this handle for their lifetime so the
    /// stale-series sweeper cannot evict the active gauge while a long-lived
    /// pinned direct connection is still open.
    pub fn direct_unsafe_connections_active_series(&self, labels: &[(&str, &str)]) -> ProxyGaugeSeries {
        let labels = external_traffic_labels(labels);
        ProxyGaugeSeries {
            series: self.direct_unsafe_connections_active.series(&labels),
        }
    }

    /// Create cached hot-path request series handles from an already-normalized
    /// label set.
    ///
    /// This is used by gateway bridges that already resolved org/interlay/
    /// endpoint labels from their accepted connection state. The caller must
    /// include `org_uuid`; the ClickHouse exporter drops unscoped metric rows.
    pub fn series(&self, labels: &[(&str, &str)]) -> ProxySeries {
        let labels = external_traffic_labels(labels);
        ProxySeries {
            requests: self.requests_total.series(&labels),
            commands: self.redis_commands_total.series(&labels),
            duration: self.request_duration_microseconds.series(&labels),
            comparable_duration: self.comparable_request_duration_microseconds.series(&labels),
            command_duration: self.redis_command_duration_microseconds.series(&labels),
            endpoint_duration: self.endpoint_duration_microseconds.series(&labels),
            command_endpoint_duration: self.redis_command_endpoint_duration_microseconds.series(&labels),
            overhead_duration: self.overhead_duration_microseconds.series(&labels),
            command_overhead_duration: self.redis_command_overhead_duration_microseconds.series(&labels),
            bytes_read: self.bytes_read_total.series(&labels),
            bytes_written: self.bytes_written_total.series(&labels),
            errors: self.errors_total.series(&labels),
            bridge_request_queue: self.bridge_request_queue_microseconds.series(&labels),
            policy_routing_duration: self.policy_routing_duration_microseconds.series(&labels),
            response_encode_duration: self.response_encode_duration_microseconds.series(&labels),
        }
    }

    /// Create cached series handles for a specific organization/interlay/endpoint/kind.
    pub fn series_for_organization(
        &self,
        organization_uuid: &OrganizationUuid,
        interlay_uuid: &InterlayUuid,
        endpoint_uuid: &EndpointUuid,
        endpoint_kind: &str,
    ) -> ProxySeries {
        let organization_uuid = organization_uuid.uuid().to_string();
        let interlay_uuid = interlay_uuid.uuid().to_string();
        let endpoint_uuid = endpoint_uuid.uuid().to_string();
        let labels: &[(&str, &str)] = &[
            ("org_uuid", organization_uuid.as_str()),
            ("interlay_uuid", interlay_uuid.as_str()),
            ("endpoint_uuid", endpoint_uuid.as_str()),
            ("endpoint_kind", endpoint_kind),
            (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL),
        ];
        self.series(labels)
    }
}
