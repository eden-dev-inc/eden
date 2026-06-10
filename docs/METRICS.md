# Eden Metrics Documentation

This document describes Eden service metrics. Most are OpenTelemetry metrics exported through OTLP or written to the configured telemetry store.

The Eden dashboard metric picker is backed by `/api/v1/analytics/series`, which currently serves ClickHouse-backed fast-telemetry metrics and selected analytics rollup tables. Prometheus-only metrics, such as load balancer and request client collectors, remain documented here but are intentionally not exposed in that picker until an analytics-series ingestion path exists for them.

## Table of Contents

- [Eden Core Metrics](#eden-core-metrics)
- [LLM Metrics](#llm-metrics)
- [IAM Metrics](#iam-metrics)
- [Endpoint Metrics](#endpoint-metrics)
- [Gateway Metrics](#gateway-metrics)
- [Wire vs Polling Metrics](#wire-vs-polling-metrics)
- [Workload Metrics (AMR Profiling)](#workload-metrics-amr-profiling)
- [Load Balancer Metrics](#load-balancer-metrics)
- [ClickHouse Analytics Metrics](#clickhouse-analytics-metrics)

---

## Eden Core Metrics

Prefix: `eden.`

Core service metrics for request handling, caching, and connections.

### Definition

**Source:** [eden_core/telemetry/src/metrics/eden.rs](../eden_core/telemetry/src/metrics/eden.rs)

### Collection Points

| Module                                | Function                              | Metrics Collected                                                                                                           |
| ------------------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `eden_core::telemetry::metrics`       | `MetricsMiddleware::call()`           | All request metrics (active_requests, request_sent, total_duration, etc.)                                                   |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::start_request()`        | active_requests, request_sent, upload_byte_count, upload_byte_distribution, unique_users                                    |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::complete_request()`     | active_requests, response_count, eden_duration, success_count, error_count, download_byte_count, download_byte_distribution |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::add_login()`            | logins                                                                                                                      |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::add_local_cache_hit()`  | local_cache_hits                                                                                                            |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::add_local_cache_miss()` | local_cache_misses                                                                                                          |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::add_redis_cache_hit()`  | redis_cache_hits (legacy API; no internal cache writer)                                                                      |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::add_redis_cache_miss()` | redis_cache_misses (legacy API; no internal cache writer)                                                                    |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::add_connection()`       | connections                                                                                                                 |
| `eden_core::telemetry::connection_tracker` | `spawn_pool_status_poller()`    | connections_in_use                                                                                                          |
| Legacy RBAC sync metric               | Removed Redis stream consumer         | rbac_pg_sync_lag (retained for compatibility; no current writer)                                                            |
| `database::db::cache::uuid`           | `get_from_cache()`                    | Cache hit/miss via MetricEvent                                                                                              |
| `database::db::cache::id`             | `get_from_cache()`                    | Cache hit/miss via MetricEvent                                                                                              |
| `eden_service::comm::auth::login`     | `login()`, `token_refresh()`          | Login via MetricEvent::LoginWith                                                                                            |

### Metrics

| Metric                            | Type          | Unit  | Description                                                                     |
| --------------------------------- | ------------- | ----- | ------------------------------------------------------------------------------- |
| `eden.active_requests`            | UpDownCounter | -     | Number of requests currently being processed                                    |
| `eden.request_sent`               | Counter       | -     | Total number of requests received                                               |
| `eden.response_count`             | Counter       | -     | Total number of responses sent                                                  |
| `eden.total_duration`             | Histogram     | μs    | Distribution of total request processing durations                              |
| `eden.eden_duration`              | Histogram     | μs    | Distribution of Eden-specific processing overhead (excludes downstream latency) |
| `eden.success_count`              | Counter       | -     | Total number of successful requests                                             |
| `eden.error_count`                | Counter       | -     | Total number of failed requests                                                 |
| `eden.upload_byte_count`          | Counter       | bytes | Total bytes uploaded (request body size)                                        |
| `eden.upload_byte_distribution`   | Histogram     | bytes | Distribution of upload sizes                                                    |
| `eden.download_byte_count`        | Counter       | bytes | Total bytes downloaded (response body size)                                     |
| `eden.download_byte_distribution` | Histogram     | bytes | Distribution of download sizes                                                  |
| `eden.unique_users`               | Counter       | -     | Total number of unique users seen                                               |
| `eden.logins`                     | Counter       | -     | Total number of login events                                                    |
| `eden.local_cache_hits`           | Counter       | -     | Number of local (in-memory) cache hits                                          |
| `eden.local_cache_misses`         | Counter       | -     | Number of local cache misses                                                    |
| `eden.redis_cache_hits`           | Counter       | -     | Legacy Redis cache hit metric; no internal cache writer                         |
| `eden.redis_cache_misses`         | Counter       | -     | Legacy Redis cache miss metric; no internal cache writer                        |
| `eden.connections`                | UpDownCounter | -     | Number of active async connections                                              |
| `eden.connections_in_use`         | UpDownCounter | -     | Sampled endpoint pool connections currently checked out; owned poller handles subtract the last positive sample when a pool is dropped or polling stops |
| `eden.rbac_pg_sync_lag`           | Gauge         | events| Legacy RBAC Redis stream lag metric; retained for compatibility, no current writer |

### Labels

- `user_id`: User identifier (for login/user-specific metrics)
- `org_id`: Organization identifier
- `cache_type`: Type of cache (local, template; legacy values may include redis)
- `db_type`: Database type (for connection metrics)
- `stream`: Legacy Redis PG sync stream name
- `group`: Legacy Redis PG sync stream consumer group
- `traffic_class`: Low-cardinality request origin. `internal` marks Eden dashboard and observability API reads; `external` marks user/API and gateway traffic. Observability pages default to external traffic so dashboard page views do not skew endpoint and request-rate panels. Rows must carry this label to match the `external` or `internal` filters; use `all` to include unlabeled historical rows.

### Key Dashboards

- **Request Rate**: `rate(eden.request_sent)`
- **Error Rate**: `eden.error_count / eden.request_sent * 100`
- **Cache Hit Rate**: `eden.local_cache_hits / (eden.local_cache_hits + eden.local_cache_misses) * 100`
- **P99 Latency**: `histogram_quantile(0.99, eden.total_duration)`

---

## LLM Metrics

Prefix: `eden.llm.`

Metrics for LLM (Large Language Model) endpoint usage and tool invocations.

### Definition

| Source File                                                                           | Metrics                        |
| ------------------------------------------------------------------------------------- | ------------------------------ |
| [eden_core/telemetry/src/metrics/eden.rs](../eden_core/telemetry/src/metrics/eden.rs) | Core LLM token/request metrics and LLM gateway telemetry |
| [endpoint-core/llm-core/src/tools.rs](../endpoint-core/llm-core/src/tools.rs)           | tool metrics                   |

### Collection Points

| Module                                | Function                          | Metrics Collected                                                                                       |
| ------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `eden_core::telemetry::metrics::eden` | `EdenMetrics::record_llm_usage()` | llm.requests, llm.prompt_tokens, llm.completion_tokens, llm.total_tokens, llm.total_tokens_distribution |
| `eden_gateway::llm`                   | `LlmGatewayRequestTelemetry::finish()` | llm.gateway.requests, llm.gateway.errors, llm.gateway.request_duration_microseconds, llm.gateway.time_to_first_chunk_microseconds, llm.gateway.time_per_output_chunk_microseconds, llm.gateway.stream_chunks, token usage/detail metrics, and in-process route latency/throughput/error observations |
| `eden_gateway::llm`                   | `LlmGatewayRequestTelemetry::set_feature_decision()`, `set_route_decision()`, and `set_response_inspection()` | Promptless span attributes for control-plane source, model policy, budget policy, PII policy, prompt security risk, tool policy, route class, selected route, price source, estimated route cost/savings, route stat sample count, selected route latency/throughput/error rollups, response cache eligibility, eval mode, streaming inspection, and request/response PII counts |
| `endpoint_core::llm_core::gateway`    | `record_llm_gateway_route_observation()` | Maintains rolling in-process route observations used by latency, throughput, and balanced gateway route selection |
| `endpoint_core::llm_core::analytics`  | `record_llm_operation()`          | Calls record_llm_usage() and enqueues durable LLM analytics events                                      |
| `endpoint_core::llm_core::pricing`    | `refresh_openrouter_pricing()`    | Refreshes live model price cache and enqueues durable LLM price snapshots                               |
| `eden_service::comm::llm::proxy`       | `chat_completions()`              | Records proxy response-cache status, durable exact response-cache hits/stores, KV route-cache status, route objective, route moves, arbitrage savings, cache savings, durable monthly usage rollups, and durable route latency/throughput observations |
| `eden_service::comm::llm::gateway_dashboard` | `get_dashboard()`                 | Returns promptless AI gateway dashboard summaries for durable usage rollups, durable route rollups, cache counts, budget windows, cost analysis, daily spend buckets, cost alerts, provider reconciliation status, and agent gateway fingerprints |
| `eden_service::comm::llm::chat`       | `record_llm_usage_metrics()`      | Builds `LlmOperationEvent` records for dashboard chat success paths                                     |
| `endpoint_core::llm_core::tools`      | `ToolMetrics::record_success()`   | tool.attempts, tool.latency                                                                             |
| `endpoint_core::llm_core::tools`      | `ToolMetrics::record_failure()`   | tool.attempts, tool.failures, tool.latency                                                              |

### Metrics

| Metric                               | Type      | Unit   | Description                                  |
| ------------------------------------ | --------- | ------ | -------------------------------------------- |
| `eden.llm.requests`                  | Counter   | -      | Total number of LLM API requests             |
| `eden.llm.prompt_tokens`             | Counter   | tokens | Total prompt tokens sent to LLMs             |
| `eden.llm.completion_tokens`         | Counter   | tokens | Total completion tokens generated by LLMs    |
| `eden.llm.total_tokens`              | Counter   | tokens | Total tokens processed (prompt + completion) |
| `eden.llm.total_tokens_distribution` | Histogram | tokens | Distribution of total tokens per request     |
| `eden.llm.cached_prompt_tokens`      | Counter   | tokens | Cached prompt tokens reported by providers   |
| `eden.llm.prompt_audio_tokens`       | Counter   | tokens | Prompt audio tokens reported by providers    |
| `eden.llm.reasoning_completion_tokens` | Counter | tokens | Reasoning tokens reported by providers       |
| `eden.llm.completion_audio_tokens`   | Counter   | tokens | Completion audio tokens reported by providers |
| `eden.llm.gateway.requests`          | Counter   | -      | Total LLM gateway HTTP requests              |
| `eden.llm.gateway.errors`            | Counter   | -      | Failed LLM gateway HTTP requests             |
| `eden.llm.gateway.request_duration_microseconds` | Histogram | μs | End-to-end LLM gateway HTTP request duration |
| `eden.llm.gateway.time_to_first_chunk_microseconds` | Histogram | μs | Streaming time to first provider chunk       |
| `eden.llm.gateway.time_per_output_chunk_microseconds` | Histogram | μs | Streaming time between output chunks         |
| `eden.llm.gateway.stream_chunks`      | Counter   | chunks | Streaming chunks emitted by the gateway      |

### Labels

- `llm.provider`: Provider name (e.g., "openai", "anthropic")
- `llm.model`: Model name (e.g., "gpt-4", "claude-3")
- `llm.tool_used`: Whether a tool was used (true/false)
- `llm.streaming`: Whether streaming was enabled (true/false)
- `endpoint_uuid`: Endpoint identifier
- `org_uuid`: Organization identifier
- `route`: LLM gateway route bucket (`chat.completions`, `models`, `health`, `unknown`, `parse_error`)
- `status_code`: HTTP status code returned or observed by the gateway
- `status_class`: HTTP status class (`2xx`, `4xx`, `5xx`)
- `error_type`: Low-cardinality gateway failure reason
- `auth_scheme`: Sanitized client auth scheme (`bearer`, `basic`, `api_key`, `none`, `other`)

### Key Dashboards

- **Token Usage**: `sum(eden.llm.total_tokens) by (llm.provider, llm.model)`
- **Cost Estimation**: Token counts can be used to estimate API costs

---

## IAM Metrics

Prefix: `eden.iam.`

Identity and Access Management metrics for tracking role assignments.

### Definition

**Source:** [eden_core/telemetry/src/metrics/iam.rs](../eden_core/telemetry/src/metrics/iam.rs)

### Collection Points

| Module                                | Function        | Metrics Collected                 |
| ------------------------------------- | --------------- | --------------------------------- |
| `database::db::methods::insert::user` | `insert_user()` | RolesGrantedBatch via MetricEvent |
| `database::db::methods::delete::user` | `delete_user()` | RoleRevoked via MetricEvent       |

### Metrics

| Metric                  | Type    | Unit | Description                 |
| ----------------------- | ------- | ---- | --------------------------- |
| `eden.iam.assignments`  | Counter | -    | IAM role assignment changes |

### Labels

- `org_uuid`: Organization identifier

### Key Dashboards

- **Assignment Changes**: Monitor IAM role assignment activity

---

## Endpoint Metrics

Prefix: `eden.endpoint.`

Metrics for database endpoint operations.

### Definition

**Source:** [eden_core/telemetry/src/metrics/endpoint.rs](../eden_core/telemetry/src/metrics/endpoint.rs)

### Collection Points

Endpoint metrics are collected via `EndpointGuard` (RAII pattern) which automatically records start/finish times:

| Module                                 | Function/Location                    | Metrics Collected                                         |
| -------------------------------------- | ------------------------------------ | --------------------------------------------------------- |
| `eden_core::telemetry::guards`         | `EndpointGuard::new()`               | active_requests (+1), total_requests (+1)                 |
| `eden_core::telemetry::guards`         | `EndpointGuard::drop()`              | active_requests (-1), endpoint_duration                   |
| `eden_core::telemetry::metrics`        | `MetricsMiddleware::call()`          | All endpoint metrics via EndpointGuard                    |
| `endpoint_core::ep_core::macros`       | `impl_operation!` macro              | All endpoint metrics via EndpointGuard (async operations) |
| `eden_service::comm::endpoints::list`  | `list_endpoints()`, `get_endpoint()` | All endpoint metrics via EndpointGuard                    |
| `endpoints::endpoint::redis::api::lib` | `execute_redis_command()`            | All endpoint metrics via EndpointGuard                    |

### Metrics

| Metric                            | Type          | Unit | Description                                  |
| --------------------------------- | ------------- | ---- | -------------------------------------------- |
| `eden.endpoint.active_requests`   | UpDownCounter | -    | Number of active endpoint requests           |
| `eden.endpoint.total_requests`    | Counter       | -    | Total number of endpoint requests            |
| `eden.endpoint.endpoint_duration` | Histogram     | ms   | Distribution of endpoint operation durations |

### Labels

- `endpoint_type`: Type of endpoint (redis, postgres, mongo, etc.)
- `endpoint_uuid`: Endpoint identifier
- `org_uuid`: Organization identifier
- `operation`: Operation type (read, write, connect, disconnect)

---

## Gateway Metrics

Prefix: `gateway.`

Metrics for the interlay gateway layer. Target: <100μs latency for gateway operations.

### Definition

**Source:** [eden_core/telemetry/src/metrics/proxy.rs](../eden_core/telemetry/src/metrics/proxy.rs)

### Collection Points

| Module                                 | Function            | Metrics Collected                                                                         |
| -------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------- |
| `eden_service::comm::interlays::start` | `start_interlay()`  | Gateway connection lifecycle gauge/registry guard; ProxyConnectionFailure via MetricEvent |
| `eden_gateway`                           | `run_proxy_bridge_loop()` | Bridge request chunks, response chunks, enqueue rejections, bridge queue/write timing |
| `gateway_redis::processor`              | `process()`         | ProxySeries gateway request, comparable request, endpoint, overhead, parse, policy/routing, pool wait, analytics, response encode, mirror-mode fan-out telemetry |
| `eden_gateway::postgres::processor`      | `process()`         | ProxySeries gateway request, comparable request, endpoint, overhead, parse, policy/routing, pool wait, analytics, response encode, mirror-mode fan-out telemetry |
| `eden_gateway::mongo::processor`         | `process()`         | ProxySeries gateway request, comparable request, endpoint, overhead, parse, policy/routing, pool wait, analytics, response encode, mirror-mode fan-out telemetry |
| `endpoints::endpoint-types::ep`        | `raw_bytes_*()`     | NetworkLatency via MetricEvent                                                            |

The gateway bridge and Redis processor also emit structured connection-end logs with
stable `reason` fields plus queue/counter snapshots so operators can separate
client disconnects from backend or processor churn under load.

### Metrics

| Metric                                   | Type          | Unit  | Description                                                                 |
| ---------------------------------------- | ------------- | ----- | --------------------------------------------------------------------------- |
| `gateway.requests_total`                   | Counter       | -     | Total gateway request batches processed across multiplexed, direct safe lane-pool, and direct unsafe pinned paths |
| `gateway.redis.commands_total`             | Counter       | -     | Total Redis commands observed inside gateway request batches when command framing is visible |
| `gateway.redis.command_duration_microseconds` | Histogram  | μs    | Redis command-equivalent gateway latency, normalized from batched wire traffic |
| `gateway.redis.command_endpoint_duration_microseconds` | Histogram | μs | Redis command-equivalent backend endpoint time, normalized from batched wire traffic |
| `gateway.redis.command_overhead_duration_microseconds` | Histogram | μs | Redis command-equivalent gateway overhead, normalized from batched wire traffic |
| `gateway.request_duration_microseconds`    | Histogram     | μs    | Gateway request latency for all recorded gateway batches, including backend dispatch time |
| `gateway.comparable_request_duration_microseconds` | Histogram | μs    | Gateway request latency for Redis/Postgres/Mongo batches with comparable endpoint/overhead timing |
| `gateway.endpoint_duration_microseconds`   | Histogram     | μs    | Comparable Redis/Postgres/Mongo endpoint dispatch time recorded from the same gateway batch |
| `gateway.overhead_duration_microseconds`   | Histogram     | μs    | Gateway overhead after subtracting comparable Redis/Postgres/Mongo endpoint dispatch time |
| `gateway.network_latency_microseconds`     | Histogram     | μs    | Backend network I/O latency recorded at the endpoint layer                  |
| `gateway.bridge_request_queue_microseconds` | Histogram    | μs    | Time from client read enqueue until the protocol processor receives the request chunk |
| `gateway.bridge_response_queue_microseconds` | Histogram   | μs    | Time from processor response enqueue until the bridge begins writing to the client |
| `gateway.bridge_client_write_microseconds` | Histogram     | μs    | Time spent writing a response chunk back to the client socket                |
| `gateway.parse_duration_microseconds`    | Histogram     | μs    | Time spent parsing or framing protocol messages before policy/routing       |
| `gateway.parse_gate_duration_microseconds` | Histogram    | μs    | Time spent evaluating parse-path gates before protocol decode begins        |
| `gateway.parse_decode_duration_microseconds` | Histogram  | μs    | Time spent scanning or decoding protocol frame boundaries before command materialization |
| `gateway.parse_materialize_duration_microseconds` | Histogram | μs | Time spent identifying/materializing parsed command structures; Redis parsed-pipeline mode records one coarse batch timer here to avoid per-command timing overhead |
| `gateway.parse_copy_duration_microseconds` | Histogram   | μs    | Time spent assembling zero-copy request byte slices for backend dispatch    |
| `gateway.parse_bookkeeping_duration_microseconds` | Histogram | μs | Residual parse time for loop/control bookkeeping not covered by the other parse buckets |
| `gateway.policy_routing_duration_microseconds` | Histogram | μs    | Per-batch time spent applying policy, resolving routing state, selecting dispatch path, and preparing backend handoff |
| `gateway.backend_pool_wait_microseconds` | Histogram     | μs    | Time spent acquiring pinned/pool-backed backend clients where visible in the gateway |
| `gateway.analytics_record_duration_microseconds` | Histogram | μs    | Per-batch time spent recording analytics/audit samples inside the processor batch window |
| `gateway.response_encode_duration_microseconds` | Histogram | μs    | Per-batch time spent encoding or assembling protocol responses before bridge enqueue |
| `gateway.mirror_requests_total`           | Counter       | -     | Mirror Mode secondary dispatch attempts; labels include `interlay_uuid`, `primary_endpoint_uuid`, `mirror_endpoint_uuid`, `endpoint_kind`, and `req_type` |
| `gateway.mirror_latency_microseconds`     | Histogram     | μs    | Mirror Mode secondary dispatch latency with the same labels as `gateway.mirror_requests_total` |
| `gateway.mirror_errors_total`             | Counter       | -     | Mirror Mode secondary upstream errors; adds `reason` |
| `gateway.mirror_skipped_total`            | Counter       | -     | Mirror Mode requests skipped for reasons such as sampling, session affinity, or in-flight limits; adds `reason` |
| `gateway.mirror_divergence_total`         | Counter       | -     | Mirror Mode response mismatches between the primary response and a secondary response; adds `reason` |
| `gateway.bridge_request_chunks_total`    | Counter       | -     | Request chunks accepted by the socket bridge                                |
| `gateway.bridge_response_chunks_total`   | Counter       | -     | Response chunks dequeued by the socket bridge                               |
| `gateway.bridge_enqueue_rejections_total` | Counter      | -     | Request/response enqueue failures labeled by `queue` and `reason`           |
| `gateway.active_connections`               | UpDownCounter | -     | Number of active gateway connections                                        |
| `gateway.lane_pool_lanes_open`             | Gauge         | -     | Direct Redis lane-pool backend connections currently open; labels `shard_id`, `interlay_uuid` |
| `gateway.lane_pool_waiters`                | Gauge         | -     | Direct Redis client tasks currently waiting on lane-pool capacity; labels `shard_id`, `interlay_uuid` |
| `gateway.lane_pool_acquire_wait_microseconds` | Histogram | μs    | Time a direct Redis request waited on lane-pool capacity; labels `shard_id`, `interlay_uuid` |
| `gateway.lane_pool_init_events_total`      | Counter       | -     | Direct Redis lane-pool initialization outcomes; labels `shard_id`, `interlay_uuid`, `result`, `reason` |
| `gateway.lane_pool_init_duration_microseconds` | Histogram | μs | Time spent opening direct Redis lane-pool backend connections; labels `shard_id`, `interlay_uuid`, `result`, `reason` |
| `gateway.lane_pool_init_wait_microseconds` | Histogram     | μs    | Time concurrent direct Redis callers waited for an in-flight lane-pool initialization attempt; labels `shard_id`, `interlay_uuid`, `result`, `reason` |
| `gateway.direct_safe_connections_total`    | Counter       | -     | Direct Redis client connections that entered safe lane-pool mode            |
| `gateway.direct_unsafe_connections_total`  | Counter       | -     | Direct Redis client connections promoted to unsafe pinned mode; labeled by `reason` |
| `gateway.direct_safe_connections_active`   | Gauge         | -     | Direct Redis client connections currently in safe lane-pool mode            |
| `gateway.direct_unsafe_connections_active` | Gauge         | -     | Direct Redis client connections currently in unsafe pinned mode; labeled by `reason` |
| `gateway.direct_state_update_dispatch_failures_total` | Counter | - | Direct Redis shard-state cleanup/update dispatch or acknowledgement failures. Interlay cleanup adds `operation` (`interlay_clear` or `interlay_retire`) and `cleanup_reason`; endpoint cleanup uses `operation=endpoint_evict`, `endpoint_uuid`, `shard_id`, `cleanup_reason`, `reason`, and optional `result` |
| `gateway.bytes_read_total`                 | Counter       | bytes | Total bytes read from gateway clients                                       |
| `gateway.bytes_written_total`              | Counter       | bytes | Total bytes written to gateway clients                                      |
| `gateway.errors_total`                     | Counter       | -     | Total gateway command errors                                                |
| `gateway.connection_failures_total`        | Counter       | -     | Total gateway connection failures                                           |
| `gateway.redis.command_end_to_end_microseconds` | Histogram | μs    | Redis command-equivalent full gateway latency: bridge first sees client bytes → bridge finishes writing the corresponding response, normalized by the command count carried with that response. Use this for per-command latency targets under pipelining. |
| `gateway.end_to_end_microseconds`          | Histogram     | μs    | Raw pipeline/batch drain latency: bridge first sees client bytes → bridge finishes writing the corresponding response. This can represent many Redis commands in one pipelined response batch. |
| `gateway.bridge_parse_microseconds`        | Histogram     | μs    | Bridge-side RESP parse loop latency (Redis only): cost of `parse_command_view_meta` on the bytes received from the client socket before they ship as a `RedisIngressBatch` |
| `gateway.multiplex_total_microseconds`     | Histogram     | μs    | Full `RedisDirectMultiplexer::send` wall clock, from caller entry to caller receiving the response oneshot |
| `gateway.multiplex_bus_send_microseconds`  | Histogram     | μs    | Time the request waits for a multiplexer worker mpsc channel to accept it (channel-full backpressure) |
| `gateway.multiplex_worker_pickup_microseconds` | Histogram | μs    | Time the request sits in the worker's mpsc buffer before the worker begins dispatching |
| `gateway.multiplex_write_microseconds`     | Histogram     | μs    | TCP write latency at the multiplexer worker for the request bytes           |
| `gateway.multiplex_pipeline_wait_microseconds` | Histogram | μs    | Worker write completion → response read completion. Captures Redis pipeline depth on the worker connection |
| `gateway.multiplex_oneshot_delivery_microseconds` | Histogram | μs | Worker response oneshot delivery latency |
| `gateway.multiplex_dispatch_command_count` | Histogram | - | Number of RESP commands per multiplexer dispatch |
| `gateway.multiplex_workers`                | Gauge         | -     | Active multiplexer worker connections per endpoint, sampled every autoscaler tick (default 100ms) |
| `gateway.multiplex_inflight`               | Gauge         | -     | In-flight multiplexer requests across all workers per endpoint, sampled every autoscaler tick |
| `gateway.multiplex_scale_up_total`         | Counter       | -     | Multiplexer autoscaler scale-up events                                       |
| `gateway.multiplex_dispatch_failures_total` | Counter      | -     | Multiplexer dispatches rejected because every worker channel was unavailable |
| `gateway.bridge_recv_to_dispatch_microseconds` | Histogram | μs | Time from bridge byte arrival to multiplexer dispatch entry |
| `gateway.shard_connections_active`          | Gauge         | -     | Connections currently dispatched to each thread-per-core shard runtime; label `shard_id`. Mirrors the per-router inflight counter that backs two-choice load balancing; use to verify `assign_shards` + `pick_shorter` are spreading load across shards rather than crowding one. **Note**: the legacy Redis processor multiplexer is disabled by default; if `multiplexed_connections` is set above `0`, each shard runtime instantiates its own per-endpoint multiplexer on first legacy dispatch, so the *process-wide* backend connection count for one endpoint is `multiplexed_connections * N_shards`; size against backend `maxclients` accordingly |
| `gateway.shard_requests_inflight`           | Gauge         | -     | In-flight per-batch requests on each shard; label `shard_id`. Distinct from `shard_connections_active`: counts request batches inside each connection. Currently 0 (per-batch dispatch hasn't landed) |
| `gateway.shard_dispatch_failures_total`     | Counter       | -     | `ShardRouter::dispatch` failures by shard and reason; labels `shard_id`, `reason` (e.g. `shard_closed`). Expected ~zero in steady state; non-zero means a shard runtime exited unexpectedly |
| `gateway.shard_local_tasks_spawned_total`   | Counter       | -     | Tasks routed through `tokio::task::spawn_local` by `eden_gateway_core::runtime::spawn_on_current_runtime`; label `shard_id`. Useful for catching regressions where hot-path work falls back to `tokio::spawn` (zero growth on a busy shard means the spawn affinity broke) |

### Metric Chain

Fast-telemetry is designed for low-overhead hot-path collection, so the proxy
records several fine-grained timing segments across the request lifecycle. Keep
the segment boundaries clear when building dashboards; the metrics are most
useful when grouped by the same `interlay_uuid`, `endpoint_uuid`, and
`endpoint_kind`.

| Order | Segment                                      | Metric                                          | Starts                                                        | Stops                                                         | Notes                                                                 |
| ----- | -------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------- |
| 1     | Client-to-processor queue                    | `gateway.bridge_request_queue_microseconds`        | After the bridge reads a client chunk and enqueues it         | When the protocol processor receives the queued chunk         | Captures bridge/processor scheduling and backpressure before parsing  |
| 2     | Protocol parse/framing                       | `gateway.parse_duration_microseconds`              | When the processor starts decoding a chunk/message            | After complete protocol messages are identified               | Umbrella parse metric; pair with the gate/decode/materialize/copy/bookkeeping lenses below to localize Redis parse overhead |
| 3     | Policy and routing preparation               | `gateway.policy_routing_duration_microseconds`     | Before policy checks, routing refresh, and dispatch setup     | Immediately before backend dispatch where practical           | Recorded once per completed Redis batch as an accumulated batch total, including dispatch prep before backend timing starts |
| 4     | Processor-visible request batch              | `gateway.comparable_request_duration_microseconds` | When the processor begins a comparable request batch          | When the processor finishes the same batch                    | Use this for algebraic breakdowns; only emitted when endpoint timing is comparable |
| 5     | Backend endpoint dispatch within the batch   | `gateway.endpoint_duration_microseconds`           | Immediately before backend endpoint dispatch                  | After backend endpoint dispatch returns                       | Included inside comparable request duration                           |
| 6     | Eden gateway overhead within the batch       | `gateway.overhead_duration_microseconds`           | Derived from the same comparable batch                        | Derived from the same comparable batch                        | `overhead = comparable_request - endpoint_duration`                   |
| 7     | Analytics/audit recording                    | `gateway.analytics_record_duration_microseconds`   | Before wire analytics/audit recording                         | After analytics/audit recording completes                     | Recorded once per completed Redis batch as an accumulated batch total inside the unified batch window |
| 8     | Response encoding/assembly                   | `gateway.response_encode_duration_microseconds`    | Before response bytes are assembled                           | Before the response is queued to the bridge                   | Recorded once per completed Redis batch as an accumulated batch total |
| 9     | Processor-to-client queue                    | `gateway.bridge_response_queue_microseconds`       | After the processor enqueues a response chunk                 | When the bridge dequeues the response for client write        | Response chunks are not guaranteed to map 1:1 with request batches under pipelining |
| 10    | Client socket write                          | `gateway.bridge_client_write_microseconds`         | Immediately before writing a response chunk to the client     | After `write_all` completes                                   | Captures slow clients, kernel/socket backpressure, and bridge write time |
| Lens  | Endpoint network I/O                         | `gateway.network_latency_microseconds`             | Inside endpoint raw-byte execution around backend network I/O | After backend network I/O returns                             | Diagnostic lens inside endpoint dispatch; do not subtract directly from request duration |
| Lens  | Backend client/pool acquisition              | `gateway.backend_pool_wait_microseconds`           | Before visible proxy-side backend/pinned-client acquisition   | After client acquisition completes                            | Diagnostic lens; lower-level endpoint pool waits may be inside endpoint dispatch |
| Umbrella | Command-equivalent full gateway latency  | `gateway.redis.command_end_to_end_microseconds`          | When the bridge first observes a client batch's bytes (Redis) | After the bridge finishes writing the corresponding response  | Full-path latency normalized by the number of Redis commands represented by the response; use this for low-latency command SLOs under pipelining |
| Umbrella | Raw pipeline/batch drain latency         | `gateway.end_to_end_microseconds`                  | When the bridge first observes a client batch's bytes (Redis) | After the bridge finishes writing the corresponding response  | Full response/batch drain view; a single sample can represent a large pipelined batch, so do not read it as single-command latency |
| 0     | Bridge-side RESP parse                       | `gateway.bridge_parse_microseconds`                | After `read_buf` returns on the bridge                         | After the bridge's parse loop finishes consuming complete frames | Redis-only; cost of materializing parsed view-metas in the bridge before shipping a `RedisIngressBatch` |
| 6a    | Multiplexer total                            | `gateway.multiplex_total_microseconds`             | When `multiplex.send()` is entered                             | When the caller receives the worker's response oneshot         | Inside `endpoint_duration`; equals `bus_send + worker_pickup + write + pipeline_wait` plus minor wakeup overhead |
| 6a-i  | Multiplexer bus send                         | `gateway.multiplex_bus_send_microseconds`          | At `multiplex.send()` entry                                    | After `worker.sender.send().await` returns Ok                  | Captures channel-full backpressure into the worker mpsc                |
| 6a-ii | Multiplexer worker pickup                    | `gateway.multiplex_worker_pickup_microseconds`     | Once the request lands in the worker's mpsc buffer             | When the worker begins `dispatch_request`                      | Time the request sits in the worker's queue before being processed     |
| 6a-iii | Multiplexer worker write                    | `gateway.multiplex_write_microseconds`             | Just before the worker's TCP write                             | Just after the TCP write returns                               | TCP write latency at the worker; usually small unless backend stalls   |
| 6a-iv | Multiplexer pipeline wait                    | `gateway.multiplex_pipeline_wait_microseconds`     | Just after the worker's TCP write completes                    | After the worker's response read completes                     | Captures Redis pipeline depth on the worker connection; ≈ `network_latency` |
| Lens  | Parse gate / branch selection                | `gateway.parse_gate_duration_microseconds`         | Before parse-path gating begins                               | After the parser path is selected                             | Captures feature gates and passthrough eligibility checks before decode |
| Lens  | Protocol decode/scan                         | `gateway.parse_decode_duration_microseconds`       | Before frame boundary scan or decode begins                   | After a complete frame boundary or parsed frame is identified | Isolates RESP boundary scanning/decoding from later command materialization |
| Lens  | Command materialization                      | `gateway.parse_materialize_duration_microseconds`  | Before parsed frames are converted into command structs       | After command structs are built                               | Redis parsed-pipeline mode uses one coarse batch timer for scan plus command identification so timing overhead does not dominate hot pipelines |
| Lens  | Parsed request byte assembly                 | `gateway.parse_copy_duration_microseconds`         | Before parsed request bytes are assembled for dispatch        | After backend-dispatch byte slices are ready                  | Helps confirm whether Redis dispatch remains zero-copy on the hot path |
| Lens  | Parse bookkeeping / residual                 | `gateway.parse_bookkeeping_duration_microseconds`  | Derived from the same parse interval                          | Derived from the same parse interval                          | Residual parse time such as loop control, cursor movement, vector pushes, and error-path bookkeeping |

For comparable Redis/Postgres/Mongo request batches:

```text
gateway.comparable_request_duration_microseconds
  = gateway.endpoint_duration_microseconds
  + gateway.overhead_duration_microseconds
```

Inside `gateway.endpoint_duration_microseconds`, the Redis multiplexer pipeline
decomposes as:

```text
gateway.endpoint_duration_microseconds
  ≈ gateway.multiplex_total_microseconds
  ≈ gateway.multiplex_bus_send_microseconds
  + gateway.multiplex_worker_pickup_microseconds
  + gateway.multiplex_write_microseconds
  + gateway.multiplex_pipeline_wait_microseconds
```

`gateway.network_latency_microseconds` is recorded inside the worker around the
write+pipeline+read window, so it overlaps `gateway.multiplex_write_microseconds
+ gateway.multiplex_pipeline_wait_microseconds`. Use the multiplex breakdown to
attribute time inside the dispatch path; use `gateway.network_latency_microseconds`
as a sanity-check lens.

For a command-level path dashboard under Redis pipelining, use
`gateway.redis.command_end_to_end_microseconds`. The raw batch-drain umbrella covering
the entire response flow is `gateway.end_to_end_microseconds`:

```text
gateway.end_to_end_microseconds
  ≈ gateway.bridge_parse_microseconds                  (Redis only)
  + gateway.bridge_request_queue_microseconds
  + gateway.comparable_request_duration_microseconds
  + gateway.bridge_response_queue_microseconds
  + gateway.bridge_client_write_microseconds
```

This path view is intentionally more useful as a latency attribution stack than
as exact per-command accounting. Request batches, backend operations, and
response chunks can have different cardinalities when a protocol pipelines
commands or splits responses. `gateway.redis.command_end_to_end_microseconds` carries
the same full-path window but normalizes the recorded value by the Redis command
count represented by the response, so a 100,000-command pipeline drain does not
masquerade as the latency of one command.

Use the fine-grained processor metrics to explain changes inside
`gateway.overhead_duration_microseconds`. Redis records the listed processor
segments once per completed batch as accumulated batch totals, which keeps
simple averages comparable with the unified batch metric. For example, graph
`gateway.parse_duration_microseconds`,
`gateway.parse_gate_duration_microseconds`,
`gateway.parse_decode_duration_microseconds`,
`gateway.parse_materialize_duration_microseconds`,
`gateway.parse_copy_duration_microseconds`,
`gateway.parse_bookkeeping_duration_microseconds`,
`gateway.policy_routing_duration_microseconds`,
`gateway.analytics_record_duration_microseconds`, and
`gateway.response_encode_duration_microseconds` together when overhead increases.

For Redis dispatch latency growth specifically, the multiplexer pipeline
provides finer attribution. Graph
`gateway.multiplex_bus_send_microseconds`,
`gateway.multiplex_worker_pickup_microseconds`,
`gateway.multiplex_write_microseconds`, and
`gateway.multiplex_pipeline_wait_microseconds` together to see whether the
contention is in the bus channel (`bus_send`), in the worker queue
(`worker_pickup`), at the TCP write (`write`), or downstream of Redis
processing (`pipeline_wait`). Pair with `gateway.multiplex_workers` and
`gateway.multiplex_inflight` to see how the autoscaler is reacting to load
and whether `gateway.multiplex_scale_up_total` is firing.

### Labels

- `interlay_uuid`: Interlay UUID
- `endpoint_uuid`: Endpoint UUID
- `endpoint_kind`: Endpoint kind/protocol, for example `redis`, `postgres`, or `mongo`
- `org_uuid`: Organization UUID
- `command_type`: Redis command type (GET, SET, HGET, etc.)
- `error_type`: Type of error (parse_error, timeout, connection_error, tls_error, bind_error, accept_error, secondary_write_failed)
- `queue`: Bridge queue name for enqueue rejection counters, for example `request` or `response`
- `reason`: Bridge enqueue rejection reason, for example `processor_closed` or `queue_full_or_closed`

### Key Dashboards

- **Gateway Throughput**: `rate(gateway.requests_total)`
- **Gateway Command End-to-End Latency**: `histogram_quantile(0.99, gateway.redis.command_end_to_end_microseconds)`. This is the command-level umbrella metric for gateway-induced latency on the Redis bridge under pipelining.
- **Gateway Pipeline Drain Latency**: `histogram_quantile(0.99, gateway.end_to_end_microseconds)`. This is the raw full response/batch drain view and can be large when one sample contains many pipelined Redis commands.
- **Gateway Latency P99**: `histogram_quantile(0.99, gateway.request_duration_microseconds)`
- **Gateway Added Latency**: `gateway.overhead_duration_microseconds`.
- **Gateway Latency Breakdown**: `gateway.comparable_request_duration_microseconds = gateway.endpoint_duration_microseconds + gateway.overhead_duration_microseconds` for comparable Redis/Postgres/Mongo batches. Filter or group by `endpoint_kind` when comparing protocols; the Eden dashboard exposes this as the Gateway endpoint type filter for `gateway.*` panels. Avoid subtracting `gateway.network_latency_microseconds` from `gateway.request_duration_microseconds` because they use different recording points and label sets.
- **Full Gateway Path**: `gateway.redis.command_end_to_end_microseconds` for command-level SLOs, `gateway.end_to_end_microseconds` for raw pipeline drains, or for the manually-stacked equivalent `gateway.bridge_parse_microseconds + gateway.bridge_request_queue_microseconds + gateway.comparable_request_duration_microseconds + gateway.bridge_response_queue_microseconds + gateway.bridge_client_write_microseconds`. Treat the stacked view as path-level attribution rather than exact per-command algebra when requests are pipelined, because request batches and response chunks do not always have a 1:1 relationship.
- **Multiplexer Pipeline Breakdown**: graph `gateway.multiplex_bus_send_microseconds`, `gateway.multiplex_worker_pickup_microseconds`, `gateway.multiplex_write_microseconds`, and `gateway.multiplex_pipeline_wait_microseconds` stacked or as separate quantiles, grouped by `endpoint_uuid`. Together they fully decompose `gateway.multiplex_total_microseconds` and explain Redis `endpoint_duration` movement.
- **Multiplexer Capacity**: `gateway.multiplex_workers` (gauge) and `gateway.multiplex_inflight` (gauge) per `endpoint_uuid`. Watch `gateway.multiplex_scale_up_total` rate when load ramps; an alert on `gateway.multiplex_dispatch_failures_total > 0` catches situations where every worker channel is unavailable.
- **Connection Health**: `gateway.active_connections` and `gateway.connection_failures_total`
- **Bandwidth**: `rate(gateway.bytes_read_total) + rate(gateway.bytes_written_total)`

## Wire vs Polling Metrics

Wire metrics are collected inline from gateway traffic as requests flow through the wire protocol, providing low-latency visibility without querying the database engine. Polling metrics come from periodic server introspection and capture system snapshots that are not observable on the wire.

Wire metrics capture latency, command distribution, hot keys, error rates, cache hit/miss signals, and TTL/expiration behavior derived from traffic. Polling metrics remain the source of truth for CPU, memory, replication state, cluster topology, and configuration data.

---

## Workload Metrics (AMR Profiling)

Prefix: `workload.`

Metrics for Azure Managed Redis (AMR) workload profiling. Used to recommend the appropriate AMR instance type based on the `ops_per_sec / used_memory_mb` ratio.

### Definition

**Source:** [eden_core/telemetry/src/metrics/workload.rs](../eden_core/telemetry/src/metrics/workload.rs)

### Collection Points

| Module                                 | Function                                | Metrics Collected                                       |
| -------------------------------------- | --------------------------------------- | ------------------------------------------------------- |
| `endpoints::endpoint::redis::metadata` | `MetadataScheduler::run_workload_job()` | WorkloadSnapshot via MetricEvent (all workload metrics) |

**Data Source:** Redis `INFO` command (memory, stats sections)

### Metrics

| Metric                               | Type  | Unit  | Description                                                                                    |
| ------------------------------------ | ----- | ----- | ---------------------------------------------------------------------------------------------- |
| `workload.avg_ops_per_sec`           | Gauge | ops/s | Average operations per second (calculated from `total_commands_processed` delta between syncs) |
| `workload.used_memory_mb`            | Gauge | MB    | Current memory usage                                                                           |
| `workload.database_size_gb`          | Gauge | GB    | Database size                                                                                  |
| `workload.workload_ratio`            | Gauge | -     | Workload ratio: `ops_per_sec / used_memory_mb`                                                 |
| `workload.amr_profile`               | Gauge | -     | AMR profile classification (0=Memory, 1=Balanced, 2=Compute)                                   |
| `workload.total_keys`                | Gauge | -     | Total keys in database                                                                         |
| `workload.keys_with_ttl`             | Gauge | -     | Keys with TTL set                                                                              |
| `workload.instantaneous_ops_per_sec` | Gauge | ops/s | Instantaneous ops/sec from Redis INFO (real-time snapshot)                                     |
| `workload.total_commands_processed`  | Gauge | -     | Cumulative total commands processed                                                            |
| `workload.used_cpu_user`             | Gauge | s     | CPU time spent in user space                                                                   |
| `workload.connected_clients`         | Gauge | -     | Connected clients                                                                              |

### AMR Profile Classification

| Workload Ratio | AMR Profile  | Recommended SKU              | Use Cases                                              |
| -------------- | ------------ | ---------------------------- | ------------------------------------------------------ |
| < 1            | Memory (0)   | M-series (Memory-optimized)  | Lookup-heavy apps, session stores, large object caches |
| 1 to 50        | Balanced (1) | P-series (General Purpose)   | Typical cache workloads, API caches                    |
| > 50           | Compute (2)  | C-series (Compute-optimized) | Real-time pub/sub, message queues, AI inference        |

### Labels

- `endpoint_uuid`: Endpoint UUID
- `org_uuid`: Organization UUID
- `amr_profile`: AMR profile name (Memory, Balanced, Compute)

### Key Dashboards

- **AMR Recommendation**: Display `workload.amr_profile` with SKU recommendation
- **Workload Ratio Trend**: `workload.workload_ratio` over time
- **Memory vs Ops**: Scatter plot of `workload.used_memory_mb` vs `workload.avg_ops_per_sec`

---

## Load Balancer Metrics

Metrics for the Pingora-based load balancer.

### Definition

**Source:** [load_balancer/src/telemetry.rs](../load_balancer/src/telemetry.rs)

### Collection Points

| Module                     | Function                   | Metrics Collected                    |
| -------------------------- | -------------------------- | ------------------------------------ |
| `load_balancer::telemetry` | `LbMetrics::add_request()` | eden_load_balancer_requests_received |
| `load_balancer::telemetry` | `LbMetrics::add_latency()` | eden_load_balancer_request_latency   |

### Metrics

| Metric                                 | Type      | Unit     | Description                                        |
| -------------------------------------- | --------- | -------- | -------------------------------------------------- |
| `eden_load_balancer_requests_received` | Counter   | requests | Total number of requests received by load balancer |
| `eden_load_balancer_request_latency`   | Histogram | μs       | Total latency of requests through load balancer    |

### Labels

- `upstream`: Upstream server identifier
- `status_code`: HTTP status code
- `method`: HTTP method (GET, POST, etc.)

---

## Common Labels

These labels are available across most metrics via `TelemetryLabels`:

| Label           | Description                                          |
| --------------- | ---------------------------------------------------- |
| `org_uuid`      | Organization UUID                                    |
| `endpoint_uuid` | Endpoint UUID                                        |
| `user_uuid`     | User UUID                                            |
| `feature`       | Feature name (e.g., "auth", "interlay")              |
| `trace_id`      | OpenTelemetry trace ID                               |
| `span_id`       | OpenTelemetry span ID                                |

---

## Metric Types Reference

| Type              | Description                         | Use Case                                         |
| ----------------- | ----------------------------------- | ------------------------------------------------ |
| **Counter**       | Monotonically increasing value      | Request counts, error counts, bytes transferred  |
| **UpDownCounter** | Value that can increase or decrease | Active connections, queue sizes                  |
| **Gauge**         | Point-in-time value                 | Memory usage, progress percentage, current state |
| **Histogram**     | Distribution of values              | Latency, request sizes                           |

---

## ClickHouse Analytics Metrics

Prefix: `eden.analytics.*`

ClickHouse stores detailed telemetry and endpoint poll analytics for supported protocols (Redis, MongoDB, PostgreSQL).

### Definition

**Schema:** [database/analytics_schema/sql/analytics/](../database/analytics_schema/sql/analytics/)

### Data Flow

```
Gateway and endpoint telemetry --> ClickHouse --> dashboard/API queries
```

### Tables

Canonical table inventory with engines and TTLs is in `ANALYTICS.md`. The tables most relevant to dashboards:

| Table                       | Purpose                                                                | TTL      |
| --------------------------- | ---------------------------------------------------------------------- | -------- |
| `command_rollups`           | Per-command, per-60s-window counts with `command_id`, sampled latency moments, cache and error-category splits (all protocols) | 7 days   |
| `command_rollups_hourly`    | Hourly aggregation of `command_rollups` with the same column set | 90 days  |
| `endpoint_metrics`          | Per-endpoint snapshots: latency, cache, command mix (all protocols)    | 90 days  |
| `target_pattern_rollups`    | Per-key-pattern cost, read/write balance (Redis-specific)              | 30 days  |
| `mongo_shape_rollups`       | Per-shape query metrics, pipeline stages (MongoDB-specific)            | 30 days  |
| `anti_patterns`             | Detected anti-patterns (N+1, hot keys, KEYS usage)                     | 30 days  |
| `anomaly_transitions`       | Detector state transitions per endpoint                                | 90 days  |
| `audit_trail`               | Compliance audit log                                                   | 365 days |
| `infrastructure_snapshots`  | Data snapshot (fan-out copy) operation metrics                         | 90 days  |

### Endpoint Metrics

| Metric                                      | Type  | Description                                            |
| ------------------------------------------- | ----- | ------------------------------------------------------ |
| `eden.analytics.endpoint.ops_per_sec`       | Gauge | Aggregate operations per second by protocol            |
| `eden.analytics.endpoint.commands`          | Gauge | Total endpoint commands observed in the last 5 minutes |
| `eden.analytics.endpoint.errors`            | Gauge | Total endpoint errors observed in the last 5 minutes   |
| `eden.analytics.endpoint.latency_p99_ms`    | Gauge | Average P99 endpoint latency by protocol               |
| `eden.analytics.endpoint.connected_clients` | Gauge | Connected client count by protocol                     |

### Command Metrics

| Metric                                             | Type  | Description                                     |
| -------------------------------------------------- | ----- | ----------------------------------------------- |
| `eden.analytics.command.requests`                  | Gauge | Request count from command rollups by protocol  |
| `eden.analytics.command.requests_by_command`       | Gauge | Top command request counts by protocol/command  |
| `eden.analytics.command.errors`                    | Gauge | Command error count by protocol                 |
| `eden.analytics.command.avg_latency_ms`            | Gauge | Average sampled command latency by protocol     |

### API Metrics

| Metric                                | Type  | Description                                      |
| ------------------------------------- | ----- | ------------------------------------------------ |
| `eden.analytics.api.requests`         | Gauge | Eden API requests by HTTP status                 |
| `eden.analytics.api.server_errors`    | Gauge | Eden API 5xx requests by HTTP method             |
| `eden.analytics.api.avg_latency_ms`   | Gauge | Average Eden API latency by HTTP method          |

### Anti-Pattern Metrics

| Metric                               | Type  | Description                                  |
| ------------------------------------ | ----- | -------------------------------------------- |
| `eden.analytics.anti_patterns.count` | Gauge | Anti-pattern occurrences by detected pattern |

### Infrastructure Snapshot Metrics

| Metric                                         | Type  | Description                         |
| ---------------------------------------------- | ----- | ----------------------------------- |
| `eden.analytics.snapshot.count_by_status`      | Gauge | Snapshot counts by status           |
| `eden.analytics.snapshot.bytes_written_total`  | Gauge | Total bytes written by snapshots    |
| `eden.analytics.snapshot.avg_duration_secs`    | Gauge | Average snapshot duration, seconds  |

### Labels

- `endpoint_uuid`: Endpoint identifier
- `command`: Redis command type
- `signal_type`: Signal classification
- `pattern_type`: Anti-pattern classification
- `latency_severity`: Latency severity level

### Key Dashboards

- **Pattern Discovery**: `eden.analytics.patterns.total` trend with `eden.analytics.patterns.new` overlay
- **Anomaly Detection**: Stacked chart of `eden.analytics.signals.count` by type
- **Anti-Pattern Alerts**: Alert on `eden.analytics.anti_patterns.*` spikes
- **Wire Performance**: `eden.wire.latency_p99_ms` with `eden.wire.error_rate_pct`
- **Endpoint Health**: Heatmap of `eden.wire.ops_per_sec` by endpoint
