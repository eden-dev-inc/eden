//! Eden service metrics using fast-telemetry for high-performance counting.
//!
//! These metrics track request lifecycle, cache performance, and LLM usage.

use crate::TelemetryDurations;
use crate::labels::{LABEL_ORG_UUID, SYSTEM_ORG_UUID};
use chrono::{DateTime, Utc};
use fast_telemetry::DynamicLabelSet;
use fast_telemetry::{DynamicCounter, DynamicDistribution, DynamicGauge, DynamicGaugeI64, ExportMetrics};
use format::id::Username;
use format::{EdenUuid, EndpointUuid, OrganizationUuid};
use std::borrow::Cow;
use std::collections::HashSet;
use tokio::sync::Mutex;

/// Default shard count for thread-sharded counters.
/// Should be >= number of CPU cores for optimal performance.
const SHARD_COUNT: usize = 16;
const EDEN_DIST_MAX_SERIES: usize = 2000;

fn labels_with_org_uuid<'a>(labels: &'a [(&'a str, &'a str)]) -> Cow<'a, [(&'a str, &'a str)]> {
    if labels.iter().any(|(key, value)| (*key == LABEL_ORG_UUID || *key == "organization_uuid") && !value.is_empty()) {
        Cow::Borrowed(labels)
    } else {
        let mut labels_with_org = Vec::with_capacity(labels.len() + 1);
        labels_with_org.extend_from_slice(labels);
        labels_with_org.push((LABEL_ORG_UUID, SYSTEM_ORG_UUID));
        Cow::Owned(labels_with_org)
    }
}

/// Core metrics that can be exported via the ExportMetrics derive macro.
///
/// Uses fast-telemetry's thread-sharded counters for ~2ns increments
/// instead of OpenTelemetry's ~40-400ns atomic operations.
#[derive(ExportMetrics)]
#[metric_prefix = "eden"]
#[otlp]
#[clickhouse]
pub struct EdenCoreMetrics {
    /// Tracks number of requests currently being processed (with org_uuid, endpoint_type labels)
    #[help = "Number of requests currently being processed"]
    pub active_requests: DynamicGaugeI64,

    /// Counts the total number of requests received (with org_uuid, endpoint_type labels)
    #[help = "Total number of requests received"]
    pub request_count: DynamicCounter,

    /// Count the total number of responses received (with org_uuid, endpoint_type labels)
    #[help = "Total number of responses sent"]
    pub response_count: DynamicCounter,

    /// Tracks the distribution of total process durations (microseconds)
    #[help = "Distribution of total request durations in microseconds"]
    pub total_duration: DynamicDistribution,

    /// Tracks the distribution of eden-exclusive durations (with org_uuid, endpoint_type labels)
    #[help = "Distribution of Eden processing time (excluding endpoint) in microseconds"]
    pub eden_duration: DynamicDistribution,

    /// Counts the number of success responses (with org_uuid, endpoint_type labels)
    #[help = "Total number of successful requests"]
    pub success_count: DynamicCounter,

    /// Counts the number of error responses (with org_uuid, endpoint_type labels)
    #[help = "Total number of failed requests"]
    pub error_count: DynamicCounter,

    /// Counts total bytes uploaded (with org_uuid, endpoint_type labels)
    #[help = "Total bytes uploaded in request bodies"]
    pub upload_byte_count: DynamicCounter,

    /// Tracks the distribution of bytes uploaded (with org_uuid, endpoint_type labels)
    #[help = "Distribution of request body sizes in bytes"]
    pub upload_byte_distribution: DynamicDistribution,

    /// Counts total bytes downloaded (with org_uuid, endpoint_type labels)
    #[help = "Total bytes downloaded in response bodies"]
    pub download_byte_count: DynamicCounter,

    /// Tracks the distribution of bytes downloaded (with org_uuid, endpoint_type labels)
    #[help = "Distribution of response body sizes in bytes"]
    pub download_byte_distribution: DynamicDistribution,

    /// Counts number of unique users
    #[help = "Total number of unique users seen"]
    pub unique_users: DynamicCounter,

    /// Counts the number of logins (with user_id, org_uuid labels)
    #[help = "Total number of login events"]
    pub logins: DynamicCounter,

    /// Counts the number of Local Cache Hits (with cache_type label)
    #[help = "Total number of local cache hits"]
    pub local_cache_hits: DynamicCounter,

    /// Counts the number of Local Cache Misses (with cache_type label)
    #[help = "Total number of local cache misses"]
    pub local_cache_misses: DynamicCounter,

    /// Counts the number of Redis Cache Hits (with cache_type label)
    #[help = "Total number of Redis cache hits"]
    pub redis_cache_hits: DynamicCounter,

    /// Counts the number of Redis Cache Misses (with cache_type label)
    #[help = "Total number of Redis cache misses"]
    pub redis_cache_misses: DynamicCounter,

    /// Count of open async connections (idle + in-use), with `db_type` / `endpoint_uuid` labels.
    #[help = "Number of open connections (idle + in-use)"]
    pub connections: DynamicGaugeI64,

    /// Count of connections currently checked out from a pool and in use.
    #[help = "Number of connections currently in use (checked out from pool)"]
    pub connections_in_use: DynamicGaugeI64,

    /// LLM request counter (with provider, model, endpoint_uuid, org_uuid labels)
    #[help = "Total number of LLM API requests"]
    pub llm_requests: DynamicCounter,

    /// LLM gateway HTTP request counter (with route, status, provider, model, endpoint_uuid, org_uuid labels)
    #[help = "Total number of LLM gateway HTTP requests"]
    pub llm_gateway_requests: DynamicCounter,

    /// LLM gateway HTTP error counter (with route, status, error_type, provider, model labels)
    #[help = "Total number of failed LLM gateway HTTP requests"]
    pub llm_gateway_errors: DynamicCounter,

    /// Distribution of LLM gateway request durations in microseconds.
    #[help = "Distribution of LLM gateway request durations in microseconds"]
    pub llm_gateway_request_duration_microseconds: DynamicDistribution,

    /// Distribution of time to first streaming chunk in microseconds.
    #[help = "Distribution of LLM gateway time to first streaming chunk in microseconds"]
    pub llm_gateway_time_to_first_chunk_microseconds: DynamicDistribution,

    /// Distribution of time between streaming output chunks in microseconds.
    #[help = "Distribution of LLM gateway time between streaming output chunks in microseconds"]
    pub llm_gateway_time_per_output_chunk_microseconds: DynamicDistribution,

    /// Total streaming chunks sent by the LLM gateway.
    #[help = "Total number of streaming chunks emitted by the LLM gateway"]
    pub llm_gateway_stream_chunks: DynamicCounter,

    /// Total prompt tokens consumed by LLM calls (with provider, model labels)
    #[help = "Total prompt tokens sent to LLMs"]
    pub llm_prompt_tokens: DynamicCounter,

    /// Total completion tokens produced by LLM calls (with provider, model labels)
    #[help = "Total completion tokens generated by LLMs"]
    pub llm_completion_tokens: DynamicCounter,

    /// Total tokens consumed by LLM calls (with provider, model labels)
    #[help = "Total tokens processed by LLMs (prompt + completion)"]
    pub llm_total_tokens: DynamicCounter,

    /// Distribution of total tokens per LLM call (with provider, model labels)
    #[help = "Distribution of tokens per LLM request"]
    pub llm_total_tokens_distribution: DynamicDistribution,

    /// Cached prompt tokens reported by providers.
    #[help = "Total cached prompt tokens reported by LLM providers"]
    pub llm_cached_prompt_tokens: DynamicCounter,

    /// Prompt audio tokens reported by providers.
    #[help = "Total prompt audio tokens reported by LLM providers"]
    pub llm_prompt_audio_tokens: DynamicCounter,

    /// Reasoning tokens reported by providers.
    #[help = "Total reasoning completion tokens reported by LLM providers"]
    pub llm_reasoning_completion_tokens: DynamicCounter,

    /// Completion audio tokens reported by providers.
    #[help = "Total completion audio tokens reported by LLM providers"]
    pub llm_completion_audio_tokens: DynamicCounter,

    /// RBAC Redis stream lag (unread + pending-unacked) for PG sync consumer.
    #[help = "RBAC PG sync stream lag"]
    pub rbac_pg_sync_lag: DynamicGauge,

    // =========================================================================
    // Orchestration metrics
    // =========================================================================
    /// Total orchestration runs started (with org_uuid label)
    #[help = "Total orchestration runs started"]
    pub orchestration_runs: DynamicCounter,

    /// Orchestration runs resumed from checkpoint (with org_uuid label)
    #[help = "Total orchestration runs resumed from checkpoint"]
    pub orchestration_resumes: DynamicCounter,

    /// Orchestration sub-tasks started (with org_uuid, task_id labels)
    #[help = "Total orchestration sub-tasks started"]
    pub orchestration_subtasks_started: DynamicCounter,

    /// Orchestration sub-tasks completed successfully
    #[help = "Total orchestration sub-tasks completed successfully"]
    pub orchestration_subtasks_completed: DynamicCounter,

    /// Orchestration sub-tasks failed
    #[help = "Total orchestration sub-tasks failed"]
    pub orchestration_subtasks_failed: DynamicCounter,

    /// Orchestration sub-task retries
    #[help = "Total orchestration sub-task retry attempts"]
    pub orchestration_subtask_retries: DynamicCounter,

    /// Distribution of orchestration plan creation latency (milliseconds)
    #[help = "Distribution of orchestration planning latency in milliseconds"]
    pub orchestration_plan_duration: DynamicDistribution,

    /// Distribution of full orchestration execution latency (milliseconds)
    #[help = "Distribution of full orchestration execution latency in milliseconds"]
    pub orchestration_total_duration: DynamicDistribution,

    /// Distribution of individual sub-task execution latency (milliseconds)
    #[help = "Distribution of sub-task execution latency in milliseconds"]
    pub orchestration_subtask_duration: DynamicDistribution,

    /// Orchestration feedback requests (planner asked user for clarification)
    #[help = "Total orchestration feedback requests"]
    pub orchestration_feedback_requests: DynamicCounter,

    /// Active orchestration runs (gauge)
    #[help = "Number of orchestrations currently executing"]
    pub orchestration_active: DynamicGaugeI64,

    /// Entitlement evaluation outcomes by status (with org_uuid, status labels)
    #[help = "Total entitlement evaluations by status"]
    pub entitlement_status_total: DynamicCounter,

    /// Entitlement evaluation failures (with org_uuid, error_type labels)
    #[help = "Total entitlement evaluation failures"]
    pub entitlement_eval_failures_total: DynamicCounter,
}

/// Struct containing all the metrics for Eden service.
///
/// Wraps `EdenCoreMetrics` and adds non-exportable state like user tracking.
pub struct EdenMetrics {
    /// Core metrics (exportable)
    pub core: EdenCoreMetrics,
    /// Stores set of seen user UUIDs to track uniqueness (not exported)
    seen_users: Mutex<HashSet<String>>,
}

impl EdenMetrics {
    /// Create a new EdenMetrics instance with fast-telemetry counters.
    pub fn new() -> Self {
        Self {
            core: EdenCoreMetrics {
                // Request lifecycle metrics (with org_uuid, endpoint_type labels)
                active_requests: DynamicGaugeI64::new(SHARD_COUNT),
                request_count: DynamicCounter::new(SHARD_COUNT),
                response_count: DynamicCounter::new(SHARD_COUNT),
                total_duration: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                eden_duration: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                success_count: DynamicCounter::new(SHARD_COUNT),
                error_count: DynamicCounter::new(SHARD_COUNT),
                upload_byte_count: DynamicCounter::new(SHARD_COUNT),
                upload_byte_distribution: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                download_byte_count: DynamicCounter::new(SHARD_COUNT),
                download_byte_distribution: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                unique_users: DynamicCounter::new(SHARD_COUNT),
                logins: DynamicCounter::new(SHARD_COUNT),
                local_cache_hits: DynamicCounter::new(SHARD_COUNT),
                local_cache_misses: DynamicCounter::new(SHARD_COUNT),
                redis_cache_hits: DynamicCounter::new(SHARD_COUNT),
                redis_cache_misses: DynamicCounter::new(SHARD_COUNT),
                connections: DynamicGaugeI64::new(SHARD_COUNT),
                connections_in_use: DynamicGaugeI64::new(SHARD_COUNT),
                llm_requests: DynamicCounter::new(SHARD_COUNT),
                llm_gateway_requests: DynamicCounter::new(SHARD_COUNT),
                llm_gateway_errors: DynamicCounter::new(SHARD_COUNT),
                llm_gateway_request_duration_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                llm_gateway_time_to_first_chunk_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                llm_gateway_time_per_output_chunk_microseconds: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                llm_gateway_stream_chunks: DynamicCounter::new(SHARD_COUNT),
                llm_prompt_tokens: DynamicCounter::new(SHARD_COUNT),
                llm_completion_tokens: DynamicCounter::new(SHARD_COUNT),
                llm_total_tokens: DynamicCounter::new(SHARD_COUNT),
                llm_total_tokens_distribution: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                llm_cached_prompt_tokens: DynamicCounter::new(SHARD_COUNT),
                llm_prompt_audio_tokens: DynamicCounter::new(SHARD_COUNT),
                llm_reasoning_completion_tokens: DynamicCounter::new(SHARD_COUNT),
                llm_completion_audio_tokens: DynamicCounter::new(SHARD_COUNT),
                rbac_pg_sync_lag: DynamicGauge::new(SHARD_COUNT),

                // Orchestration metrics
                orchestration_runs: DynamicCounter::new(SHARD_COUNT),
                orchestration_resumes: DynamicCounter::new(SHARD_COUNT),
                orchestration_subtasks_started: DynamicCounter::new(SHARD_COUNT),
                orchestration_subtasks_completed: DynamicCounter::new(SHARD_COUNT),
                orchestration_subtasks_failed: DynamicCounter::new(SHARD_COUNT),
                orchestration_subtask_retries: DynamicCounter::new(SHARD_COUNT),
                orchestration_plan_duration: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                orchestration_total_duration: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                orchestration_subtask_duration: DynamicDistribution::with_max_series(SHARD_COUNT, EDEN_DIST_MAX_SERIES),
                orchestration_feedback_requests: DynamicCounter::new(SHARD_COUNT),
                orchestration_active: DynamicGaugeI64::new(SHARD_COUNT),

                entitlement_status_total: DynamicCounter::new(SHARD_COUNT),
                entitlement_eval_failures_total: DynamicCounter::new(SHARD_COUNT),
            },
            seen_users: Mutex::new(HashSet::new()),
        }
    }

    /// Export metrics in Prometheus format.
    pub fn export_prometheus(&self, output: &mut String) {
        self.core.export_prometheus(output);
    }

    /// Export metrics in DogStatsD format.
    pub fn export_dogstatsd(&self, output: &mut String, tags: &[(&str, &str)]) {
        self.core.export_dogstatsd(output, tags);
    }

    /// Called when a request starts processing.
    pub async fn start_request(&self, user: Username, bytes: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.active_requests.inc(labels);
        self.core.request_count.inc(labels);
        self.core.upload_byte_count.add(labels, bytes as isize);
        self.core.upload_byte_distribution.record(labels, bytes);

        // Track unique users
        let mut seen_users = self.seen_users.lock().await;
        if seen_users.insert(user.to_string()) {
            self.core.unique_users.inc(labels);
        }
    }

    /// Called when a request completes.
    pub fn complete_request(
        &self,
        bytes: u64,
        end: DateTime<Utc>,
        durations: &mut TelemetryDurations,
        is_error: bool,
        labels: &[(&str, &str)],
    ) {
        self.complete_request_with_active_labels(bytes, end, durations, is_error, labels, labels);
    }

    /// Complete a request while pairing the active-request decrement with the
    /// labels used at request start.
    pub fn complete_request_with_active_labels(
        &self,
        bytes: u64,
        end: DateTime<Utc>,
        durations: &mut TelemetryDurations,
        is_error: bool,
        labels: &[(&str, &str)],
        active_labels: &[(&str, &str)],
    ) {
        durations.set_eden_response(end);

        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        let active_labels = labels_with_org_uuid(active_labels);
        let active_labels = active_labels.as_ref();

        self.core.active_requests.add(active_labels, -1);
        self.core.response_count.inc(labels);

        if let Some(eden_duration_wrapper) = durations.get_eden_duration() {
            let duration_micros = eden_duration_wrapper.as_duration().num_microseconds().unwrap_or(0) as u64;
            self.core.total_duration.record(labels, duration_micros);
            self.core.eden_duration.record(labels, duration_micros);
        }

        if is_error {
            self.core.error_count.inc(labels);
        } else {
            self.core.success_count.inc(labels);
        }

        self.core.download_byte_count.add(labels, bytes as isize);
        self.core.download_byte_distribution.record(labels, bytes);
    }

    /// Record a login event.
    #[inline]
    pub fn add_login(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.logins.inc(labels);
    }

    /// Record a local cache hit.
    #[inline]
    pub fn add_local_cache_hit(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.local_cache_hits.inc(labels);
    }

    /// Record a local cache miss.
    #[inline]
    pub fn add_local_cache_miss(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.local_cache_misses.inc(labels);
    }

    /// Record a Redis cache hit.
    #[inline]
    pub fn add_redis_cache_hit(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.redis_cache_hits.inc(labels);
    }

    /// Record a Redis cache miss.
    #[inline]
    pub fn add_redis_cache_miss(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.redis_cache_misses.inc(labels);
    }

    /// Record a connection being established.
    #[inline]
    pub fn add_connection(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.connections.inc(labels);
    }

    /// Record a connection being closed.
    #[inline]
    pub fn remove_connection(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.connections.add(labels, -1);
    }

    /// Record a connection being checked out from a pool (in-use).
    #[inline]
    pub fn add_connection_in_use(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.connections_in_use.inc(labels);
    }

    /// Record a connection being returned to the pool (idle).
    #[inline]
    pub fn remove_connection_in_use(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.connections_in_use.add(labels, -1);
    }

    /// Get total connections currently in use (checked out).
    pub fn get_connections_in_use(&self) -> i64 {
        self.core.connections_in_use.sum_all()
    }

    /// Snapshot in-use connection counts grouped by dynamic labels.
    pub fn snapshot_connections_in_use(&self) -> Vec<(DynamicLabelSet, i64)> {
        self.core.connections_in_use.snapshot()
    }

    /// Record RBAC PG sync lag.
    #[inline]
    pub fn set_rbac_pg_sync_lag(&self, lag: usize, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.rbac_pg_sync_lag.set(labels, lag as f64);
    }

    /// Record token usage for an LLM request.
    ///
    /// Attributes include the owning organization UUID for analytics scoping.
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn record_llm_usage(
        &self,
        prompt_tokens: Option<u64>,
        completion_tokens: Option<u64>,
        total_tokens: Option<u64>,
        provider: &str,
        model: &str,
        endpoint_uuid: Option<&EndpointUuid>,
        org_uuid: &OrganizationUuid,
        tool_used: bool,
        streaming: bool,
    ) {
        // Build labels directly as string references
        let tool_used_str = if tool_used { "true" } else { "false" };
        let streaming_str = if streaming { "true" } else { "false" };

        // Create owned strings for optional values
        let org_owned = org_uuid.uuid().to_string();
        let endpoint_owned = endpoint_uuid.map(|endpoint_uuid| endpoint_uuid.uuid().to_string());

        // Build labels array
        let mut labels_vec: Vec<(&str, &str)> = vec![
            ("org_uuid", org_owned.as_str()),
            ("provider", provider),
            ("model", model),
            ("tool_used", tool_used_str),
            ("streaming", streaming_str),
        ];
        if let Some(ref ep) = endpoint_owned {
            labels_vec.push(("endpoint_uuid", ep.as_str()));
        }

        let labels = labels_vec.as_slice();

        self.core.llm_requests.inc(labels);

        if let Some(value) = prompt_tokens {
            self.core.llm_prompt_tokens.add(labels, value as isize);
        }
        if let Some(value) = completion_tokens {
            self.core.llm_completion_tokens.add(labels, value as isize);
        }
        if let Some(value) = total_tokens {
            self.core.llm_total_tokens.add(labels, value as isize);
            self.core.llm_total_tokens_distribution.record(labels, value);
        }
    }

    /// Record gateway-level LLM HTTP request telemetry.
    #[inline]
    pub fn record_llm_gateway_request(&self, duration_us: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.llm_gateway_requests.inc(labels);
        self.core.llm_gateway_request_duration_microseconds.record(labels, duration_us);
    }

    /// Record one gateway-level LLM HTTP error outcome.
    #[inline]
    pub fn record_llm_gateway_error(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.llm_gateway_errors.inc(labels);
    }

    /// Record time to first streaming chunk for LLM gateway requests.
    #[inline]
    pub fn record_llm_gateway_time_to_first_chunk(&self, duration_us: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.llm_gateway_time_to_first_chunk_microseconds.record(labels, duration_us);
    }

    /// Record time between streaming output chunks for LLM gateway requests.
    #[inline]
    pub fn record_llm_gateway_time_per_output_chunk(&self, duration_us: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.llm_gateway_time_per_output_chunk_microseconds.record(labels, duration_us);
    }

    /// Record streaming chunk volume for LLM gateway responses.
    #[inline]
    pub fn record_llm_gateway_stream_chunks(&self, chunks: u64, labels: &[(&str, &str)]) {
        if chunks > 0 {
            let labels = labels_with_org_uuid(labels);
            let labels = labels.as_ref();
            self.core.llm_gateway_stream_chunks.add(labels, chunks as isize);
        }
    }

    /// Record provider-specific token detail counters when they are present.
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn record_llm_token_details(
        &self,
        cached_prompt_tokens: Option<u64>,
        prompt_audio_tokens: Option<u64>,
        reasoning_completion_tokens: Option<u64>,
        completion_audio_tokens: Option<u64>,
        provider: &str,
        model: &str,
        endpoint_uuid: Option<&EndpointUuid>,
        org_uuid: &OrganizationUuid,
        tool_used: bool,
        streaming: bool,
    ) {
        let tool_used_str = if tool_used { "true" } else { "false" };
        let streaming_str = if streaming { "true" } else { "false" };
        let org_owned = org_uuid.uuid().to_string();
        let endpoint_owned = endpoint_uuid.map(|endpoint_uuid| endpoint_uuid.uuid().to_string());

        let mut labels_vec: Vec<(&str, &str)> = vec![
            ("org_uuid", org_owned.as_str()),
            ("provider", provider),
            ("model", model),
            ("tool_used", tool_used_str),
            ("streaming", streaming_str),
        ];
        if let Some(ref ep) = endpoint_owned {
            labels_vec.push(("endpoint_uuid", ep.as_str()));
        }

        let labels = labels_vec.as_slice();

        if let Some(value) = cached_prompt_tokens {
            self.core.llm_cached_prompt_tokens.add(labels, value as isize);
        }
        if let Some(value) = prompt_audio_tokens {
            self.core.llm_prompt_audio_tokens.add(labels, value as isize);
        }
        if let Some(value) = reasoning_completion_tokens {
            self.core.llm_reasoning_completion_tokens.add(labels, value as isize);
        }
        if let Some(value) = completion_audio_tokens {
            self.core.llm_completion_audio_tokens.add(labels, value as isize);
        }
    }

    /// Record one entitlement evaluation result.
    #[inline]
    pub fn record_entitlement_status(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.entitlement_status_total.inc(labels);
    }

    /// Record one entitlement evaluation failure.
    #[inline]
    pub fn record_entitlement_eval_failure(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.entitlement_eval_failures_total.inc(labels);
    }

    // =========================================================================
    // Orchestration recording methods
    // =========================================================================

    /// Record the start of an orchestration run.
    pub fn record_orchestration_start(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_runs.inc(labels);
        self.core.orchestration_active.inc(labels);
    }

    /// Record the end of an orchestration run (success or failure).
    pub fn record_orchestration_end(&self, duration_ms: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_active.dec(labels);
        self.core.orchestration_total_duration.record(labels, duration_ms);
    }

    /// Record a resumed orchestration run.
    pub fn record_orchestration_resume(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_resumes.inc(labels);
        self.core.orchestration_active.inc(labels);
    }

    /// Record orchestration planning latency.
    pub fn record_orchestration_plan_duration(&self, duration_ms: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_plan_duration.record(labels, duration_ms);
    }

    /// Record a sub-task starting.
    pub fn record_subtask_started(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_subtasks_started.inc(labels);
    }

    /// Record a sub-task completing successfully.
    pub fn record_subtask_completed(&self, duration_ms: u64, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_subtasks_completed.inc(labels);
        self.core.orchestration_subtask_duration.record(labels, duration_ms);
    }

    /// Record a sub-task failing.
    pub fn record_subtask_failed(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_subtasks_failed.inc(labels);
    }

    /// Record a sub-task retry.
    pub fn record_subtask_retry(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_subtask_retries.inc(labels);
    }

    /// Record that the planner requested feedback.
    pub fn record_orchestration_feedback(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.core.orchestration_feedback_requests.inc(labels);
    }

    // =========================================================================
    // Snapshot methods for reading current values
    // =========================================================================

    /// Get current active request count (sum across all label sets).
    pub fn get_active_requests(&self) -> i64 {
        self.core.active_requests.sum_all()
    }

    /// Get total request count (sum across all label sets).
    pub fn get_request_count(&self) -> u64 {
        self.core.request_count.sum_all() as u64
    }

    /// Get total response count (sum across all label sets).
    pub fn get_response_count(&self) -> u64 {
        self.core.response_count.sum_all() as u64
    }

    /// Get success count (sum across all label sets).
    pub fn get_success_count(&self) -> u64 {
        self.core.success_count.sum_all() as u64
    }

    /// Get error count (sum across all label sets).
    pub fn get_error_count(&self) -> u64 {
        self.core.error_count.sum_all() as u64
    }

    /// Get total uploaded bytes (sum across all label sets).
    pub fn get_upload_bytes(&self) -> u64 {
        self.core.upload_byte_count.sum_all() as u64
    }

    /// Get total downloaded bytes (sum across all label sets).
    pub fn get_download_bytes(&self) -> u64 {
        self.core.download_byte_count.sum_all() as u64
    }

    /// Get unique user count.
    pub fn get_unique_users(&self) -> u64 {
        self.core.unique_users.sum_all() as u64
    }

    /// Get login count (sum across all label sets).
    pub fn get_logins(&self) -> u64 {
        self.core.logins.sum_all() as u64
    }

    /// Get local cache hit count (sum across all label sets).
    pub fn get_local_cache_hits(&self) -> u64 {
        self.core.local_cache_hits.sum_all() as u64
    }

    /// Get local cache miss count (sum across all label sets).
    pub fn get_local_cache_misses(&self) -> u64 {
        self.core.local_cache_misses.sum_all() as u64
    }

    /// Get Redis cache hit count (sum across all label sets).
    pub fn get_redis_cache_hits(&self) -> u64 {
        self.core.redis_cache_hits.sum_all() as u64
    }

    /// Get Redis cache miss count (sum across all label sets).
    pub fn get_redis_cache_misses(&self) -> u64 {
        self.core.redis_cache_misses.sum_all() as u64
    }

    /// Get active connection count (sum across all label sets).
    pub fn get_connections(&self) -> i64 {
        self.core.connections.sum_all()
    }

    /// Get LLM request count (sum across all label sets).
    pub fn get_llm_requests(&self) -> u64 {
        self.core.llm_requests.sum_all() as u64
    }

    /// Get total LLM prompt tokens (sum across all label sets).
    pub fn get_llm_prompt_tokens(&self) -> u64 {
        self.core.llm_prompt_tokens.sum_all() as u64
    }

    /// Get total LLM completion tokens (sum across all label sets).
    pub fn get_llm_completion_tokens(&self) -> u64 {
        self.core.llm_completion_tokens.sum_all() as u64
    }

    /// Get total LLM tokens (sum across all label sets).
    pub fn get_llm_total_tokens(&self) -> u64 {
        self.core.llm_total_tokens.sum_all() as u64
    }

    /// Snapshot current cumulative LLM total tokens grouped by dynamic labels.
    pub fn snapshot_llm_total_tokens(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.core.llm_total_tokens.snapshot()
    }

    /// Snapshot current cumulative LLM prompt tokens grouped by dynamic labels.
    pub fn snapshot_llm_prompt_tokens(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.core.llm_prompt_tokens.snapshot()
    }

    /// Snapshot current cumulative LLM completion tokens grouped by dynamic labels.
    pub fn snapshot_llm_completion_tokens(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.core.llm_completion_tokens.snapshot()
    }

    /// Snapshot current cumulative uploaded bytes grouped by dynamic labels.
    pub fn snapshot_upload_bytes(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.core.upload_byte_count.snapshot()
    }

    /// Snapshot current cumulative downloaded bytes grouped by dynamic labels.
    pub fn snapshot_download_bytes(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.core.download_byte_count.snapshot()
    }

    /// Snapshot current endpoint connection counts grouped by dynamic labels (db_type).
    pub fn snapshot_connections(&self) -> Vec<(DynamicLabelSet, i64)> {
        self.core.connections.snapshot()
    }

    /// Snapshot current active request counts grouped by dynamic labels.
    pub fn snapshot_active_requests(&self) -> Vec<(DynamicLabelSet, i64)> {
        self.core.active_requests.snapshot()
    }

    /// Snapshot cumulative request counts grouped by dynamic labels.
    pub fn snapshot_request_count(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.core.request_count.snapshot()
    }

    /// Get eden duration distribution for direct access.
    pub fn eden_duration_distribution(&self) -> &DynamicDistribution {
        &self.core.eden_duration
    }

    /// Get total duration distribution for direct access.
    pub fn total_duration_histogram(&self) -> &DynamicDistribution {
        &self.core.total_duration
    }

    /// Get total series cardinality across all dynamic Eden metrics.
    pub fn cardinality(&self) -> usize {
        self.core.active_requests.cardinality()
            + self.core.connections.cardinality()
            + self.core.connections_in_use.cardinality()
            + self.core.request_count.cardinality()
            + self.core.response_count.cardinality()
            + self.core.success_count.cardinality()
            + self.core.error_count.cardinality()
            + self.core.upload_byte_count.cardinality()
            + self.core.download_byte_count.cardinality()
            + self.core.logins.cardinality()
            + self.core.local_cache_hits.cardinality()
            + self.core.local_cache_misses.cardinality()
            + self.core.redis_cache_hits.cardinality()
            + self.core.redis_cache_misses.cardinality()
            + self.core.llm_requests.cardinality()
            + self.core.llm_prompt_tokens.cardinality()
            + self.core.llm_completion_tokens.cardinality()
            + self.core.llm_total_tokens.cardinality()
            + self.core.unique_users.cardinality()
            + self.core.total_duration.cardinality()
            + self.core.eden_duration.cardinality()
            + self.core.upload_byte_distribution.cardinality()
            + self.core.download_byte_distribution.cardinality()
            + self.core.llm_total_tokens_distribution.cardinality()
            + self.core.rbac_pg_sync_lag.cardinality()
            + self.core.entitlement_status_total.cardinality()
            + self.core.entitlement_eval_failures_total.cardinality()
    }

    /// Evict stale series from all dynamic metrics.
    ///
    /// Series that haven't been accessed for `max_staleness` cycles are removed.
    /// Returns total number of series evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        // Gauges
        evicted += self.core.active_requests.evict_stale(max_staleness);
        evicted += self.core.connections.evict_stale(max_staleness);
        evicted += self.core.connections_in_use.evict_stale(max_staleness);
        evicted += self.core.rbac_pg_sync_lag.evict_stale(max_staleness);
        // Counters
        evicted += self.core.request_count.evict_stale(max_staleness);
        evicted += self.core.response_count.evict_stale(max_staleness);
        evicted += self.core.success_count.evict_stale(max_staleness);
        evicted += self.core.error_count.evict_stale(max_staleness);
        evicted += self.core.upload_byte_count.evict_stale(max_staleness);
        evicted += self.core.download_byte_count.evict_stale(max_staleness);
        evicted += self.core.unique_users.evict_stale(max_staleness);
        evicted += self.core.logins.evict_stale(max_staleness);
        evicted += self.core.local_cache_hits.evict_stale(max_staleness);
        evicted += self.core.local_cache_misses.evict_stale(max_staleness);
        evicted += self.core.redis_cache_hits.evict_stale(max_staleness);
        evicted += self.core.redis_cache_misses.evict_stale(max_staleness);
        evicted += self.core.llm_requests.evict_stale(max_staleness);
        evicted += self.core.llm_prompt_tokens.evict_stale(max_staleness);
        evicted += self.core.llm_completion_tokens.evict_stale(max_staleness);
        evicted += self.core.llm_total_tokens.evict_stale(max_staleness);
        evicted += self.core.entitlement_status_total.evict_stale(max_staleness);
        evicted += self.core.entitlement_eval_failures_total.evict_stale(max_staleness);
        // Distributions
        evicted += self.core.total_duration.evict_stale(max_staleness);
        evicted += self.core.eden_duration.evict_stale(max_staleness);
        evicted += self.core.upload_byte_distribution.evict_stale(max_staleness);
        evicted += self.core.download_byte_distribution.evict_stale(max_staleness);
        evicted += self.core.llm_total_tokens_distribution.evict_stale(max_staleness);
        evicted
    }
}

impl Default for EdenMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TelemetryDurations;

    #[tokio::test]
    async fn complete_request_uses_start_labels_for_active_request_decrement() {
        let metrics = EdenMetrics::new();
        let active_labels = [("org_uuid", "org-1"), ("endpoint_type", "redis")];
        let completion_labels = [("org_uuid", "org-1"), ("endpoint_type", "redis"), ("http_status", "200")];

        metrics.start_request(Username::default(), 12, &active_labels).await;
        let mut durations = TelemetryDurations::default();
        metrics.complete_request_with_active_labels(34, Utc::now(), &mut durations, false, &completion_labels, &active_labels);

        assert_eq!(metrics.get_active_requests(), 0);
        assert!(metrics.snapshot_active_requests().iter().all(|(_, value)| *value >= 0));
        assert_eq!(metrics.get_response_count(), 1);
        assert_eq!(metrics.get_success_count(), 1);
    }
}
