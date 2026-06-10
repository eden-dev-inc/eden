#![cfg_attr(test, allow(clippy::unwrap_used))]
pub(crate) mod aof;
pub mod ingress;
pub mod processor;
pub(crate) mod psync;
pub(crate) mod replication;
pub(crate) mod resp_scan;
pub mod response;
pub(crate) mod wire_utils;

pub use ingress::RedisIngressBatch;
pub use processor::cluster;
pub use processor::{RedisProtocolProcessor, RedisStreamingProcessor};

#[doc(hidden)]
pub mod validation {
    use bytes::Bytes;
    use endpoints::endpoint::ep_redis::api::{RedisApi, RedisJsonValue};
    use tokio::io::DuplexStream;
    use tokio::sync::mpsc::UnboundedReceiver;

    pub use crate::replication::ReplicationManager;

    pub fn scan_resp_elements_count(buf: &[u8], cmd_count: usize) -> Option<usize> {
        let elements = crate::resp_scan::RespScanner::scan(buf, cmd_count)?;
        let _has_metadata = elements
            .iter()
            .any(|element| element.len == 0 || element.is_error || element.is_nil_array || element.error_kind.is_some());
        Some(elements.len())
    }

    pub fn has_ttl_flag_for_validation(cmd_upper: &str, args: &[RedisJsonValue]) -> bool {
        crate::wire_utils::RedisTtl::has_ttl_flag(cmd_upper, args)
    }

    pub fn is_resp_null_for_validation(data: &[u8]) -> bool {
        crate::processor::RedisWire::is_resp_null(data)
    }

    pub fn response_contains_redis_error_for_validation(resp: &[u8]) -> bool {
        crate::processor::RedisWire::response_contains_redis_error(resp)
    }

    pub fn command_dispatch_uses_pinned_conn_for_validation(has_policy_override: bool, has_pinned_connection: bool) -> bool {
        matches!(
            crate::processor::RedisDispatch::command_path(has_policy_override, has_pinned_connection),
            crate::processor::CommandDispatchPath::PinnedConnection
        )
    }

    pub fn command_dispatch_uses_routed_conn_for_validation(has_policy_override: bool, has_pinned_connection: bool) -> bool {
        matches!(
            crate::processor::RedisDispatch::command_path(has_policy_override, has_pinned_connection),
            crate::processor::CommandDispatchPath::RoutedConnection
        )
    }

    pub fn command_has_explicit_local_state_handling_for_validation(command: RedisApi) -> bool {
        matches!(
            crate::processor::RedisDispatch::pre_dispatch_handling(&command),
            crate::processor::PreDispatchHandling::ExplicitLocalState
        )
    }

    pub fn should_capture_replication_bytes_for_validation(
        is_write: bool,
        was_policy_blocked: bool,
        has_replication_manager: bool,
        allow_replication_stream: bool,
    ) -> bool {
        crate::processor::RedisDispatch::should_capture_replication_bytes(
            is_write,
            was_policy_blocked,
            has_replication_manager,
            allow_replication_stream,
        )
    }

    pub fn responses_differ_for_validation(old_resp: &[u8], new_resp: &[u8]) -> bool {
        crate::processor::RedisResponseComparison::responses_differ(old_resp, new_resp)
    }

    pub fn version_compare_prefers_new_for_validation(old_resp: Bytes, new_resp: Bytes) -> bool {
        crate::processor::RedisResponseComparison::resolve_version_compare_result(Ok(old_resp), Ok(new_resp.clone()))
            .ok()
            .flatten()
            .is_some_and(|selected| selected == new_resp)
    }

    pub fn client_visible_response_slots_for_validation(results: &[eden_core::error::ResultEP<Option<Bytes>>]) -> usize {
        results.iter().map(crate::processor::RedisWire::client_visible_response_slots).sum()
    }

    pub fn render_client_response_bytes_for_validation(results: &[eden_core::error::ResultEP<Option<Bytes>>]) -> Bytes {
        crate::processor::RedisWire::render_client_response_bytes(results)
    }

    pub fn request_buffer_retention_for_validation(chunks: &[Bytes]) -> (usize, usize) {
        crate::processor::RedisWire::measure_request_buffer_retention(chunks)
    }

    pub fn format_resp_error_line_for_validation(message: &str) -> Bytes {
        crate::processor::RedisWire::format_resp_error_line(message)
    }

    pub fn count_resp_line_terminators_for_validation(frame: &[u8]) -> usize {
        crate::processor::RedisWire::count_resp_line_terminators(frame)
    }

    pub fn current_pipeline_request_bytes_for_validation(bytes_read: u64, cmd_count: u64) -> u32 {
        crate::processor::RedisPipelineMetrics::request_bytes(bytes_read, cmd_count)
    }

    pub fn current_pipeline_response_bytes_for_validation(total_bytes_written: u64, cmd_count: u64) -> u32 {
        crate::processor::RedisPipelineMetrics::response_bytes(total_bytes_written, cmd_count)
    }

    pub fn current_pipeline_per_command_latency_us_for_validation(duration_us: u64, cmd_count: u64) -> u64 {
        crate::processor::RedisPipelineMetrics::per_command_latency_us(duration_us, cmd_count)
    }

    pub fn current_pipeline_marks_slow_for_validation(duration_us: u64, cmd_count: u64, slow_threshold_us: u64) -> bool {
        crate::processor::RedisPipelineMetrics::marks_slow(duration_us, cmd_count, slow_threshold_us)
    }

    pub async fn queue_conflict_timeout_hits_before_delay_for_validation(max_timeout_ms: u64, ready_after: std::time::Duration) -> bool {
        tokio::time::timeout(crate::processor::RedisPipelineMetrics::queue_conflict_timeout_duration(max_timeout_ms), async {
            tokio::task::yield_now().await;
            tokio::time::sleep(ready_after).await;
        })
        .await
        .is_err()
    }

    pub fn routing_refresh_stales_after_cache_mutation_for_validation(remove_after_refresh: bool, post_refresh_mutations: usize) -> bool {
        crate::processor::RoutingRuntime::refresh_snapshot_stales_after_cache_mutation(remove_after_refresh, post_refresh_mutations)
    }

    pub async fn replication_connection_handler_on_duplex_for_validation(
        stream: DuplexStream,
        receiver: UnboundedReceiver<Bytes>,
    ) -> Result<(), String> {
        crate::replication::replication_connection_handler_on_stream(stream, receiver).await.map_err(|err| err.to_string())
    }
}

use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContext;
use endpoints::endpoint::ep_redis::protocol::decoder::RedisCommandArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PolicyEnforcementMode {
    Observe,
    Warn,
    Block,
}
impl PolicyEnforcementMode {
    fn from_config() -> Self {
        match eden_config::features().policy_enforcement_mode {
            eden_config::PolicyMode::Observe => Self::Observe,
            eden_config::PolicyMode::Warn => Self::Warn,
            eden_config::PolicyMode::Block => Self::Block,
        }
    }
}
pub(crate) fn policy_enforcement_mode() -> PolicyEnforcementMode {
    PolicyEnforcementMode::from_config()
}

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub(crate) fn policy_override_from_guard(
    _ctx: &LogContext,
    _parsed: &RedisCommandArgs,
    _mode: PolicyEnforcementMode,
    _organization_uuid: Option<&str>,
    _endpoint_uuid: Option<&eden_core::format::EndpointUuid>,
    _telemetry_wrapper: &mut TelemetryWrapper,
) -> Option<bytes::Bytes> {
    None
}
