use crate::comm::interlays::shard::ShardRouter;
#[cfg(feature = "redis")]
use eden_core::format::EdenUuid;
use eden_core::format::{EndpointUuid, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;

#[cfg(feature = "redis")]
pub(crate) async fn evict_endpoint_runtime_resources(
    shard_router: &ShardRouter,
    org_uuid: &OrganizationUuid,
    endpoint_uuid: &EndpointUuid,
    cleanup_reason: &'static str,
    telemetry_wrapper: &TelemetryWrapper,
) {
    let endpoint_uuid_label = endpoint_uuid.uuid().to_string();
    let organization_uuid_label = org_uuid.uuid().to_string();

    for shard in shard_router.shard_ids() {
        let endpoint_label = endpoint_uuid_label.clone();
        if let Err(err) = shard_router.dispatch(
            shard,
            Box::new(move || {
                endpoint_core::redis_core::multiplex::shard_multiplexer_evict(endpoint_label.as_str());
            }),
        ) {
            let shard_id = shard.index().to_string();
            record_endpoint_evict_issue(
                telemetry_wrapper,
                organization_uuid_label.as_str(),
                endpoint_uuid_label.as_str(),
                shard_id.as_str(),
                cleanup_reason,
                "shard_dispatch_failed",
            );
            log::warn!(
                "Endpoint runtime cleanup dispatch failed: endpoint_uuid={} shard_id={} cleanup_reason={} error={}",
                endpoint_uuid_label,
                shard.index(),
                cleanup_reason,
                err
            );
        }
    }
}

#[cfg(not(feature = "redis"))]
pub(crate) async fn evict_endpoint_runtime_resources(
    _shard_router: &ShardRouter,
    _org_uuid: &OrganizationUuid,
    _endpoint_uuid: &EndpointUuid,
    _cleanup_reason: &'static str,
    _telemetry_wrapper: &TelemetryWrapper,
) {
}

#[cfg(feature = "redis")]
fn record_endpoint_evict_issue(
    telemetry_wrapper: &TelemetryWrapper,
    org_uuid: &str,
    endpoint_uuid: &str,
    shard_id: &str,
    cleanup_reason: &str,
    reason: &str,
) {
    telemetry_wrapper.metrics().proxy().record_direct_state_update_dispatch_failure(&[
        ("org_uuid", org_uuid),
        ("operation", "endpoint_evict"),
        ("endpoint_uuid", endpoint_uuid),
        ("shard_id", shard_id),
        ("cleanup_reason", cleanup_reason),
        ("reason", reason),
    ]);
}
