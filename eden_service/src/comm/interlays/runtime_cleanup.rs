use crate::comm::interlays::shard::ShardRouter;
#[cfg(feature = "redis")]
use eden_core::format::cache_uuid::CacheUuid;
use eden_core::format::cache_uuid::InterlayCacheUuid;
use eden_core::telemetry::TelemetryWrapper;

pub(crate) async fn clear_interlay_runtime_resources(
    shard_router: &ShardRouter,
    interlay_uuid: &InterlayCacheUuid,
    cleanup_reason: &'static str,
    telemetry_wrapper: &TelemetryWrapper,
) {
    broadcast_interlay_runtime_cleanup(
        shard_router,
        interlay_uuid,
        cleanup_reason,
        InterlayRuntimeCleanupOperation::Clear,
        telemetry_wrapper,
    )
    .await;
}

pub(crate) async fn retire_interlay_runtime_resources(
    shard_router: &ShardRouter,
    interlay_uuid: &InterlayCacheUuid,
    cleanup_reason: &'static str,
    telemetry_wrapper: &TelemetryWrapper,
) {
    broadcast_interlay_runtime_cleanup(
        shard_router,
        interlay_uuid,
        cleanup_reason,
        InterlayRuntimeCleanupOperation::Retire,
        telemetry_wrapper,
    )
    .await;
}

#[derive(Clone, Copy)]
enum InterlayRuntimeCleanupOperation {
    Clear,
    Retire,
}

#[cfg(feature = "redis")]
impl InterlayRuntimeCleanupOperation {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Clear => "interlay_clear",
            Self::Retire => "interlay_retire",
        }
    }

    fn log_label(self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::Retire => "retire",
        }
    }
}

async fn broadcast_interlay_runtime_cleanup(
    shard_router: &ShardRouter,
    interlay_uuid: &InterlayCacheUuid,
    cleanup_reason: &'static str,
    operation: InterlayRuntimeCleanupOperation,
    telemetry_wrapper: &TelemetryWrapper,
) {
    #[cfg(not(feature = "redis"))]
    {
        let _ = (shard_router, interlay_uuid, cleanup_reason, operation, telemetry_wrapper);
    }

    #[cfg(feature = "redis")]
    {
        let interlay_uuid_label = interlay_uuid.uuid().to_string();
        let organization_uuid_label = interlay_uuid.org().map(|org| org.uuid().to_string());

        for shard in shard_router.shard_ids() {
            if let Err(err) = shard_router.dispatch(
                shard,
                Box::new(move || {
                    endpoint_core::redis_core::multiplex::clear_shard_multiplexers();
                }),
            ) {
                let shard_id = shard.index().to_string();
                record_interlay_cleanup_issue(
                    telemetry_wrapper,
                    operation,
                    organization_uuid_label.as_deref(),
                    interlay_uuid_label.as_str(),
                    shard_id.as_str(),
                    cleanup_reason,
                    "shard_dispatch_failed",
                );
                log::warn!(
                    "Interlay runtime cleanup dispatch failed: operation={} interlay_uuid={} shard_id={} cleanup_reason={} error={}",
                    operation.log_label(),
                    interlay_uuid_label,
                    shard.index(),
                    cleanup_reason,
                    err
                );
            }
        }
    }
}

#[cfg(feature = "redis")]
fn record_interlay_cleanup_issue(
    telemetry_wrapper: &TelemetryWrapper,
    operation: InterlayRuntimeCleanupOperation,
    org_uuid: Option<&str>,
    interlay_uuid: &str,
    shard_id: &str,
    cleanup_reason: &str,
    reason: &str,
) {
    let Some(org_uuid) = org_uuid else {
        return;
    };
    telemetry_wrapper.metrics().proxy().record_direct_state_update_dispatch_failure(&[
        ("org_uuid", org_uuid),
        ("operation", operation.metric_label()),
        ("interlay_uuid", interlay_uuid),
        ("shard_id", shard_id),
        ("cleanup_reason", cleanup_reason),
        ("reason", reason),
    ]);
}
