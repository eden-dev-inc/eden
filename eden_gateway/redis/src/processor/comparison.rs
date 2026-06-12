//! Redis response comparison helpers for migration validation.

use super::*;

pub(crate) struct RedisResponseComparison;

impl RedisResponseComparison {
    #[inline]
    pub(crate) fn responses_differ(old_resp: &[u8], new_resp: &[u8]) -> bool {
        old_resp != new_resp
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn record_mismatch(org_uuid: &str, interlay_id: &str, telemetry_wrapper: &mut TelemetryWrapper, ctx: &LogContext) {
        telemetry_wrapper.record(MetricEvent::ProxyError {
            org_uuid,
            interlay_uuid: interlay_id,
            error_type: "version_compare_mismatch",
        });
        log_warn!(
            ctx.clone(),
            "VERSION COMPARE MISMATCH: responses differ between endpoints",
            audience = LogAudience::Internal
        );
    }

    #[inline]
    pub(crate) fn resolve_version_compare_result(old_result: ResultEP<Bytes>, new_result: ResultEP<Bytes>) -> ResultEP<Option<Bytes>> {
        match (old_result, new_result) {
            (_, Ok(new_resp)) => Ok(Some(new_resp)),
            (Ok(old_resp), Err(_)) => Ok(Some(old_resp)),
            (Err(e), Err(_)) => Err(e),
        }
    }
}
