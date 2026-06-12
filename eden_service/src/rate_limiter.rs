/// Redis key for a per-org token bucket state.
///
/// The bucket is used for pre-checks and implements a smooth refill rate.
/// We keep it separate from the fixed-window counter for visibility/compatibility.
///
/// Format: `rate_limit_bucket:{org_uuid}:{metric}`.
pub fn token_bucket_key(org_uuid: &str, metric: &str) -> String {
    format!("rate_limit_bucket:{org_uuid}:{metric}")
}
