use crate::comm::organization::get::get_organization;
use crate::error_handling;
use crate::middleware::org_rate_limit::{BANDWIDTH_BUCKET_WINDOW_SECS, TOKEN_BUCKET_WINDOW_SECS};
use crate::rate_limiter::token_bucket_key;
use actix_web::{HttpRequest, Responder, web};
use chrono::Utc;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::lib::ShardCache;
use eden_core::auth::ParsedJwt;
use eden_core::format::CacheObjectType;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::response::EdenResponse;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

struct BucketSnapshot {
    stored_t: f64,
    stored_ts: i64,
    consumed: u64,
}

async fn cache_bucket_snapshot(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    key: &str,
    limit: u64,
    now_ts: i64,
) -> BucketSnapshot {
    match database.internal_cache().rate_bucket_get(key).await.ok().flatten() {
        Some(state) => BucketSnapshot {
            stored_t: state.tokens,
            stored_ts: state.last,
            consumed: state.consumed,
        },
        None => BucketSnapshot { stored_t: limit as f64, stored_ts: now_ts, consumed: 0 },
    }
}

/// Get current rate-limit status for the authenticated organization.
///
/// Reads the live internal-cache token-bucket state for all four metrics
/// (`bandwidth_ingress`, `bandwidth_egress`, `token_ingress`, `token_egress`) and
/// returns a snapshot of current usage, limits, and reset times.
/// NOTE: We'll probably want more buckets pretty soon, e.g. some queries may entail
/// a lot of compute but not send or receive much data
///
/// # Refill applied in-response
///
/// Remaining values are **not** read raw from storage. The same continuous refill formula
/// used by the enforcement middleware is applied before returning, so clients always see
/// a value consistent with what the next request would see:
/// ```text
/// remaining = min(limit, stored_t + elapsed × (limit / window_secs))
/// ```
///
/// # Reset times
///
/// `resets_at` is the RFC3339 timestamp at which the bucket would reach full capacity
/// assuming no further requests consume tokens. It is computed as:
/// ```text
/// secs_to_full = ceil((limit - remaining) × window_secs / limit)
/// ```
/// When the bucket is already full, `resets_at` equals the current time.
///
/// # No-limit fields
///
/// Fields that have no configured limit are omitted from the response
/// (`#[serde(skip_serializing_if = "Option::is_none")]`). For token metrics with no
/// limit `limit_tokens` and `remaining_tokens` are both 0; clients **must** treat
/// `limit_tokens == 0` as "unlimited", not as an exhausted quota.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Organization"],
    path="/organizations/rate-limit",
    operation_id = "get_organization_rate_limit",
    responses((status = OK, body = EdenResponse<RateLimitResponse>))
)]
pub async fn get_rate_limit(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let org = get_organization(
        &database,
        &CacheObjectType::new(Some(OrganizationCacheUuid::new(None, auth.org_uuid().clone())), None),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let settings = org.rate_limit_settings();

    let enabled = settings.map(|s| s.enabled).unwrap_or(false);

    let now = Utc::now();
    let now_ts = now.timestamp();
    let org_uuid_str = auth.org_uuid().to_string();

    // Read bandwidth usage from internal token buckets.
    //
    // `consumed` – cumulative bytes consumed in the current bucket window (written by
    //   enforcement on every successful deduction). This is the authoritative "used" value shown
    //   in the UI: it accumulates as requests arrive and resets naturally when the cache entry
    //   expires (i.e. after one full window with no requests).  Unlike `limit - remaining`,
    //   it does not decrease as the bucket refills, so small KB-scale requests remain visible
    //   even against MB-scale limits with a short refill window.
    //
    // `t`/`ts` – rate-limiting state used only to compute `resets_at` (time until the bucket
    //   reaches full capacity again, i.e. when the next burst window opens).
    let (ingress_used, ingress_remaining, ingress_resets_at, egress_used, egress_remaining, egress_resets_at) = {
        let (ing_used, ing_remaining, ing_resets) = if let Some(limit) = settings.and_then(|s| s.bandwidth_ingress_limit_bytes) {
            let key = token_bucket_key(&org_uuid_str, "bandwidth_ingress");
            let bucket = cache_bucket_snapshot(database.as_ref(), &key, limit, now_ts).await;
            let stored_t = bucket.stored_t;
            let stored_ts = bucket.stored_ts;
            let used = bucket.consumed;
            let elapsed = (now_ts - stored_ts).max(0) as f64;
            let remaining = (stored_t + elapsed * limit as f64 / BANDWIDTH_BUCKET_WINDOW_SECS as f64).min(limit as f64).max(0.0);
            let secs_to_full = if remaining >= limit as f64 {
                0i64
            } else {
                ((limit as f64 - remaining) * BANDWIDTH_BUCKET_WINDOW_SECS as f64 / limit as f64).ceil() as i64
            };
            let resets = (now + chrono::Duration::seconds(secs_to_full)).to_rfc3339();
            (used, remaining.floor() as u64, resets)
        } else {
            (0, 0, now.to_rfc3339())
        };
        let (egr_used, egr_remaining, egr_resets) = if let Some(limit) = settings.and_then(|s| s.bandwidth_egress_limit_bytes) {
            let key = token_bucket_key(&org_uuid_str, "bandwidth_egress");
            let bucket = cache_bucket_snapshot(database.as_ref(), &key, limit, now_ts).await;
            let stored_t = bucket.stored_t;
            let stored_ts = bucket.stored_ts;
            let used = bucket.consumed;
            let elapsed = (now_ts - stored_ts).max(0) as f64;
            let remaining = (stored_t + elapsed * limit as f64 / BANDWIDTH_BUCKET_WINDOW_SECS as f64).min(limit as f64).max(0.0);
            let secs_to_full = if remaining >= limit as f64 {
                0i64
            } else {
                ((limit as f64 - remaining) * BANDWIDTH_BUCKET_WINDOW_SECS as f64 / limit as f64).ceil() as i64
            };
            let resets = (now + chrono::Duration::seconds(secs_to_full)).to_rfc3339();
            (used, remaining.floor() as u64, resets)
        } else {
            (0, 0, now.to_rfc3339())
        };
        (ing_used, ing_remaining, ing_resets, egr_used, egr_remaining, egr_resets)
    };

    let bandwidth_ingress = settings.and_then(|s| {
        s.bandwidth_ingress_limit_bytes.map(|limit| BandwidthStatus {
            used_bytes: ingress_used,
            limit_bytes: limit,
            remaining_bytes: ingress_remaining.min(limit),
            resets_at: ingress_resets_at,
        })
    });

    let bandwidth_egress = settings.and_then(|s| {
        s.bandwidth_egress_limit_bytes.map(|limit| BandwidthStatus {
            used_bytes: egress_used,
            limit_bytes: limit,
            remaining_bytes: egress_remaining.min(limit),
            resets_at: egress_resets_at,
        })
    });

    // Read current token bucket state from the internal cache to derive remaining/used for display.
    // Applies the continuous refill formula so the returned values are always up to date.
    // Also computes a stable resets_at anchored to stored_ts so that refreshing the
    // status endpoint does not reset the countdown (see: resets_at should not drift
    // to "now + window" every time a request barely squeezes through the partial refill).
    let (token_ingress_remaining, token_ingress_resets_at, token_egress_remaining, token_egress_resets_at) = {
        let (ing_rem, ing_resets) = if let Some(limit) = settings.and_then(|s| s.token_ingress_limit) {
            let key = token_bucket_key(&org_uuid_str, "token_ingress");
            let bucket = cache_bucket_snapshot(database.as_ref(), &key, limit, now_ts).await;
            let stored_t = bucket.stored_t;
            let stored_ts = bucket.stored_ts;
            let elapsed = (now_ts - stored_ts).max(0) as f64;
            let remaining = ((stored_t + elapsed * limit as f64 / TOKEN_BUCKET_WINDOW_SECS as f64).min(limit as f64)).floor() as u64;
            let secs = ((limit as f64 - stored_t.min(limit as f64)) * TOKEN_BUCKET_WINDOW_SECS as f64 / limit as f64).ceil() as i64;
            let resets_at = chrono::DateTime::from_timestamp(stored_ts + secs.max(0), 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
            (remaining, Some(resets_at))
        } else {
            (0, None)
        };
        let (egr_rem, egr_resets) = if let Some(limit) = settings.and_then(|s| s.token_egress_limit) {
            let key = token_bucket_key(&org_uuid_str, "token_egress");
            let bucket = cache_bucket_snapshot(database.as_ref(), &key, limit, now_ts).await;
            let stored_t = bucket.stored_t;
            let stored_ts = bucket.stored_ts;
            let elapsed = (now_ts - stored_ts).max(0) as f64;
            let remaining = ((stored_t + elapsed * limit as f64 / TOKEN_BUCKET_WINDOW_SECS as f64).min(limit as f64)).floor() as u64;
            let secs = ((limit as f64 - stored_t.min(limit as f64)) * TOKEN_BUCKET_WINDOW_SECS as f64 / limit as f64).ceil() as i64;
            let resets_at = chrono::DateTime::from_timestamp(stored_ts + secs.max(0), 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
            (remaining, Some(resets_at))
        } else {
            (0, None)
        };
        (ing_rem, ing_resets, egr_rem, egr_resets)
    };

    // Return a TokenStatus for each metric.  When no limit is configured,
    // limit_tokens and remaining_tokens are both 0; clients treat limit_tokens == 0
    // as "unlimited" rather than as an exhausted quota.
    let token_ingress = Some(compute_token_status(
        token_ingress_remaining,
        settings.and_then(|s| s.token_ingress_limit),
        token_ingress_resets_at,
    ));

    let token_egress = Some(compute_token_status(
        token_egress_remaining,
        settings.and_then(|s| s.token_egress_limit),
        token_egress_resets_at,
    ));

    EdenResponse::response(RateLimitResponse {
        enabled,
        bandwidth_ingress,
        bandwidth_egress,
        token_ingress,
        token_egress,
    })
    .into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RateLimitResponse {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_ingress: Option<BandwidthStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_egress: Option<BandwidthStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_ingress: Option<TokenStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_egress: Option<TokenStatus>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BandwidthStatus {
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub resets_at: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TokenStatus {
    pub used_tokens: u64,
    /// 0 indicates "no configured limit".
    pub limit_tokens: u64,

    /// When `limit_tokens` is zero this field will also be zero.  Clients should
    /// treat a zero limit as unlimited rather than as an exhausted quota.
    pub remaining_tokens: u64,
    pub resets_at: String,
}

/// Builds a [`TokenStatus`] snapshot from the **post-refill** remaining token count.
///
/// # Arguments
///
/// * `remaining` – tokens currently available in the bucket after applying the lazy
///   refill formula (caller is responsible for this calculation).
/// * `limit` – configured per-org limit, or `None`/`Some(0)` when unlimited.
///
/// # Behaviour when no limit is configured
///
/// Returns a zeroed `TokenStatus` with `limit_tokens = 0`. Clients must treat
/// `limit_tokens == 0` as "no limit" rather than an exhausted quota.
///
/// # `resets_at`
///
/// RFC3339 timestamp of when the bucket will next reach full capacity:
/// ```text
/// secs_to_full = ceil((limit - remaining) × TOKEN_BUCKET_WINDOW_SECS / limit)
/// ```
/// Set to the current time when the bucket is already full.
/// `resets_at_override`: when the caller has already read the cached bucket state it
/// should pass the pre-computed anchor-based timestamp here so the deadline is stable
/// across repeated calls. Pass `None` to fall back to the `now`-relative formula.
fn compute_token_status(remaining: u64, limit: Option<u64>, resets_at_override: Option<String>) -> TokenStatus {
    match limit {
        Some(l) if l > 0 => {
            let used = l.saturating_sub(remaining);
            let full_at = resets_at_override.unwrap_or_else(|| {
                let seconds_to_full = if remaining >= l {
                    0i64
                } else {
                    ((l - remaining) as f64 * TOKEN_BUCKET_WINDOW_SECS as f64 / l as f64).ceil() as i64
                };
                (chrono::Utc::now() + chrono::Duration::seconds(seconds_to_full)).to_rfc3339()
            });
            TokenStatus {
                used_tokens: used,
                limit_tokens: l,
                remaining_tokens: remaining,
                resets_at: full_at,
            }
        }
        _ => TokenStatus {
            used_tokens: 0,
            limit_tokens: 0,
            remaining_tokens: 0,
            resets_at: chrono::Utc::now().to_rfc3339(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_token_status_unlimited() {
        // When no limit is configured, all fields are 0.
        let status = compute_token_status(0, None, None);
        assert_eq!(status.used_tokens, 0);
        assert_eq!(status.limit_tokens, 0);
        assert_eq!(status.remaining_tokens, 0);
    }

    #[test]
    fn compute_token_status_with_limit() {
        // remaining=50, limit=100 → used=50, refills in 30 minutes (1800s).
        let status = compute_token_status(50, Some(100), None);
        assert_eq!(status.used_tokens, 50);
        assert_eq!(status.limit_tokens, 100);
        assert_eq!(status.remaining_tokens, 50);
    }

    #[test]
    fn compute_token_status_full_bucket() {
        let status = compute_token_status(100, Some(100), None);
        assert_eq!(status.used_tokens, 0);
        assert_eq!(status.remaining_tokens, 100);
    }
}
