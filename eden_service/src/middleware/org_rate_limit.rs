#[cfg(not(embedded_db))]
use actix_web::HttpResponse;
use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, HttpMessage};
use database::db::cache::CacheFunctions;
#[cfg(not(embedded_db))]
use database::db::lib::ShardCache;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
#[cfg(not(embedded_db))]
use database::internal_cache::{InternalCache, RateBucketState};
use eden_core::auth::ParsedJwt;
use eden_core::comm::NodeData;
#[cfg(not(embedded_db))]
use eden_core::error::ResultEP;
use eden_core::format::cache_id::OrganizationCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid};
use eden_core::telemetry::{AllMetrics, TelemetryDurations, TelemetryLabels, TelemetryWrapper};
use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
#[cfg(not(embedded_db))]
use std::sync::OnceLock;

#[cfg(not(embedded_db))]
use crate::rate_limiter::token_bucket_key;

#[cfg(not(embedded_db))]
static BUCKET_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[cfg(not(embedded_db))]
fn bucket_lock() -> &'static tokio::sync::Mutex<()> {
    BUCKET_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Refill window for **token** (LLM prompt/completion) buckets, in seconds (1 hour).
///
/// Tokens refill continuously at `limit / 3600` tokens per second up to the configured
/// per-org cap. A 1-hour window gives organisations a smooth hourly budget while still
/// protecting against runaway LLM usage that would be costly to reverse.
///
/// Token limits **fail-closed** inside a process: if the local internal cache is
/// unavailable when a token pre-check runs, the request is rejected with 503.
pub const TOKEN_BUCKET_WINDOW_SECS: u64 = 3600;

/// Refill window for **bandwidth** (ingress/egress byte) buckets, in seconds (5 minutes).
///
/// Bandwidth tokens refill continuously at `limit / 300` bytes per second. A shorter
/// window means a single large burst at the start of an hour only blocks an org for at
/// most 5 minutes rather than a full hour, trading off tighter burst control for faster
/// recovery.
///
/// Bandwidth limits **fail-open**: if the cache is unavailable the request is allowed through
/// (a warning is logged). This avoids making the gateway unavailable during cache outages
/// where there are no financial consequences comparable to unbounded LLM charges.
pub const BANDWIDTH_BUCKET_WINDOW_SECS: u64 = 300;

/// Per-org bandwidth and token rate limiting middleware.
///
/// Must be placed **before** the bearer auth middleware in the `.wrap()` chain
/// (i.e. added first) so that it runs **after** bearer auth has populated `ParsedJwt`.
///
/// # Pass-through conditions
///
/// The middleware skips rate limiting and calls the inner service unchanged when:
/// - No `ParsedJwt` is present in request extensions (unauthenticated route).
/// - Required `app_data` is missing.
/// - The org has no `RateLimitSettings` or they are disabled.
///
/// # Request lifecycle
///
/// **Step 1 – Bandwidth ingress** (fail-open):
/// Reads `Content-Length`, and if > 0, reserves that many bytes from the process-local
/// `bandwidth_ingress` cache bucket under an intra-process lock. Requests with no body
/// (GET, HEAD) are skipped so they do not inflate the ingress counter. On 429 the request
/// is rejected.
///
/// **Step 2 – Bandwidth egress** (fail-open):
/// Performs a **read-only** check of the `bandwidth_egress` bucket (no bytes reserved).
/// If the bucket is at zero the request is rejected with 429. The actual byte count is
/// deducted post-response (see steps 4a/4b). Stores `(key, limit)` for reconciliation.
///
/// **Step 3 – Post-response: egress bandwidth**:
/// Deducts the response `Content-Length` from the `bandwidth_egress` bucket. LLM routes
/// are excluded because their handlers reconcile actual token usage and emitted
/// response bytes directly after the response is constructed.
///
/// # Fail modes
///
/// These buckets are process-local. A multi-replica deployment needs a distributed
/// ShardCache/shardmap backend or another shared limiter to enforce fleet-wide quotas.
///
/// | Metric | cache unavailable |
/// |---|---|
/// | `bandwidth_ingress` | fail-open (warning logged) |
/// | `bandwidth_egress` | fail-open (warning logged) |
/// | `token_ingress` | reported by the visibility API |
/// | `token_egress` | reported by the visibility API |
pub async fn org_rate_limit<B: MessageBody>(req: ServiceRequest, next: Next<B>) -> Result<ServiceResponse<EitherBody<B>>, Error> {
    // Skip rate limiting for CORS preflight requests. OPTIONS preflights have no body
    // and must reach the actix-cors middleware to return the proper
    // Access-Control-Allow-Methods/Headers headers. Rate-limiting a preflight would
    // produce a 429 without those headers, causing the browser to treat the
    // originating fetch() as a network error ("Failed to fetch") instead of a 429.
    if req.method() == actix_web::http::Method::OPTIONS {
        return next.call(req).await.map(|r| r.map_into_left_body());
    }

    // Keep org-management and visibility routes reachable even when an org has exhausted
    // its bandwidth buckets; operators need these endpoints to inspect counters and raise
    // limits without waiting for refill.
    if is_rate_limit_management_route(req.path()) {
        return next.call(req).await.map(|r| r.map_into_left_body());
    }

    // 1. Extract ParsedJwt (set by bearer auth middleware).
    let jwt = req.extensions().get::<ParsedJwt>().cloned();
    let Some(jwt) = jwt else {
        return next.call(req).await.map(|r| r.map_into_left_body());
    };

    // 2. Get required app_data.
    // Clone the `Data<T>` so we don’t keep a borrow of `req` across `next.call(req)`.

    let Some(db) = req.app_data::<Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>>().cloned() else {
        return next.call(req).await.map(|r| r.map_into_left_body());
    };
    let Some(metrics) = req.app_data::<Data<AllMetrics>>().cloned() else {
        return next.call(req).await.map(|r| r.map_into_left_body());
    };

    // 3. Create minimal telemetry wrapper for cache lookup.
    let node_uuid = req.app_data::<Data<NodeData>>().map(|n| n.uuid().to_owned()).unwrap_or_else(eden_core::format::EdenNodeUuid::new_uuid);
    let mut telemetry =
        TelemetryWrapper::new(metrics.clone().into_inner(), TelemetryLabels::new(&node_uuid), TelemetryDurations::default());

    // 4. Look up org's RateLimitSettings from cache.
    let org_cache_uuid = OrganizationCacheUuid::new(None, jwt.org_uuid().clone());
    let cache_object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> = CacheObjectType::new(Some(org_cache_uuid), None);

    let Ok(org) = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_from_cache(&db, &cache_object, &mut telemetry)
    .await
    else {
        return next.call(req).await.map(|r| r.map_into_left_body());
    };

    // 5. If rate limiting is disabled or absent, pass through.
    let Some(settings) = org.rate_limit_settings() else {
        return next.call(req).await.map(|r| r.map_into_left_body());
    };
    if !settings.enabled {
        return next.call(req).await.map(|r| r.map_into_left_body());
    }

    #[cfg(embedded_db)]
    {
        req.extensions_mut().insert(settings.clone());
        return next.call(req).await.map(|r| r.map_into_left_body());
    }

    #[cfg(not(embedded_db))]
    {
        // 6. Management endpoints must remain reachable so operators can inspect or update limits
        //    even when quota is exhausted. Skip rate limiting entirely for org management routes.
        if is_rate_limit_management_route(req.path()) {
            return next.call(req).await.map(|r| r.map_into_left_body());
        }

        // 7. Bandwidth ingress pre-check (fail-open). Reserves actual content bytes from the
        //    cache token bucket. If the bucket is exhausted the request is rejected with 429.
        //    Cache errors allow the request through to avoid availability impact.
        let org_uuid_str = jwt.org_uuid().to_string();
        let cache = db.internal_cache().clone();

        let content_length =
            req.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        if let Some(bw_limit) = settings.bandwidth_ingress_limit_bytes {
            // Only deduct when the request has an actual body; zero-body requests (GET, HEAD)
            // do not consume bandwidth and should not inflate the ingress counter.
            if content_length > 0 {
                let bw_ingress_key = token_bucket_key(&org_uuid_str, "bandwidth_ingress");
                match reserve_token_ingress(&cache, &bw_ingress_key, bw_limit, content_length, BANDWIDTH_BUCKET_WINDOW_SECS).await {
                    Ok((false, current)) => {
                        let (req_parts, _) = req.into_parts();
                        return Ok(ServiceResponse::new(req_parts, rate_limit_response(bw_limit, current, "ingress")).map_into_right_body());
                    }
                    Ok((true, _)) => {}
                    Err(e) => {
                        tracing::warn!(
                            org_uuid = %org_uuid_str,
                            error = %e,
                            "Cache error during bandwidth_ingress check; allowing request"
                        );
                    }
                }
            }
        }

        // 8. Bandwidth egress pre-check (fail-open). Reads current bucket capacity without
        //    reserving; actual bytes are deducted post-response. Non-atomic, matching the
        //    eventual-consistency guarantee of the original design.
        let mut bw_egress_for_reconcile: Option<(String, u64)> = None;
        if let Some(bw_limit) = settings.bandwidth_egress_limit_bytes {
            let bw_egress_key = token_bucket_key(&org_uuid_str, "bandwidth_egress");
            match current_bucket_tokens(&cache, &bw_egress_key, bw_limit, BANDWIDTH_BUCKET_WINDOW_SECS).await {
                Ok(current) => {
                    if current <= 0.0 {
                        let (req_parts, _) = req.into_parts();
                        return Ok(ServiceResponse::new(req_parts, rate_limit_response(bw_limit, 0, "egress")).map_into_right_body());
                    }
                    bw_egress_for_reconcile = Some((bw_egress_key, bw_limit));
                }
                Err(e) => {
                    tracing::warn!(
                        org_uuid = %org_uuid_str,
                        error = %e,
                        "Cache error during bandwidth_egress check; allowing request"
                    );
                }
            }
        }

        // 9. Store settings in request extensions so downstream handlers can
        //    reconcile bandwidth usage without making an extra cache fetch.
        req.extensions_mut().insert(settings.clone());

        let response = next.call(req).await;

        // Post-response: reconcile egress bandwidth from Content-Length for non-LLM routes.
        // LLM routes reconcile inside their handlers where the emitted response size is known.
        if let Ok(ref resp) = response {
            let resp_len =
                resp.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            if resp_len > 0 {
                if let Some((key, limit)) = bw_egress_for_reconcile {
                    let _ = adjust_token_bucket(&cache, &key, limit, resp_len as i64, BANDWIDTH_BUCKET_WINDOW_SECS).await;
                }
            }
        }

        response.map(|r| r.map_into_left_body())
    }
}

/// Rate-limit configuration routes that must remain available even when rate limits are
/// exhausted so operators can always inspect or adjust their limits.
fn is_rate_limit_management_route(path: &str) -> bool {
    let prefixes = ["/api/v1/organizations", "/v1/organizations", "/organizations"];

    prefixes.iter().any(|p| path == format!("{}/rate-limit", p) || path == format!("{}/rate-limit/", p))
}

/// Atomically reserves `delta` tokens from a per-org cache token bucket.
///
/// # Bucket layout
///
/// Stored as a typed cache value at `bucket_key`:
/// - `tokens` – current token count (float, >= 0, <= `limit`)
/// - `last` – unix timestamp (seconds) of the last refill/update
/// - `consumed` – total tokens consumed in the current bucket
///
/// When the key does not yet exist it is initialised to full capacity (`t = limit`).
/// The key TTL is set to `window_secs + 300` on every call (a 5-minute grace period
/// ensures the key outlives one full window even if no requests arrive exactly at reset).
///
/// # Refill algorithm (lazy)
///
/// On each invocation the helper computes:
/// ```text
/// elapsed = now - ts
/// t = min(limit, t + elapsed × (limit / window_secs))
/// ```
/// This is a continuous token-bucket refill: tokens accumulate at a steady rate and are
/// capped at `limit`. No background job is required.
///
/// # Return value
///
/// Returns `(reserved, remaining)` where:
/// - `reserved = true`  – `delta` tokens were deducted; the request may proceed.
/// - `reserved = false` – bucket had fewer than `delta` tokens; bucket is **not** modified.
///   `remaining` contains the current (post-refill) count so callers can log it.
#[cfg(not(embedded_db))]
async fn reserve_token_ingress(cache: &InternalCache, bucket_key: &str, limit: u64, delta: u64, window_secs: u64) -> ResultEP<(bool, u64)> {
    let _guard = bucket_lock().lock().await;
    let now = chrono::Utc::now().timestamp();
    let (mut tokens, mut last, consumed) = read_bucket(cache, bucket_key, limit, now).await?;

    if now > last {
        let elapsed = (now - last).max(0) as f64;
        tokens = (tokens + elapsed * limit as f64 / window_secs as f64).min(limit as f64);
        last = now;
    }

    if tokens < delta as f64 {
        return Ok((false, 0));
    }

    tokens -= delta as f64;
    write_bucket(cache, bucket_key, tokens, last, consumed.saturating_add(delta), window_secs + 300).await?;
    Ok((true, tokens.max(0.0).floor() as u64))
}

/// Adjusts a per-org cache token bucket by `delta` and returns the new remaining count.
///
/// Unlike [`reserve_token_ingress`] this function **always** modifies the bucket:
/// - Positive `delta` – consumes tokens (e.g. post-response reconciliation of actual
///   prompt/completion token count or bandwidth egress bytes).
/// - Negative `delta` – refunds tokens and decrements consumed accounting (e.g. rolling
///   back a 1-token pre-check reservation when the request fails before the handler can
///   reconcile actual usage).
///
/// The result is clamped to `[0, limit]`: the bucket can never go negative (over-consume)
/// or exceed its configured maximum (over-refund). The same lazy refill formula as
/// [`reserve_token_ingress`] is applied atomically before the adjustment so the bucket
/// state is always current.
///
/// The key TTL is refreshed on every call to `window_secs + 300`.
///
/// # Usage sites
/// - Middleware error path: refund ingress/egress 1-token reservations on failed responses.
/// - LLM chat handlers: reconcile actual prompt/completion counts and deduct measured
///   `bandwidth_egress` bytes after the response is built or streamed.
#[cfg(not(embedded_db))]
pub(crate) async fn adjust_token_bucket(cache: &InternalCache, key: &str, limit: u64, delta: i64, window_secs: u64) -> ResultEP<u64> {
    let _guard = bucket_lock().lock().await;
    let now = chrono::Utc::now().timestamp();
    let (mut tokens, mut last, consumed) = read_bucket(cache, key, limit, now).await?;

    if delta <= 0 {
        tokens = (tokens - delta as f64).min(limit as f64);
        let refunded = delta.saturating_abs() as u64;
        write_bucket(cache, key, tokens, last, consumed.saturating_sub(refunded), window_secs + 300).await?;
        return Ok(tokens.max(0.0).floor() as u64);
    }

    if now > last {
        let elapsed = (now - last).max(0) as f64;
        tokens = (tokens + elapsed * limit as f64 / window_secs as f64).min(limit as f64);
        last = now;
    }

    tokens -= delta as f64;
    if tokens > limit as f64 {
        tokens = limit as f64;
    }

    write_bucket(cache, key, tokens, last, consumed.saturating_add(delta as u64), window_secs + 300).await?;
    Ok(tokens.max(0.0).floor() as u64)
}

#[cfg(not(embedded_db))]
async fn current_bucket_tokens(cache: &InternalCache, key: &str, limit: u64, window_secs: u64) -> ResultEP<f64> {
    let _guard = bucket_lock().lock().await;
    let now = chrono::Utc::now().timestamp();
    let (tokens, last, _) = read_bucket(cache, key, limit, now).await?;
    let elapsed = (now - last).max(0) as f64;
    Ok((tokens + elapsed * limit as f64 / window_secs as f64).min(limit as f64))
}

#[cfg(not(embedded_db))]
async fn read_bucket(cache: &InternalCache, key: &str, limit: u64, now: i64) -> ResultEP<(f64, i64, u64)> {
    match cache.rate_bucket_get(key).await? {
        Some(state) => Ok((state.tokens, state.last, state.consumed)),
        None => Ok((limit as f64, now, 0)),
    }
}

#[cfg(not(embedded_db))]
async fn write_bucket(cache: &InternalCache, key: &str, tokens: f64, last: i64, consumed: u64, ttl_secs: u64) -> ResultEP<()> {
    cache.rate_bucket_set_ex(key, RateBucketState { tokens, last, consumed }, ttl_secs).await?;
    Ok(())
}

/// Build a 429 Too Many Requests response with rate-limit headers (bandwidth).
#[cfg(not(embedded_db))]
fn rate_limit_response(limit: u64, used: u64, direction: &str) -> HttpResponse {
    HttpResponse::TooManyRequests()
        .insert_header(("X-RateLimit-Limit", limit.to_string()))
        .insert_header(("X-RateLimit-Remaining", "0"))
        .json(serde_json::json!({
            "error": format!("Bandwidth {direction} rate limit exceeded"),
            "error_code": "0x0102",
            "limit_bytes": limit,
            "used_bytes": used,
        }))
}

#[cfg(test)]
mod tests {
    use super::is_rate_limit_management_route;

    #[cfg(not(embedded_db))]
    use super::{BANDWIDTH_BUCKET_WINDOW_SECS, adjust_token_bucket, current_bucket_tokens, read_bucket, reserve_token_ingress};
    #[cfg(not(embedded_db))]
    use database::internal_cache::{InternalCache, RateBucketState};

    #[test]
    fn test_rate_limit_management_route_matches_variants() {
        let good = [
            "/api/v1/organizations/rate-limit",
            "/api/v1/organizations/rate-limit/",
            "/v1/organizations/rate-limit",
            "/organizations/rate-limit",
            "/organizations/rate-limit/",
        ];
        for path in good {
            assert!(is_rate_limit_management_route(path), "expected true for {}", path);
        }

        let bad = [
            "/api/v1/organizations",
            "/api/v1/organizations/",
            "/v1/organizations",
            "/api/v1/organizations/export",
            "/api/v1/organizations/foo",
            "/api/v1/llm/agent-gateway/connections",
            "/random",
        ];
        for path in bad {
            assert!(!is_rate_limit_management_route(path), "expected false for {}", path);
        }
    }

    #[cfg(not(embedded_db))]
    #[tokio::test]
    async fn token_bucket_refund_decrements_consumed_accounting() {
        let cache = InternalCache::new();
        let key = "org:token_ingress";

        let (reserved, remaining) = reserve_token_ingress(&cache, key, 10, 1, BANDWIDTH_BUCKET_WINDOW_SECS).await.expect("reserve token");
        assert!(reserved);
        assert_eq!(remaining, 9);
        let (_, _, consumed) = read_bucket(&cache, key, 10, chrono::Utc::now().timestamp()).await.expect("read bucket after reserve");
        assert_eq!(consumed, 1);

        let remaining = adjust_token_bucket(&cache, key, 10, -1, BANDWIDTH_BUCKET_WINDOW_SECS).await.expect("refund token");
        assert_eq!(remaining, 10);
        let (_, _, consumed) = read_bucket(&cache, key, 10, chrono::Utc::now().timestamp()).await.expect("read bucket after refund");
        assert_eq!(consumed, 0);
    }

    #[cfg(not(embedded_db))]
    #[tokio::test]
    async fn token_bucket_adjust_consumes_and_clamps_tokens() {
        let cache = InternalCache::new();
        let key = "org:bandwidth_egress";

        let remaining = adjust_token_bucket(&cache, key, 10, 4, BANDWIDTH_BUCKET_WINDOW_SECS).await.expect("consume tokens");
        assert_eq!(remaining, 6);
        let (tokens, _, consumed) = read_bucket(&cache, key, 10, chrono::Utc::now().timestamp()).await.expect("read bucket after consume");
        assert_eq!(tokens.floor() as u64, 6);
        assert_eq!(consumed, 4);

        let remaining = adjust_token_bucket(&cache, key, 10, -100, BANDWIDTH_BUCKET_WINDOW_SECS).await.expect("over-refund tokens");
        assert_eq!(remaining, 10);
        let (tokens, _, consumed) =
            read_bucket(&cache, key, 10, chrono::Utc::now().timestamp()).await.expect("read bucket after over-refund");
        assert_eq!(tokens.floor() as u64, 10);
        assert_eq!(consumed, 0);
    }

    #[cfg(not(embedded_db))]
    #[tokio::test]
    async fn token_bucket_refills_after_elapsed_time() {
        let cache = InternalCache::new();
        let key = "org:token_refill";
        let now = chrono::Utc::now().timestamp();
        cache.rate_bucket_set(key, RateBucketState { tokens: 0.0, last: now - 150, consumed: 5 }).await.expect("seed bucket");

        let tokens = current_bucket_tokens(&cache, key, 300, 300).await.expect("current bucket tokens");

        assert!((150.0..=151.0).contains(&tokens), "expected about half the bucket to refill, got {tokens}");
    }

    #[cfg(not(embedded_db))]
    #[tokio::test]
    async fn token_bucket_rejects_without_mutating_state() {
        let cache = InternalCache::new();
        let key = "org:token_reject";
        let now = chrono::Utc::now().timestamp();
        let original = RateBucketState { tokens: 0.0, last: now, consumed: 7 };
        cache.rate_bucket_set(key, original).await.expect("seed bucket");

        let (reserved, remaining) = reserve_token_ingress(&cache, key, 10, 1, BANDWIDTH_BUCKET_WINDOW_SECS).await.expect("reserve token");

        assert!(!reserved);
        assert_eq!(remaining, 0);
        assert_eq!(cache.rate_bucket_get(key).await.expect("read bucket"), Some(original));
    }
}
