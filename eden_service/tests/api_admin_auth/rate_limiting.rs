#![cfg(external_db)]

//! Integration tests for org-level bandwidth rate limiting.
//!
//! # Test isolation
//!
//! Each test calls `test_server`, which spins up fresh PostgreSQL / Redis /
//! ClickHouse containers and creates a unique organisation per invocation.
//! There is therefore no shared state between tests.
//!
//! # Redis bucket key format
//!
//! The rate-limiter middleware builds bucket keys from `jwt.org_uuid().to_string()`,
//! which includes the `"org:"` type-prefix produced by `OrganizationUuid`'s `Display`
//! impl.  The plain UUID returned by the API has no prefix, so tests prepend it:
//! `rate_limit_bucket:org:{raw_uuid}:{metric}`.

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_org_raw, get_rate_limit_api, patch_org_raw};
use crate::util::{TestConfig, test_server};
use redis::AsyncCommands;
use serde_json::json;

// ---------------------------------------------------------------------------
// Redis helpers
// ---------------------------------------------------------------------------

/// Build the Redis bucket key matching the format used by the rate-limiter.
fn bucket_key(raw_uuid: &str, metric: &str) -> String {
    format!("rate_limit_bucket:org:{}:{}", raw_uuid, metric)
}

/// Write token-bucket state directly into Redis.
///
/// `tokens` sets the remaining token count.  Supply a **past** `ts` to simulate
/// elapsed time (triggering refill on next access); a **far-future** `ts` prevents
/// any refill regardless of elapsed time.
async fn set_bucket(redis_url: &str, raw_uuid: &str, metric: &str, tokens: i64, ts: Option<i64>) {
    let client = redis::Client::open(redis_url).expect("redis open");
    let mut conn = client.get_multiplexed_async_connection().await.expect("redis connect");
    let key = bucket_key(raw_uuid, metric);
    let ts_val = ts.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let _: () = conn.hset_multiple(&key, &[("t", tokens.to_string()), ("ts", ts_val.to_string())]).await.expect("hset bucket");
}

async fn set_bucket_with_consumed(redis_url: &str, raw_uuid: &str, metric: &str, tokens: i64, ts: i64, consumed: u64) {
    let client = redis::Client::open(redis_url).expect("redis open");
    let mut conn = client.get_multiplexed_async_connection().await.expect("redis connect");
    let key = bucket_key(raw_uuid, metric);
    let _: () = conn
        .hset_multiple(
            &key,
            &[
                ("t", tokens.to_string()),
                ("ts", ts.to_string()),
                ("consumed", consumed.to_string()),
            ],
        )
        .await
        .expect("hset bucket with consumed");
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

/// Extract the raw org UUID string from a PATCH /organizations or GET /organizations response.
///
/// Eden wraps successful responses in `{"data": {...}}`.  Falls back to the top-level
/// `"uuid"` field for flexibility.
fn extract_uuid(body: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(body).expect("parse response JSON");
    v.pointer("/data/uuid")
        .or_else(|| v.pointer("/uuid"))
        .and_then(|u| u.as_str())
        .expect("uuid field missing from response")
        .to_string()
}

// ===========================================================================
// § 1  Bandwidth ingress enforcement
// ===========================================================================

/// A PATCH whose Content-Length exceeds the remaining ingress quota must be
/// rejected with HTTP 429, `error_code` "0x0102", and the configured limit
/// reflected in `X-RateLimit-Limit` / `X-RateLimit-Remaining: 0` headers.
///
/// A fresh organisation is used so the ingress bucket is uninitialised when
/// limits are first applied; the Lua script sets `t = limit` on the first
/// reserved write, which is immediately exceeded by the oversized body.
#[tokio::test]
async fn test_bandwidth_ingress_limit_enforced() {
    const LIMIT: u64 = 100; // bytes

    test_server(
        async move || {
            let client = reqwest::Client::new();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // Enable ingress limiting.  The org has no rate-limit settings yet, so the
            // middleware passes this PATCH through unconditionally.
            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": LIMIT,
                        "bandwidth_egress_limit_bytes": null
                    }
                }),
            )
            .await
            .expect("enable ingress limits");
            assert_eq!(status, 200, "PATCH to enable ingress limits must succeed");

            // Send a PATCH body larger than LIMIT bytes.  The bucket initialises at
            // t = LIMIT = 100; since Content-Length > 100, the Lua reservation fails.
            let big_desc = "x".repeat((LIMIT + 50) as usize);
            let (status, body, headers) =
                patch_org_raw(&client, &jwt.token, &json!({ "description": big_desc })).await.expect("oversized PATCH");

            assert_eq!(status, 429, "Expected 429 when ingress exceeds limit, got {status}: {body}");

            let body_json: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            assert_eq!(
                body_json.get("error_code").and_then(|v| v.as_str()),
                Some("0x0102"),
                "unexpected error_code: {body}"
            );

            let rl_limit = headers.get("X-RateLimit-Limit").and_then(|v| v.to_str().ok()).expect("missing X-RateLimit-Limit header");
            assert_eq!(rl_limit, LIMIT.to_string().as_str(), "X-RateLimit-Limit header mismatch");

            let rl_remaining =
                headers.get("X-RateLimit-Remaining").and_then(|v| v.to_str().ok()).expect("missing X-RateLimit-Remaining header");
            assert_eq!(rl_remaining, "0", "X-RateLimit-Remaining should be 0 when limit is exceeded");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

// ===========================================================================
// § 2  Bandwidth egress enforcement
// ===========================================================================

/// When the egress bucket is seeded to −1 with a far-future timestamp (disabling
/// any refill), GET /organizations must be rejected with HTTP 429 and
/// `error_code` "0x0102".
///
/// The far-future `ts` ensures `elapsed = max(0, now − ts) = 0`, so
/// `current = −1 + 0 × rate = −1 ≤ 0` regardless of test execution time.
#[tokio::test]
async fn test_bandwidth_egress_limit_enforced() {
    const LIMIT: u64 = 50; // bytes

    test_server(
        async move || {
            let client = reqwest::Client::new();
            let redis_url = TestConfig::get_redis_conn();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // GET /organizations before enabling limits to learn the org UUID.
            // No settings are configured yet, so the middleware passes the request through.
            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("initial GET");
            assert_eq!(status, 200, "initial GET must succeed: {body}");
            let org_uuid = extract_uuid(&body);

            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": null,
                        "bandwidth_egress_limit_bytes": LIMIT
                    }
                }),
            )
            .await
            .expect("enable egress limits");
            assert_eq!(status, 200, "PATCH to enable egress limits must succeed");

            // Seed the egress bucket to t = −1 with a far-future timestamp.
            // elapsed = max(0, now − far_future) = 0 → current = −1 ≤ 0 → always rejected.
            let far_future = chrono::Utc::now().timestamp() + 3_600;
            set_bucket(&redis_url, &org_uuid, "bandwidth_egress", -1, Some(far_future)).await;

            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("GET after exhaustion");
            assert_eq!(status, 429, "Expected 429 when egress is exhausted, got {status}: {body}");

            let body_json: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            assert_eq!(
                body_json.get("error_code").and_then(|v| v.as_str()),
                Some("0x0102"),
                "unexpected error_code: {body}"
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

// ===========================================================================
// § 3  Bandwidth ingress continuous refill
// ===========================================================================

/// After the ingress bucket is set to `t = 0` with a timestamp backdated by
/// 150 seconds, the server's lazy-refill formula credits back
/// `150 × (1000 / 300) = 500` bytes.  A subsequent ~100-byte PATCH therefore
/// succeeds without sleeping.
#[tokio::test]
async fn test_bandwidth_ingress_refills_continuously() {
    const LIMIT: u64 = 1_000;

    test_server(
        async move || {
            let client = reqwest::Client::new();
            let redis_url = TestConfig::get_redis_conn();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // GET /organizations before enabling limits to learn the org UUID.
            // No settings are configured yet, so the middleware passes the request through.
            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("initial GET");
            assert_eq!(status, 200, "initial GET must succeed: {body}");
            let org_uuid = extract_uuid(&body);

            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": LIMIT,
                        "bandwidth_egress_limit_bytes": null
                    }
                }),
            )
            .await
            .expect("enable ingress limits");
            assert_eq!(status, 200, "enable limits must succeed");

            // Simulate 150 s of elapsed time by backdating the bucket timestamp.
            // Refill: remaining = min(1000, 0 + 150 × (1000/300)) ≈ 500 bytes.
            let past_ts = chrono::Utc::now().timestamp() - 150;
            set_bucket(&redis_url, &org_uuid, "bandwidth_ingress", 0, Some(past_ts)).await;

            // A ~100-byte PATCH is well within the ~500-byte refilled capacity.
            let (status, body, _) =
                patch_org_raw(&client, &jwt.token, &json!({ "description": "x".repeat(80) })).await.expect("PATCH after refill");

            assert_eq!(status, 200, "Expected 200 after ~500-byte ingress refill, got {status}: {body}");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

// ===========================================================================
// § 4  Bandwidth egress continuous refill
// ===========================================================================

/// Mirrors the ingress refill test for the egress direction.  After backdating
/// the egress bucket by 150 s, the pre-check sees ~500 bytes of capacity and
/// allows the GET through.
#[tokio::test]
async fn test_bandwidth_egress_refills_continuously() {
    const LIMIT: u64 = 1_000;

    test_server(
        async move || {
            let client = reqwest::Client::new();
            let redis_url = TestConfig::get_redis_conn();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // GET /organizations before enabling limits to learn the org UUID.
            // No settings are configured yet, so the middleware passes the request through.
            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("initial GET");
            assert_eq!(status, 200, "initial GET must succeed: {body}");
            let org_uuid = extract_uuid(&body);

            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": null,
                        "bandwidth_egress_limit_bytes": LIMIT
                    }
                }),
            )
            .await
            .expect("enable egress limits");
            assert_eq!(status, 200, "enable limits must succeed");

            // Backdate the egress bucket by 150 s → ~500 bytes refilled.
            let past_ts = chrono::Utc::now().timestamp() - 150;
            set_bucket(&redis_url, &org_uuid, "bandwidth_egress", 0, Some(past_ts)).await;

            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("GET after refill");
            assert_eq!(status, 200, "Expected 200 after egress refill, got {status}: {body}");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

// ===========================================================================
// § 5  Bandwidth visibility API
// ===========================================================================

/// GET /organizations/rate-limit returns a correctly shaped bandwidth snapshot:
/// `used_bytes`, `remaining_bytes`, `limit_bytes`, and an RFC-3339 `resets_at`
/// for both ingress and egress directions.
#[tokio::test]
async fn test_visibility_api_reports_bandwidth_status() {
    const INGRESS_LIMIT: u64 = 50_000;
    const EGRESS_LIMIT: u64 = 100_000;

    test_server(
        async move || {
            let client = reqwest::Client::new();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": INGRESS_LIMIT,
                        "bandwidth_egress_limit_bytes": EGRESS_LIMIT
                    }
                }),
            )
            .await
            .expect("enable limits");
            assert_eq!(status, 200);

            let _ = patch_org_raw(&client, &jwt.token, &json!({ "description": "visibility-api-test" })).await.expect("traffic PATCH");
            let _ = get_org_raw(&client, &jwt.token).await.expect("traffic GET");

            let (status, data) = get_rate_limit_api(&client, &jwt.token).await.expect("visibility API");
            assert_eq!(status, 200, "GET /organizations/rate-limit failed: {data}");

            // Eden wraps responses in {"data": {...}}; fall back to the root if absent.
            let data = data.pointer("/data").cloned().unwrap_or(data);

            assert_eq!(
                data.get("enabled").and_then(|v| v.as_bool()),
                Some(true),
                "rate limiting should be reported as enabled"
            );

            let ing = data.get("bandwidth_ingress").expect("bandwidth_ingress key missing");
            assert_eq!(ing.get("limit_bytes").and_then(|v| v.as_u64()), Some(INGRESS_LIMIT), "ingress limit_bytes mismatch");
            assert!(ing.get("remaining_bytes").is_some(), "ingress remaining_bytes missing");
            let resets_at = ing.get("resets_at").and_then(|v| v.as_str()).expect("ingress resets_at missing");
            assert!(resets_at.contains('T'), "ingress resets_at not RFC3339: {resets_at}");

            let egr = data.get("bandwidth_egress").expect("bandwidth_egress key missing");
            assert_eq!(egr.get("limit_bytes").and_then(|v| v.as_u64()), Some(EGRESS_LIMIT), "egress limit_bytes mismatch");
            assert!(egr.get("remaining_bytes").is_some(), "egress remaining_bytes missing");
            let resets_at = egr.get("resets_at").and_then(|v| v.as_str()).expect("egress resets_at missing");
            assert!(resets_at.contains('T'), "egress resets_at not RFC3339: {resets_at}");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

/// The rate-limit inspection/management route must remain reachable even when bandwidth
/// buckets are exhausted so admins can inspect counters and raise limits without waiting
/// for refill. Base org GET/PATCH remain subject to rate limits.
#[tokio::test]
async fn test_management_routes_bypass_bandwidth_limits() {
    const LIMIT: u64 = 1_024;

    test_server(
        async move || {
            let client = reqwest::Client::new();
            let redis_url = TestConfig::get_redis_conn();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("initial GET");
            assert_eq!(status, 200, "initial GET must succeed: {body}");
            let org_uuid = extract_uuid(&body);

            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": LIMIT,
                        "bandwidth_egress_limit_bytes": LIMIT
                    }
                }),
            )
            .await
            .expect("enable limits");
            assert_eq!(status, 200, "enable limits must succeed");

            let far_future = chrono::Utc::now().timestamp() + 3_600;
            set_bucket(&redis_url, &org_uuid, "bandwidth_ingress", -1, Some(far_future)).await;
            set_bucket(&redis_url, &org_uuid, "bandwidth_egress", -1, Some(far_future)).await;

            let (status, data) = get_rate_limit_api(&client, &jwt.token).await.expect("rate-limit GET");
            assert_eq!(status, 200, "GET /organizations/rate-limit should bypass bandwidth limits, got {status}: {data}");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

/// `remaining_bytes` should reflect the live refilled bucket capacity rather than
/// the historical `consumed` counter. After 150 seconds of refill, a bucket with
/// `t = 100` and `consumed = 900` on a 1000-byte limit should report well above 100
/// bytes remaining.
#[tokio::test]
async fn test_visibility_api_reports_live_bandwidth_remaining() {
    const LIMIT: u64 = 1_000;

    test_server(
        async move || {
            let client = reqwest::Client::new();
            let redis_url = TestConfig::get_redis_conn();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("initial GET");
            assert_eq!(status, 200, "initial GET must succeed: {body}");
            let org_uuid = extract_uuid(&body);

            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": LIMIT,
                        "bandwidth_egress_limit_bytes": null
                    }
                }),
            )
            .await
            .expect("enable limits");
            assert_eq!(status, 200, "enable limits must succeed");

            let past_ts = chrono::Utc::now().timestamp() - 150;
            set_bucket_with_consumed(&redis_url, &org_uuid, "bandwidth_ingress", 100, past_ts, 900).await;

            let (status, data) = get_rate_limit_api(&client, &jwt.token).await.expect("visibility API");
            assert_eq!(status, 200, "GET /organizations/rate-limit failed: {data}");

            let data = data.pointer("/data").cloned().unwrap_or(data);
            let ingress = data.get("bandwidth_ingress").expect("bandwidth_ingress key missing");
            let used = ingress.get("used_bytes").and_then(|v| v.as_u64()).expect("used_bytes missing");
            let remaining = ingress.get("remaining_bytes").and_then(|v| v.as_u64()).expect("remaining_bytes missing");

            assert_eq!(used, 900, "used_bytes should continue to expose the historical consumed counter");
            assert!(
                remaining > 500,
                "remaining_bytes should reflect live refill capacity, expected > 500 after 150s refill, got {remaining}"
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

// ===========================================================================
// § 6  Disabled rate limiting bypasses all checks
// ===========================================================================

/// When rate limiting is disabled (`enabled = false`), every request passes
/// through unconditionally even when both buckets are seeded to −1 in Redis.
///
/// Ordering is critical: limits are disabled **while budget is still available**
/// so the PATCH that disables them is not itself rate-limited.  The buckets are
/// then exhausted via Redis, and both subsequent requests must return 200.
#[tokio::test]
async fn test_disabled_rate_limiting_bypasses_all_checks() {
    const LIMIT: u64 = 1_024;

    test_server(
        async move || {
            let client = reqwest::Client::new();
            let redis_url = TestConfig::get_redis_conn();

            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .expect("create org");
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // GET /organizations before enabling limits to learn the org UUID.
            // No settings are configured yet, so the middleware passes the request through.
            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("initial GET");
            assert_eq!(status, 200, "initial GET must succeed: {body}");
            let org_uuid = extract_uuid(&body);

            // Step 1: enable limits while the org has no prior settings (full budget).
            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": true,
                        "bandwidth_ingress_limit_bytes": LIMIT,
                        "bandwidth_egress_limit_bytes": LIMIT
                    }
                }),
            )
            .await
            .expect("enable limits");
            assert_eq!(status, 200, "enable limits must succeed");

            // Step 2: disable rate limiting while budget is still available.
            let (status, _, _) = patch_org_raw(
                &client,
                &jwt.token,
                &json!({
                    "rate_limit_settings": {
                        "enabled": false,
                        "bandwidth_ingress_limit_bytes": null,
                        "bandwidth_egress_limit_bytes": null
                    }
                }),
            )
            .await
            .expect("disable limits");
            assert_eq!(status, 200, "PATCH to disable rate limiting must succeed");

            // Step 3: exhaust both buckets in Redis with a far-future ts so no refill occurs.
            let far_future = chrono::Utc::now().timestamp() + 3_600;
            set_bucket(&redis_url, &org_uuid, "bandwidth_ingress", -1, Some(far_future)).await;
            set_bucket(&redis_url, &org_uuid, "bandwidth_egress", -1, Some(far_future)).await;

            // Step 4: both requests must return 200 because rate limiting is disabled.
            let (status, body, _) =
                patch_org_raw(&client, &jwt.token, &json!({ "description": "disabled-check" })).await.expect("PATCH while disabled");
            assert_eq!(status, 200, "PATCH should pass when rate limiting is disabled, got {status}: {body}");

            let (status, body, _) = get_org_raw(&client, &jwt.token).await.expect("GET while disabled");
            assert_eq!(status, 200, "GET should pass when rate limiting is disabled, got {status}: {body}");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}
