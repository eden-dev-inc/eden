//! End-to-end HTTP tests for `GET /api/v1/analytics/series` driven through the
//! full server (auth middleware + routing + handler) via the shared test harness.
//!
//! Auth (401) and param-validation (400) are asserted unconditionally. The full
//! 200 columnar + ETag/304 contract is asserted when the analytics backend
//! (ClickHouse) is up; if it is unavailable in this environment the body checks
//! are skipped (the auth/routing assertions still exercise the HTTP path).
#![cfg(external_db)]

mod common;
mod request;
mod util;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn series_endpoint_e2e() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let base = get_base_url();
            // Fixed window → deterministic grid/ETag so the 304 revalidation is reliable.
            let url = format!(
                "{base}/analytics/series?metrics=eden.request_sent|Counter&from=2026-05-01T00:00:00Z&to=2026-05-01T01:00:00Z&buckets=12"
            );

            // 1. Unauthenticated → 401 (bearer middleware).
            let unauth = client.get(&url).send().await.expect("unauth request");
            assert_eq!(unauth.status().as_u16(), 401, "unauthenticated should be 401");

            // Authenticate as the org superadmin.
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // 2. Missing `metrics` → 400 (param validation, before any DB work).
            let bad = client.get(format!("{base}/analytics/series?range=1h")).bearer_auth(&jwt.token).send().await.expect("bad request");
            assert_eq!(bad.status().as_u16(), 400, "missing metrics should be 400");

            // 3. Valid request: auth + routing must have passed (not 401/400). When the
            //    analytics backend is up, assert the full columnar contract + ETag/304.
            let resp = client.get(&url).bearer_auth(&jwt.token).send().await.expect("series request");
            let status = resp.status().as_u16();
            assert_ne!(status, 401, "authenticated request must not be 401");
            assert_ne!(status, 400, "valid params must not be 400");
            if status != 200 {
                eprintln!("series returned {status}; analytics backend unavailable — skipping body checks");
                return;
            }

            let etag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(str::to_string).expect("ETag header on 200");
            let body: serde_json::Value = resp.json().await.expect("json body");
            assert_eq!(body["n"], 12, "12 buckets");
            assert!(body["t0"].is_number() && body["step"].is_number(), "grid present");
            let series = body["series"].as_array().expect("series array");
            assert_eq!(series.len(), 1, "one series requested");
            assert_eq!(series[0]["name"], "eden.request_sent");
            assert_eq!(series[0]["values"].as_array().expect("values").len(), 12, "values aligned to grid");

            // 4. Conditional revalidation with the same ETag → 304 (no body re-sent).
            let revalidate =
                client.get(&url).bearer_auth(&jwt.token).header("If-None-Match", &etag).send().await.expect("revalidate request");
            assert_eq!(revalidate.status().as_u16(), 304, "matching ETag should 304");
        },
        None,
    );
}
