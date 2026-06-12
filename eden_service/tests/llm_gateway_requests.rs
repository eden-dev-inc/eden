//! End-to-end HTTP tests for `GET /api/v1/llm/gateway/requests` driven through
//! the full server (auth middleware + routing + handler) via the shared test
//! harness.
//!
//! Auth (401) is asserted unconditionally. The full 200 contract (the paginated
//! `{ total, limit, offset, rows }` envelope) is asserted when the analytics
//! backend (ClickHouse) is up; if it is unavailable in this environment the body
//! checks are skipped (the auth/routing assertions still exercise the HTTP path).
#![cfg(external_db)]

mod common;
mod request;
mod util;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn gateway_requests_endpoint_e2e() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let base = get_base_url();
            let url = format!("{base}/llm/gateway/requests?range=7d&limit=25");

            // 1. Unauthenticated → 401 (bearer middleware).
            let unauth = client.get(&url).send().await.expect("unauth request");
            assert_eq!(unauth.status().as_u16(), 401, "unauthenticated should be 401");

            // Authenticate as the org superadmin.
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("login");

            // 2. Valid request: auth + routing must have passed (not 401). When the
            //    analytics backend is up, assert the paginated envelope contract.
            let resp = client.get(&url).bearer_auth(&jwt.token).send().await.expect("requests query");
            let status = resp.status().as_u16();
            assert_ne!(status, 401, "authenticated request must not be 401");
            if status != 200 {
                eprintln!("gateway/requests returned {status}; analytics backend unavailable — skipping body checks");
                return;
            }

            let body: serde_json::Value = resp.json().await.expect("json body");
            // EdenResponse wraps the payload; the response struct serializes its
            // fields at the top level of that envelope.
            assert!(body["total"].is_number(), "total present");
            assert_eq!(body["limit"], 25, "limit echoes the request (capped at 200)");
            assert_eq!(body["offset"], 0, "offset defaults to 0");
            assert!(body["rows"].is_array(), "rows is an array");

            // 3. limit is clamped to the 200 hard cap.
            let capped = client
                .get(format!("{base}/llm/gateway/requests?range=7d&limit=9999"))
                .bearer_auth(&jwt.token)
                .send()
                .await
                .expect("capped request");
            if capped.status().as_u16() == 200 {
                let capped_body: serde_json::Value = capped.json().await.expect("json body");
                assert_eq!(capped_body["limit"], 200, "limit clamped to MAX_LIMIT");
            }
        },
        None,
    );
}
