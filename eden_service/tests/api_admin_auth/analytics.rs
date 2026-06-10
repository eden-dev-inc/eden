#![cfg(external_db)]

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn test_analytics_status() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // GET /api/v1/analytics/status
            // Verbose analytics routes are unavailable in this distribution.
            let resp = client
                .get(format!("{}/analytics/status", get_base_url()))
                .bearer_auth(token)
                .send()
                .await
                .expect("Failed to send analytics status request");

            let status = resp.status().as_u16();
            assert_ne!(status, 404, "Analytics status endpoint should exist (not 404)");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_analytics_enable() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // POST /api/v1/analytics/enable
            let resp = client
                .post(format!("{}/analytics/enable", get_base_url()))
                .bearer_auth(token)
                .send()
                .await
                .expect("Failed to send analytics enable request");

            let status = resp.status().as_u16();
            assert_ne!(status, 404, "Analytics enable endpoint should exist (not 404)");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_analytics_disable() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // POST /api/v1/analytics/disable
            let resp = client
                .post(format!("{}/analytics/disable", get_base_url()))
                .bearer_auth(token)
                .send()
                .await
                .expect("Failed to send analytics disable request");

            let status = resp.status().as_u16();
            assert_ne!(status, 404, "Analytics disable endpoint should exist (not 404)");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_clickhouse_metrics_endpoint() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // GET /api/v1/analytics/clickhouse/metrics
            let resp = client
                .get(format!("{}/analytics/clickhouse/metrics?range=5m&limit=1", get_base_url()))
                .bearer_auth(token)
                .send()
                .await
                .expect("Failed to send ClickHouse metrics request");

            let status = resp.status().as_u16();
            assert_ne!(status, 404, "ClickHouse metrics endpoint should exist (not 404)");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
