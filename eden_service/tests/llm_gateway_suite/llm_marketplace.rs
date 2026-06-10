#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn test_llm_marketplace_search_route_exists() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let resp = client
                .get(format!("{}/llm/marketplace/search?q=test", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed to send request to /llm/marketplace/search");

            let status = resp.status();
            assert_ne!(status.as_u16(), 404, "GET /api/v1/llm/marketplace/search should not return 404, got {}", status);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_llm_marketplace_import_route_exists() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "url": "https://example.com/skill"
            });

            let resp = client
                .post(format!("{}/admin/llm/marketplace/import", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .json(&body)
                .send()
                .await
                .expect("Failed to send request to /admin/llm/marketplace/import");

            let status = resp.status();
            assert_ne!(
                status.as_u16(),
                404,
                "POST /api/v1/admin/llm/marketplace/import should not return 404, got {}",
                status
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_llm_marketplace_updates_route_exists() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let resp = client
                .get(format!("{}/admin/llm/marketplace/updates", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed to send request to /admin/llm/marketplace/updates");

            let status = resp.status();
            assert_ne!(
                status.as_u16(),
                404,
                "GET /api/v1/admin/llm/marketplace/updates should not return 404, got {}",
                status
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
