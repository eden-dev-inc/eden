#![cfg(feature = "postgres")]
#![cfg(external_db)]

use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, endpoint_connect_pg, get_base_url, make_method_request};
use crate::util::test_server;

use eden_core::format::EdenUuid;

#[tokio::test]
async fn test_list_pipelines() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // LIST: GET /api/v1/pipelines
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/pipelines", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to list pipelines");

            assert!(result.is_some(), "List pipelines should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_create_pipeline() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Connect source endpoint
            let source_ep = endpoint_connect_pg(&client, &admin_jwt.token)
                .await
                .expect("Failed to connect source endpoint")
                .expect("No source endpoint response");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // CREATE: POST /api/v1/pipelines
            // Use source endpoint UUID for both source and target to verify endpoint exists
            let body = json!({
                "id": "test-pipe",
                "source_endpoint": source_ep.uuid.uuid().to_string(),
                "target_endpoint": source_ep.uuid.uuid().to_string()
            });

            // Send request and verify endpoint is reachable (not 404)
            let response = reqwest::Client::default()
                .post(format!("{}/pipelines", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .json(&body)
                .send()
                .await
                .expect("Failed to send create pipeline request");

            let status = response.status();
            assert_ne!(status.as_u16(), 404, "POST /api/v1/pipelines should not return 404");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
