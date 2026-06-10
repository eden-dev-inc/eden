#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn test_create_function_endpoint_reachable() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "endpoint": "test-fn",
                "kind": "function",
                "config": {}
            });

            // POST /api/v1/functions — verify the endpoint is reachable (not 404)
            let resp = client
                .post(format!("{}/functions", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .json(&body)
                .send()
                .await
                .expect("Failed to send request to /functions");

            let status = resp.status();
            assert_ne!(status.as_u16(), 404, "POST /api/v1/functions should not return 404, got {}", status);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
