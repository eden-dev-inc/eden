#![cfg(external_db)]

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn test_org_export_endpoint_reachable() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // POST /api/v1/organizations/export — verify endpoint exists (not 404)
            let resp = client
                .post(format!("{}/organizations/export", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed to send request to /organizations/export");

            let status = resp.status();
            assert_ne!(status.as_u16(), 404, "POST /api/v1/organizations/export should not return 404, got {}", status);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_org_import_endpoint_reachable() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // POST /api/v1/organizations/import — verify endpoint exists (not 404)
            let resp = client
                .post(format!("{}/organizations/import", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .json(&serde_json::json!({}))
                .send()
                .await
                .expect("Failed to send request to /organizations/import");

            let status = resp.status();
            assert_ne!(status.as_u16(), 404, "POST /api/v1/organizations/import should not return 404, got {}", status);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
