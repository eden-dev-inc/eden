#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, ORG_DESCR, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request, make_request};
use crate::util::test_server;

/// Test GET /api/v1/organizations - retrieve organization details after creation
#[tokio::test]
async fn test_get_organization() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // GET organization details
            let org_url = format!("{}/organizations", get_base_url());
            let resp: Option<Value> =
                make_request(&client, &admin_jwt.token, &org_url, None::<&()>, false, None).await.expect("Failed to GET organization");

            let org_data = resp.expect("Expected organization response body");

            // Verify the response contains expected fields
            assert!(org_data.get("data").is_some(), "Response should contain 'data' field, got: {}", org_data);

            let data = org_data.get("data").expect("Missing data field");

            assert!(data.get("id").is_some(), "Organization should have an 'id' field");
            assert!(data.get("uuid").is_some(), "Organization should have a 'uuid' field");

            // Verify description matches what was set during creation
            if let Some(desc) = data.get("description") {
                assert_eq!(desc.as_str().unwrap_or_default(), ORG_DESCR, "Organization description should match creation input");
            }
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test PATCH /api/v1/organizations - update organization description
#[tokio::test]
async fn test_patch_organization() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let org_url = format!("{}/organizations", get_base_url());

            // PATCH: update the organization description
            let new_description = "Updated organization description";
            let patch_body = json!({
                "description": new_description
            });

            let _: Option<Value> =
                make_method_request(&client, &admin_jwt.token, HttpMethod::Patch, &org_url, Some(&patch_body), Some(200))
                    .await
                    .expect("Failed to PATCH organization");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // GET: verify the description was updated
            let resp: Option<Value> = make_request(&client, &admin_jwt.token, &org_url, None::<&()>, false, None)
                .await
                .expect("Failed to GET organization after PATCH");

            let org_data = resp.expect("Expected organization response body");
            let data = org_data.get("data").expect("Missing data field");

            if let Some(desc) = data.get("description") {
                assert_eq!(desc.as_str().unwrap_or_default(), new_description, "Organization description should be updated");
            }
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test DELETE /api/v1/organizations - delete an organization
#[tokio::test]
async fn test_delete_organization() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let org_url = format!("{}/organizations", get_base_url());

            // DELETE the organization
            let _: Option<Value> = make_method_request(&client, &admin_jwt.token, HttpMethod::Delete, &org_url, None::<&()>, Some(200))
                .await
                .expect("Failed to DELETE organization");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify: GET after delete should fail (org no longer exists)
            let get_resp = client.get(&org_url).bearer_auth(&admin_jwt.token).send().await.expect("Failed to send GET after DELETE");

            assert!(
                !get_resp.status().is_success(),
                "GET should fail after organization is deleted (got status: {})",
                get_resp.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
