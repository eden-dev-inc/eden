#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

/// Helper: create an API via POST /api/v1/apis
async fn create_api(
    client: &reqwest::Client,
    token: &str,
    api_id: &str,
    description: &str,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let body = json!({
        "id": api_id,
        "description": description,
        "fields": [],
        "bindings": []
    });
    make_method_request::<serde_json::Value, serde_json::Value>(
        client,
        token,
        HttpMethod::Post,
        &format!("{}/apis", get_base_url()),
        Some(&body),
        Some(200),
    )
    .await
}

#[tokio::test]
async fn test_create_api() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // CREATE: POST /api/v1/apis
            create_api(&client, &admin_jwt.token, "test-api", "Test API").await.expect("Failed to create API");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_list_apis() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Create an API first
            create_api(&client, &admin_jwt.token, "list-api-1", "API for listing").await.expect("Failed to create API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // LIST: GET /api/v1/apis
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/apis", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to list APIs");

            assert!(result.is_some(), "List APIs should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_get_single_api() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let api_id = "get-api-1";
            create_api(&client, &admin_jwt.token, api_id, "API for get").await.expect("Failed to create API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // GET: GET /api/v1/apis/{api}
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/apis/{}", get_base_url(), api_id),
                None,
                None,
            )
            .await
            .expect("Failed to get API");

            assert!(result.is_some(), "Get API should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_update_api() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let api_id = "update-api-1";

            // CREATE with a description and one field.
            let create_body = json!({
                "id": api_id,
                "description": "Original description",
                "fields": [{ "name": "user_id", "field_type": "string", "description": "id", "required": true }],
                "bindings": []
            });
            make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/apis", get_base_url()),
                Some(&create_body),
                Some(200),
            )
            .await
            .expect("Failed to create API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // PATCH only the description — fields must be preserved.
            let patch_status = client
                .patch(format!("{}/apis/{}", get_base_url(), api_id))
                .bearer_auth(&admin_jwt.token)
                .json(&json!({ "description": "Updated description" }))
                .send()
                .await
                .expect("Failed to send PATCH")
                .status();
            assert_eq!(patch_status, 200, "PATCH /apis/{{api}} should succeed (got {patch_status})");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verbose GET returns the full ApiSchema; confirm the merge.
            let resp = client
                .get(format!("{}/apis/{}", get_base_url(), api_id))
                .bearer_auth(&admin_jwt.token)
                .header("X-Eden-Verbose", "true")
                .send()
                .await
                .expect("Failed to GET API after patch");
            assert!(resp.status().is_success(), "verbose GET after PATCH should succeed (got {})", resp.status());
            let text = resp.text().await.unwrap_or_default();
            assert!(text.contains("Updated description"), "PATCH should update the description; body: {text}");
            assert!(text.contains("user_id"), "PATCH must preserve unspecified fields (user_id); body: {text}");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_delete_api() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let api_id = "delete-api-1";
            create_api(&client, &admin_jwt.token, api_id, "API to delete").await.expect("Failed to create API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE: DELETE /api/v1/apis/{api}
            let _: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Delete,
                &format!("{}/apis/{}", get_base_url(), api_id),
                None,
                Some(200),
            )
            .await
            .expect("Failed to delete API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify: GET should fail after deletion
            let get_after_delete = client
                .get(format!("{}/apis/{}", get_base_url(), api_id))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed API");

            assert!(
                !get_after_delete.status().is_success(),
                "GET should fail after API deletion (got status: {})",
                get_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_api_full_crud_lifecycle() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let api_id = "lifecycle-api";

            // CREATE
            create_api(&client, &admin_jwt.token, api_id, "Lifecycle test API").await.expect("Failed to create API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ (single)
            let get_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/apis/{}", get_base_url(), api_id),
                None,
                None,
            )
            .await
            .expect("Failed to get API");
            assert!(get_result.is_some(), "API should exist after creation");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ (list)
            let list_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/apis", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to list APIs");
            assert!(list_result.is_some(), "List should return results");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE
            let _: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Delete,
                &format!("{}/apis/{}", get_base_url(), api_id),
                None,
                Some(200),
            )
            .await
            .expect("Failed to delete API");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // CONFIRM DELETED
            let get_after_delete = client
                .get(format!("{}/apis/{}", get_base_url(), api_id))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed API");

            assert!(
                !get_after_delete.status().is_success(),
                "API should not exist after deletion (got status: {})",
                get_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
