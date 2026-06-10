#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

/// Helper: create a robot via POST /api/v1/iam/agents
async fn create_robot(
    client: &reqwest::Client,
    token: &str,
    robot_id: &str,
    description: &str,
    ttl: u64,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let body = json!({
        "username": robot_id,
        "description": description,
        "ttl_sec": ttl
    });
    make_method_request::<serde_json::Value, serde_json::Value>(
        client,
        token,
        HttpMethod::Post,
        &format!("{}/iam/agents", get_base_url()),
        Some(&body),
        Some(201),
    )
    .await
}

#[tokio::test]
async fn test_create_robot() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // CREATE: POST /api/v1/iam/agents
            create_robot(&client, &admin_jwt.token, "test-robot-1", "A test robot", 3600).await.expect("Failed to create robot");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_list_robots() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Create a robot first
            create_robot(&client, &admin_jwt.token, "list-robot-1", "Robot for listing", 3600)
                .await
                .expect("Failed to create robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // LIST: GET /api/v1/iam/agents
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/agents", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to list robots");

            assert!(result.is_some(), "List robots should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_get_single_robot() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let robot_id = "get-robot-1";
            create_robot(&client, &admin_jwt.token, robot_id, "Robot for get", 7200).await.expect("Failed to create robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // GET: GET /api/v1/iam/agents/{robot}
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/agents/{}", get_base_url(), robot_id),
                None,
                None,
            )
            .await
            .expect("Failed to get robot");

            assert!(result.is_some(), "Get robot should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_update_robot() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let robot_id = "update-robot-1";
            create_robot(&client, &admin_jwt.token, robot_id, "Original description", 3600).await.expect("Failed to create robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // UPDATE: PATCH /api/v1/iam/agents/{robot}
            let patch_body = json!({
                "description": "Updated robot description"
            });
            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Patch,
                &format!("{}/iam/agents/{}", get_base_url(), robot_id),
                Some(&patch_body),
                Some(200),
            )
            .await
            .expect("Failed to update robot");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_rotate_robot_key() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let robot_id = "rotate-robot-1";
            create_robot(&client, &admin_jwt.token, robot_id, "Robot for key rotation", 3600).await.expect("Failed to create robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ROTATE KEY: POST /api/v1/iam/agents/{robot}/rotate-key
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/iam/agents/{}/rotate-key", get_base_url(), robot_id),
                None,
                None,
            )
            .await
            .expect("Failed to rotate robot key");

            assert!(result.is_some(), "Rotate key should return a response with the new key");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_delete_robot() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let robot_id = "delete-robot-1";
            create_robot(&client, &admin_jwt.token, robot_id, "Robot to delete", 3600).await.expect("Failed to create robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE: DELETE /api/v1/iam/agents/{robot}
            let _: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Delete,
                &format!("{}/iam/agents/{}", get_base_url(), robot_id),
                None,
                Some(204),
            )
            .await
            .expect("Failed to delete robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify: GET should fail after deletion
            let get_after_delete = client
                .get(format!("{}/iam/agents/{}", get_base_url(), robot_id))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed API");

            assert!(
                !get_after_delete.status().is_success(),
                "GET should fail after robot deletion (got status: {})",
                get_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_robot_full_crud_lifecycle() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let robot_id = "lifecycle-robot";

            // CREATE
            create_robot(&client, &admin_jwt.token, robot_id, "Lifecycle test robot", 3600).await.expect("Failed to create robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ (single)
            let get_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/agents/{}", get_base_url(), robot_id),
                None,
                None,
            )
            .await
            .expect("Failed to get robot");
            assert!(get_result.is_some(), "Robot should exist after creation");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ (list)
            let list_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/agents", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to list robots");
            assert!(list_result.is_some(), "List should return results");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // UPDATE
            let patch_body = json!({
                "description": "Updated lifecycle robot"
            });
            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Patch,
                &format!("{}/iam/agents/{}", get_base_url(), robot_id),
                Some(&patch_body),
                Some(200),
            )
            .await
            .expect("Failed to update robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ROTATE KEY
            let rotate_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/iam/agents/{}/rotate-key", get_base_url(), robot_id),
                None,
                None,
            )
            .await
            .expect("Failed to rotate key");
            assert!(rotate_result.is_some(), "Rotate key should return new key data");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE
            let _: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Delete,
                &format!("{}/iam/agents/{}", get_base_url(), robot_id),
                None,
                Some(204),
            )
            .await
            .expect("Failed to delete robot");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // CONFIRM DELETED
            let get_after_delete = client
                .get(format!("{}/iam/agents/{}", get_base_url(), robot_id))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed API");

            assert!(
                !get_after_delete.status().is_success(),
                "Robot should not exist after deletion (got status: {})",
                get_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
