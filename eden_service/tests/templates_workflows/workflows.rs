#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_workflow_crud_lifecycle() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let workflow_id = "test-workflow-1";
            let workflow_body = json!({
                "id": workflow_id,
                "description": "A test workflow",
                "dag": {
                    "nodes": [
                        {"id": "step1", "type": "query", "endpoint": "postgres_test1"},
                        {"id": "step2", "type": "query", "endpoint": "postgres_test1"}
                    ],
                    "edges": [
                        {"from": "step1", "to": "step2"}
                    ]
                }
            });

            // CREATE: POST /api/v1/workflows
            let create_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/workflows", get_base_url()),
                Some(&workflow_body),
                Some(201),
            )
            .await
            .expect("Failed to create workflow");
            println!("Create response: {:?}", create_resp);

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ: GET /api/v1/workflows/{workflow}
            let get_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/workflows/{}", get_base_url(), workflow_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get workflow");

            let get_data = get_resp.expect("Expected get response body");
            println!("Get response: {:?}", get_data);
            assert_eq!(
                get_data.get("id").and_then(|v| v.as_str()).unwrap_or_default(),
                workflow_id,
                "Workflow id should match"
            );
            assert_eq!(
                get_data.get("description").and_then(|v| v.as_str()).unwrap_or_default(),
                "A test workflow",
                "Workflow description should match"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // UPDATE: PATCH /api/v1/workflows/{workflow}
            let update_body = json!({
                "description": "Updated test workflow description"
            });

            let _update_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Patch,
                &format!("{}/workflows/{}", get_base_url(), workflow_id),
                Some(&update_body),
                Some(200),
            )
            .await
            .expect("Failed to update workflow");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // VERIFY UPDATE: GET after PATCH
            let get_after_update: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/workflows/{}", get_base_url(), workflow_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get workflow after update");

            let updated_data = get_after_update.expect("Expected updated response body");
            assert_eq!(
                updated_data.get("description").and_then(|v| v.as_str()).unwrap_or_default(),
                "Updated test workflow description",
                "Workflow description should be updated"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE: DELETE /api/v1/workflows/{workflow}
            let _delete_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Delete,
                &format!("{}/workflows/{}", get_base_url(), workflow_id),
                None::<&Value>,
                Some(200),
            )
            .await
            .expect("Failed to delete workflow");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // VERIFY DELETE: GET after DELETE should fail
            let get_after_delete = make_method_request::<Value, Value>(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/workflows/{}", get_base_url(), workflow_id),
                None,
                None,
            )
            .await;

            assert!(get_after_delete.is_err(), "GET after DELETE should fail, but got: {:?}", get_after_delete);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_workflow_get_nonexistent() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // GET a workflow that does not exist
            let get_resp = make_method_request::<Value, Value>(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/workflows/{}", get_base_url(), "nonexistent-workflow-xyz"),
                None,
                None,
            )
            .await;

            assert!(get_resp.is_err(), "GET non-existent workflow should fail, but got: {:?}", get_resp);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_workflow_create_multiple_and_read() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Create multiple workflows
            for i in 1..=3 {
                let body = json!({
                    "id": format!("multi-workflow-{}", i),
                    "description": format!("Workflow number {}", i),
                    "dag": {
                        "nodes": [
                            {"id": format!("step-{}", i), "type": "query", "endpoint": "ep1"}
                        ],
                        "edges": []
                    }
                });

                let _: Option<Value> =
                    make_method_request(&client, token, HttpMethod::Post, &format!("{}/workflows", get_base_url()), Some(&body), Some(201))
                        .await
                        .unwrap_or_else(|_| panic!("Failed to create workflow {}", i));

                tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            }

            // Verify each workflow can be read individually
            for i in 1..=3 {
                let get_resp: Option<Value> = make_method_request(
                    &client,
                    token,
                    HttpMethod::Get,
                    &format!("{}/workflows/multi-workflow-{}", get_base_url(), i),
                    None::<&Value>,
                    None,
                )
                .await
                .unwrap_or_else(|_| panic!("Failed to get workflow {}", i));

                let data = get_resp.expect("Expected response body");
                assert_eq!(
                    data.get("id").and_then(|v| v.as_str()).unwrap_or_default(),
                    format!("multi-workflow-{}", i),
                    "Workflow id should match for workflow {}",
                    i
                );

                tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            }
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
