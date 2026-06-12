#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_template_crud_lifecycle() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let template_id = "test-template-1";
            let template_body = json!({
                "id": template_id,
                "description": "A test template",
                "template": {
                    "type": "sql",
                    "content": "SELECT * FROM users WHERE id = :id"
                }
            });

            // CREATE: POST /api/v1/templates
            let create_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/templates", get_base_url()),
                Some(&template_body),
                Some(201),
            )
            .await
            .expect("Failed to create template");
            println!("Create response: {:?}", create_resp);

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // LIST: GET /api/v1/templates
            let list_resp: Option<Value> =
                make_method_request(&client, token, HttpMethod::Get, &format!("{}/templates", get_base_url()), None::<&Value>, None)
                    .await
                    .expect("Failed to list templates");

            let list_data = list_resp.expect("Expected list response body");
            println!("List response: {:?}", list_data);
            // Verify the created template appears in the list
            if let Some(arr) = list_data.as_array() {
                let found = arr.iter().any(|t| t.get("id").and_then(|v| v.as_str()) == Some(template_id));
                assert!(found, "Created template should appear in list, got: {}", list_data);
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ: GET /api/v1/templates/{template}
            let get_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/templates/{}", get_base_url(), template_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get template");

            let get_data = get_resp.expect("Expected get response body");
            println!("Get response: {:?}", get_data);
            assert_eq!(
                get_data.get("id").and_then(|v| v.as_str()).unwrap_or_default(),
                template_id,
                "Template id should match"
            );
            assert_eq!(
                get_data.get("description").and_then(|v| v.as_str()).unwrap_or_default(),
                "A test template",
                "Template description should match"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // UPDATE: PATCH /api/v1/templates/{template}
            let update_body = json!({
                "description": "Updated test template description"
            });

            let _update_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Patch,
                &format!("{}/templates/{}", get_base_url(), template_id),
                Some(&update_body),
                Some(200),
            )
            .await
            .expect("Failed to update template");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // VERIFY UPDATE: GET after PATCH
            let get_after_update: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/templates/{}", get_base_url(), template_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get template after update");

            let updated_data = get_after_update.expect("Expected updated response body");
            assert_eq!(
                updated_data.get("description").and_then(|v| v.as_str()).unwrap_or_default(),
                "Updated test template description",
                "Template description should be updated"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE: DELETE /api/v1/templates/{template}
            let _delete_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Delete,
                &format!("{}/templates/{}", get_base_url(), template_id),
                None::<&Value>,
                Some(200),
            )
            .await
            .expect("Failed to delete template");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // VERIFY DELETE: GET after DELETE should fail
            let get_after_delete = make_method_request::<Value, Value>(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/templates/{}", get_base_url(), template_id),
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
async fn test_template_create_and_list_multiple() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Create multiple templates
            for i in 1..=3 {
                let body = json!({
                    "id": format!("multi-template-{}", i),
                    "description": format!("Template number {}", i),
                    "template": {
                        "type": "sql",
                        "content": format!("SELECT {} FROM test", i)
                    }
                });

                let _: Option<Value> =
                    make_method_request(&client, token, HttpMethod::Post, &format!("{}/templates", get_base_url()), Some(&body), Some(201))
                        .await
                        .unwrap_or_else(|_| panic!("Failed to create template {}", i));

                tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            }

            // List all templates and verify count
            let list_resp: Option<Value> =
                make_method_request(&client, token, HttpMethod::Get, &format!("{}/templates", get_base_url()), None::<&Value>, None)
                    .await
                    .expect("Failed to list templates");

            let list_data = list_resp.expect("Expected list response body");
            if let Some(arr) = list_data.as_array() {
                assert!(arr.len() >= 3, "Should have at least 3 templates, got {}", arr.len());
            }
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_template_get_nonexistent() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // GET a template that does not exist
            let get_resp = make_method_request::<Value, Value>(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/templates/{}", get_base_url(), "nonexistent-template-xyz"),
                None,
                None,
            )
            .await;

            assert!(get_resp.is_err(), "GET non-existent template should fail, but got: {:?}", get_resp);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
