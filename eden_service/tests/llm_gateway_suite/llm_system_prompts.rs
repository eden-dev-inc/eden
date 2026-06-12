#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_llm_admin_system_prompts_crud() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let prompt_key = "test-prompt-key";

            // CREATE via PUT /api/v1/admin/llm/system-prompts/{prompt_key}
            let create_body = json!({
                "display_name": "Test Prompt",
                "description": "A test",
                "prompt": "You are helpful.",
                "is_active": true,
                "is_default": false
            });

            let upsert_url = format!("{}/admin/llm/system-prompts/{}", get_base_url(), prompt_key);
            let created: Option<Value> = make_method_request(&client, token, HttpMethod::Put, &upsert_url, Some(&create_body), None)
                .await
                .expect("Failed to upsert system prompt");

            let created = created.expect("Expected a response body from upsert");
            let prompt_data = if let Some(data) = created.get("data") {
                data.clone()
            } else {
                created.clone()
            };

            assert_eq!(prompt_data.get("display_name").and_then(Value::as_str).unwrap_or_default(), "Test Prompt");
            assert_eq!(prompt_data.get("prompt").and_then(Value::as_str).unwrap_or_default(), "You are helpful.");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // LIST: GET /api/v1/admin/llm/system-prompts — verify the created prompt appears
            let list_url = format!("{}/admin/llm/system-prompts", get_base_url());
            let listed: Option<Value> = make_method_request(&client, token, HttpMethod::Get, &list_url, None::<&Value>, None)
                .await
                .expect("Failed to list system prompts");

            let listed = listed.expect("Expected a response body from list");
            let prompts_array = if let Some(data) = listed.get("data") {
                data.as_array().expect("data should be an array")
            } else {
                listed.as_array().expect("response should be an array")
            };

            let found = prompts_array.iter().any(|p| {
                p.get("prompt_key").and_then(Value::as_str) == Some(prompt_key)
                    || p.get("display_name").and_then(Value::as_str) == Some("Test Prompt")
            });
            assert!(found, "Created system prompt should appear in the list");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // UPDATE via PUT again with changed values
            let update_body = json!({
                "display_name": "Updated Test Prompt",
                "description": "An updated test",
                "prompt": "You are very helpful.",
                "is_active": true,
                "is_default": false
            });

            let updated: Option<Value> = make_method_request(&client, token, HttpMethod::Put, &upsert_url, Some(&update_body), None)
                .await
                .expect("Failed to update system prompt");

            let updated = updated.expect("Expected a response body from update");
            let updated_data = if let Some(data) = updated.get("data") {
                data.clone()
            } else {
                updated.clone()
            };

            assert_eq!(
                updated_data.get("display_name").and_then(Value::as_str).unwrap_or_default(),
                "Updated Test Prompt",
                "display_name should be updated"
            );
            assert_eq!(
                updated_data.get("prompt").and_then(Value::as_str).unwrap_or_default(),
                "You are very helpful.",
                "prompt should be updated"
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
