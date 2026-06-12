#![cfg(external_db)]
#![allow(unused_variables)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_llm_credentials_crud() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // --- CREATE: POST /api/v1/llm/credentials ---
            let create_body = json!({
                "provider": "openai",
                "label": "Test OpenAI Key",
                "description": "Integration test credential",
                "api_key": "sk-test-key-1234567890abcdef"
            });

            let create_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/llm/credentials", get_base_url()),
                Some(&create_body),
                Some(201),
            )
            .await
            .expect("Failed to create LLM credential");

            // Re-fetch to get the response body (make_method_request returns None when expect_status is set)
            // Instead, create and parse in one call without expect_status
            let create_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/llm/credentials", get_base_url()),
                Some(&json!({
                    "provider": "openai",
                    "label": "Test OpenAI Key",
                    "description": "Integration test credential",
                    "api_key": "sk-test-key-1234567890abcdef"
                })),
                None,
            )
            .await
            .expect("Failed to create LLM credential");

            let created = create_resp.expect("Expected credential response body");
            let credential_id = created.get("id").expect("Response missing 'id'").as_str().expect("'id' is not a string").to_string();

            assert_eq!(
                created.get("provider").and_then(|v| v.as_str()).unwrap_or_default(),
                "openai",
                "Provider should be openai"
            );
            assert_eq!(
                created.get("label").and_then(|v| v.as_str()).unwrap_or_default(),
                "Test OpenAI Key",
                "Label should match"
            );
            assert!(
                created.get("has_api_key").and_then(|v| v.as_bool()).unwrap_or_default(),
                "has_api_key should be true"
            );
            assert_eq!(
                created.get("api_key_last_four").and_then(|v| v.as_str()).unwrap_or_default(),
                "cdef",
                "api_key_last_four should show last 4 chars"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // --- LIST: GET /api/v1/llm/credentials ---
            let list_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/llm/credentials", get_base_url()),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to list LLM credentials");

            let credentials = list_resp.expect("Expected list response body");
            let creds_array = credentials.as_array().expect("Expected array of credentials");
            assert!(
                creds_array.iter().any(|c| c.get("id").and_then(|v| v.as_str()) == Some(&credential_id)),
                "Created credential should appear in list"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // --- UPDATE: PATCH /api/v1/llm/credentials/{credential_id} ---
            let update_body = json!({
                "label": "Updated Label"
            });

            let update_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Patch,
                &format!("{}/llm/credentials/{}", get_base_url(), credential_id),
                Some(&update_body),
                None,
            )
            .await
            .expect("Failed to update LLM credential");

            let updated = update_resp.expect("Expected update response body");
            assert_eq!(
                updated.get("label").and_then(|v| v.as_str()).unwrap_or_default(),
                "Updated Label",
                "Label should be updated"
            );
            assert_eq!(
                updated.get("id").and_then(|v| v.as_str()).unwrap_or_default(),
                credential_id,
                "Credential ID should remain the same"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // --- DELETE: DELETE /api/v1/llm/credentials/{credential_id} ---
            let _: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Delete,
                &format!("{}/llm/credentials/{}", get_base_url(), credential_id),
                None::<&Value>,
                Some(204),
            )
            .await
            .expect("Failed to delete LLM credential");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // --- VERIFY DELETE: GET /api/v1/llm/credentials ---
            let list_after_delete: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/llm/credentials", get_base_url()),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to list LLM credentials after delete");

            let remaining = list_after_delete.expect("Expected list response body");
            let remaining_array = remaining.as_array().expect("Expected array of credentials");
            assert!(
                !remaining_array.iter().any(|c| c.get("id").and_then(|v| v.as_str()) == Some(&credential_id)),
                "Deleted credential should not appear in list"
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
