#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_users_me() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // GET /api/v1/iam/humans/me — get current user (should be superadmin)
            let me_url = format!("{}/iam/humans/me", get_base_url());
            let me_resp: Option<Value> = make_method_request(&client, token, HttpMethod::Get, &me_url, None::<&Value>, None)
                .await
                .expect("Failed to get /iam/humans/me");

            let me_resp = me_resp.expect("Expected a response body from /iam/humans/me");
            let user_data = if let Some(data) = me_resp.get("data") {
                data.clone()
            } else {
                me_resp.clone()
            };

            // The returned user should be the superadmin
            let username = user_data.get("username").and_then(Value::as_str).unwrap_or_default();
            assert_eq!(username, SUPERADMIN_ID, "Current user should be the superadmin");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // PATCH /api/v1/iam/humans/me — update own profile
            let patch_body = json!({
                "bio": "Updated bio"
            });

            let patched: Option<Value> = make_method_request(&client, token, HttpMethod::Patch, &me_url, Some(&patch_body), None)
                .await
                .expect("Failed to patch /iam/humans/me");

            let patched = patched.expect("Expected a response body from PATCH /iam/humans/me");
            let patched_data = if let Some(data) = patched.get("data") {
                data.clone()
            } else {
                patched.clone()
            };

            assert_eq!(
                patched_data.get("bio").and_then(Value::as_str).unwrap_or_default(),
                "Updated bio",
                "bio should be updated"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // GET /api/v1/iam/humans/me again — verify the update persisted
            let me_after: Option<Value> = make_method_request(&client, token, HttpMethod::Get, &me_url, None::<&Value>, None)
                .await
                .expect("Failed to get /iam/humans/me after update");

            let me_after = me_after.expect("Expected a response body from /iam/humans/me after update");
            let after_data = if let Some(data) = me_after.get("data") {
                data.clone()
            } else {
                me_after.clone()
            };

            assert_eq!(
                after_data.get("bio").and_then(Value::as_str).unwrap_or_default(),
                "Updated bio",
                "bio should persist after re-fetch"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // GET /api/v1/iam/humans — list all users
            let users_url = format!("{}/iam/humans", get_base_url());
            let users_resp: Option<Value> = make_method_request(&client, token, HttpMethod::Get, &users_url, None::<&Value>, None)
                .await
                .expect("Failed to list users");

            let users_resp = users_resp.expect("Expected a response body from /iam/humans");
            let users_array = users_resp.get("humans").and_then(Value::as_array).expect("response should contain a `humans` array");

            assert!(!users_array.is_empty(), "Users list should not be empty");

            let found_superadmin = users_array.iter().any(|u| u.get("username").and_then(Value::as_str) == Some(SUPERADMIN_ID));
            assert!(found_superadmin, "Superadmin should appear in the users list");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
