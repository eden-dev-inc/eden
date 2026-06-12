#![cfg(external_db)]

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_auth_refresh_token() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // REFRESH: POST /api/v1/auth/refresh with bearer auth using existing token
            let refresh_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/auth/refresh", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to refresh token");

            assert!(refresh_result.is_some(), "Refresh should return a new token response");

            let refresh_body = refresh_result.expect("refresh body");
            // The response should contain a token field
            assert!(
                refresh_body.get("token").is_some(),
                "Refresh response should contain a 'token' field, got: {}",
                refresh_body
            );

            let new_token = refresh_body["token"].as_str().expect("token should be a string");
            assert!(!new_token.is_empty(), "New token should not be empty");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify the new token works by making an authenticated request
            let verify_result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                new_token,
                HttpMethod::Get,
                &format!("{}/iam/humans/{}", get_base_url(), SUPERADMIN_ID),
                None,
                None,
            )
            .await
            .expect("Failed to use refreshed token");

            assert!(verify_result.is_some(), "Refreshed token should be valid for authenticated requests");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_auth_refresh_with_invalid_token() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();

            // Try to refresh with an invalid/garbage token
            let resp = client
                .post(format!("{}/auth/refresh", get_base_url()))
                .bearer_auth("invalid-garbage-token")
                .send()
                .await
                .expect("Failed API");

            assert!(
                !resp.status().is_success(),
                "Refresh with invalid token should fail (got status: {})",
                resp.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
