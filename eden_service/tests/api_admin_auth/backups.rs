#![cfg(feature = "postgres")]
#![cfg(external_db)]

use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, endpoint_connect_pg, get_base_url, make_method_request};
use crate::util::test_server;

use eden_core::format::EdenUuid;

#[tokio::test]
async fn test_list_backups() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // LIST: GET /api/v1/backups
            let result: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/backups", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to list backups");

            assert!(result.is_some(), "List backups should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_create_backup() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Connect an endpoint first
            let ep_response = endpoint_connect_pg(&client, &admin_jwt.token).await.expect("Failed to connect endpoint");
            let ep = ep_response.expect("No endpoint response returned");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // CREATE: POST /api/v1/backups
            let body = json!({
                "endpoint_uuid": ep.uuid.uuid().to_string()
            });

            let result: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/backups", get_base_url()),
                Some(&body),
                None,
            )
            .await
            .expect("Failed to create backup");

            assert!(result.is_some(), "Create backup should return a response body");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
