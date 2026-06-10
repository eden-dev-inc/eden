#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_json_flatten() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "data": { "a": { "b": 1, "c": 2 } }
            });

            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/json/flatten", get_base_url()),
                Some(&body),
                Some(200),
            )
            .await
            .expect("Failed to call json/flatten");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_json_unflatten() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "data": { "a.b": 1, "a.c": 2 }
            });

            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/json/unflatten", get_base_url()),
                Some(&body),
                Some(200),
            )
            .await
            .expect("Failed to call json/unflatten");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_json_parse() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "data": "{\"key\": \"value\"}"
            });

            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/json/parse", get_base_url()),
                Some(&body),
                Some(200),
            )
            .await
            .expect("Failed to call json/parse");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_json_map() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "data": { "name": "test" },
                "mapping": { "name": "label" }
            });

            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/json/map", get_base_url()),
                Some(&body),
                Some(200),
            )
            .await
            .expect("Failed to call json/map");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_json_reduce() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let body = json!({
                "data": [1, 2, 3]
            });

            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/json/reduce", get_base_url()),
                Some(&body),
                Some(200),
            )
            .await
            .expect("Failed to call json/reduce");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
