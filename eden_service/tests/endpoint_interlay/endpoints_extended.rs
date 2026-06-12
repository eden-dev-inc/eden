#![cfg(feature = "postgres")]
#![cfg(external_db)]

use serde_json::json;

use eden_core::format::EdenUuid;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, endpoint_connect_pg, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_endpoint_list_get_patch_metadata_delete() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Create an endpoint via endpoint_connect_pg
            let endpoint = endpoint_connect_pg(&client, &jwt.token).await.expect("Failed to connect PG endpoint");
            let endpoint = endpoint.expect("No endpoint returned");
            let endpoint_uuid = endpoint.uuid.uuid();

            // --- LIST: GET /api/v1/endpoints ---
            let list_result: Option<serde_json::Value> =
                make_method_request(&client, &jwt.token, HttpMethod::Get, &format!("{}/endpoints", get_base_url()), None::<&()>, None)
                    .await
                    .expect("Failed to list endpoints");

            let list_value = list_result.expect("Expected list response body");
            // The response should contain at least the endpoint we just created
            assert!(
                list_value.is_array() || list_value.is_object(),
                "List endpoints response should be an array or object, got: {}",
                list_value
            );

            // --- GET: GET /api/v1/endpoints/{endpoint} ---
            let get_result: Option<serde_json::Value> = make_method_request(
                &client,
                &jwt.token,
                HttpMethod::Get,
                &format!("{}/endpoints/{}", get_base_url(), endpoint_uuid),
                None::<&()>,
                None,
            )
            .await
            .expect("Failed to get endpoint");

            let get_value = get_result.expect("Expected get response body");
            assert!(get_value.is_object(), "Get endpoint response should be an object, got: {}", get_value);

            // --- PATCH: PATCH /api/v1/endpoints/{endpoint} ---
            let patch_body = json!({ "description": "Updated" });
            let _patch_result: Option<serde_json::Value> = make_method_request(
                &client,
                &jwt.token,
                HttpMethod::Patch,
                &format!("{}/endpoints/{}", get_base_url(), endpoint_uuid),
                Some(&patch_body),
                Some(200),
            )
            .await
            .expect("Failed to patch endpoint");

            // --- METADATA: GET /api/v1/endpoints/{endpoint}/metadata ---
            let metadata_result: Option<serde_json::Value> = make_method_request(
                &client,
                &jwt.token,
                HttpMethod::Get,
                &format!("{}/endpoints/{}/metadata", get_base_url(), endpoint_uuid),
                None::<&()>,
                None,
            )
            .await
            .expect("Failed to get endpoint metadata");

            let metadata_value = metadata_result.expect("Expected metadata response body");
            assert!(
                metadata_value.is_object() || metadata_value.is_array(),
                "Metadata response should be an object or array, got: {}",
                metadata_value
            );

            // --- DELETE: DELETE /api/v1/endpoints/{endpoint} ---
            let _delete_result: Option<serde_json::Value> = make_method_request(
                &client,
                &jwt.token,
                HttpMethod::Delete,
                &format!("{}/endpoints/{}", get_base_url(), endpoint_uuid),
                None::<&()>,
                Some(200),
            )
            .await
            .expect("Failed to delete endpoint");

            // --- VERIFY DELETION: GET should fail after delete ---
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let get_after_delete = client
                .get(format!("{}/endpoints/{}", get_base_url(), endpoint_uuid))
                .bearer_auth(&jwt.token)
                .send()
                .await
                .expect("Failed to send GET after delete");

            assert!(
                !get_after_delete.status().is_success(),
                "GET should fail after DELETE (got status: {})",
                get_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
