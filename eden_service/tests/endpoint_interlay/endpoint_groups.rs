#![cfg(feature = "postgres")]
#![cfg(external_db)]

use eden_core::format::EdenUuid;
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, endpoint_connect_pg, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_endpoint_group_crud_lifecycle() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Connect a postgres endpoint so we have a member to add later
            let endpoint = endpoint_connect_pg(&client, token).await.expect("Failed to connect pg endpoint");
            let endpoint_uuid = endpoint.expect("endpoint response").uuid;

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── CREATE: POST /api/v1/endpoint-groups ──
            let group_id = "test-group-1";
            let group_description = "Integration test endpoint group";
            let create_body = json!({
                "id": group_id,
                "description": group_description,
                "ep_kind": "postgres",
                "members": []
            });

            let _: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/endpoint-groups", get_base_url()),
                Some(&create_body),
                Some(201),
            )
            .await
            .expect("Failed to create endpoint group");

            // POST with expect_status returns None; fetch it via GET to verify
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── GET single: GET /api/v1/endpoint-groups/{group} ──
            let fetched: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get endpoint group");

            let fetched = fetched.expect("Expected response body from GET");
            // EdenResponse wraps in "data"
            let group_data = fetched.get("data").unwrap_or(&fetched);
            assert_eq!(group_data.get("id").and_then(|v| v.as_str()).unwrap_or_default(), group_id, "Group id should match");
            assert_eq!(
                group_data.get("description").and_then(|v| v.as_str()).unwrap_or_default(),
                group_description,
                "Group description should match"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── LIST: GET /api/v1/endpoint-groups ──
            let list: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/endpoint-groups", get_base_url()),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to list endpoint groups");

            let list = list.expect("Expected response body from GET all");
            // The response may be wrapped in "data" or be an array directly
            let list_data = list.get("data").unwrap_or(&list);
            let groups_array = list_data.as_array().expect("Expected array of groups");
            assert!(
                groups_array.iter().any(|g| g.get("id").and_then(|v| v.as_str()) == Some(group_id)),
                "Created group should appear in list"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── UPDATE: PATCH /api/v1/endpoint-groups/{group} ──
            let updated_description = "Updated integration test group";
            let patch_body = json!({
                "description": updated_description
            });

            let _: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Patch,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                Some(&patch_body),
                None,
            )
            .await
            .expect("Failed to update endpoint group");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify update via GET
            let after_patch: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get endpoint group after patch");

            let after_patch = after_patch.expect("Expected response body from GET after patch");
            let patched_data = after_patch.get("data").unwrap_or(&after_patch);
            assert_eq!(
                patched_data.get("description").and_then(|v| v.as_str()).unwrap_or_default(),
                updated_description,
                "Description should be updated"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── ADD MEMBER: POST /api/v1/endpoint-groups/{group}/members ──
            let member_body = json!({
                "endpoint_uuid": endpoint_uuid.uuid().to_string()
            });

            let _: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/endpoint-groups/{}/members", get_base_url(), group_id),
                Some(&member_body),
                None,
            )
            .await
            .expect("Failed to add member to endpoint group");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify member was added via GET
            let with_member: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get endpoint group after adding member");

            let with_member = with_member.expect("Expected response body");
            let member_data = with_member.get("data").unwrap_or(&with_member);
            let members = member_data.get("members").and_then(|v| v.as_array()).expect("Expected members array");
            assert!(!members.is_empty(), "Members list should not be empty after adding a member");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── REMOVE MEMBER: DELETE /api/v1/endpoint-groups/{group}/members/{endpoint} ──
            let _: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Delete,
                &format!("{}/endpoint-groups/{}/members/{}", get_base_url(), group_id, endpoint_uuid.uuid()),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to remove member from endpoint group");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify member was removed
            let without_member: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                None::<&Value>,
                None,
            )
            .await
            .expect("Failed to get endpoint group after removing member");

            let without_member = without_member.expect("Expected response body");
            let no_member_data = without_member.get("data").unwrap_or(&without_member);
            let members_after = no_member_data.get("members").and_then(|v| v.as_array()).expect("Expected members array");
            assert!(members_after.is_empty(), "Members list should be empty after removing the member");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // ── DELETE: DELETE /api/v1/endpoint-groups/{group} ──
            let _: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Delete,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                None::<&Value>,
                Some(204),
            )
            .await
            .expect("Failed to delete endpoint group");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify deletion: GET should fail
            let after_delete_result: Result<Option<Value>, _> = make_method_request(
                &client,
                token,
                HttpMethod::Get,
                &format!("{}/endpoint-groups/{}", get_base_url(), group_id),
                None::<&Value>,
                None,
            )
            .await;

            assert!(after_delete_result.is_err(), "GET on deleted endpoint group should fail");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
