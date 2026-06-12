#![cfg(feature = "postgres")]
#![cfg(external_db)]

use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD, USER2_ID, USER2_PWD};
use crate::request::{
    HttpMethod, auth_login, create_org_with_superadmin, create_user, endpoint_connect_pg, get_base_url, make_method_request,
};
use crate::util::test_server;
use eden_core::format::EdenUuid;
use endpoint_core::ep_core::database::schema::user::UserInput;

#[tokio::test]
async fn test_rbac_endpoint_subjects() {
    test_server(
        async || {
            let client = reqwest::Client::default();

            // 1. Create org + superadmin, log in
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // 2. Create a second user
            let user2 = UserInput::new(USER2_ID.to_string(), USER2_PWD.to_string(), None, None, None, Default::default());
            create_user(&client, &admin_jwt.token, &user2).await.expect("Failed to create user2");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 3. Connect a postgres endpoint
            let response = endpoint_connect_pg(&client, &admin_jwt.token).await.expect("Failed to connect pg endpoint");
            let ep_response = response.expect("No response from endpoint connect");
            let endpoint_uuid = ep_response.uuid.uuid();

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 4. PUT /api/v1/iam/control/endpoints/{endpoint}/subjects/{subject} - grant USER2 "R"
            let add_subject_body = json!({ "perms": "R" });
            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Put,
                &format!("{}/iam/control/endpoints/{}/subjects/{}", get_base_url(), endpoint_uuid, USER2_ID),
                Some(&add_subject_body),
                Some(200),
            )
            .await
            .expect("Failed to add endpoint RBAC subject");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 5. GET /api/v1/iam/control/endpoints/{endpoint} - verify RBAC info
            let ep_rbac: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/control/endpoints/{}", get_base_url(), endpoint_uuid),
                None,
                None,
            )
            .await
            .expect("Failed to get endpoint RBAC info");
            assert!(ep_rbac.is_some(), "Endpoint RBAC info should return a response body");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 6. GET /api/v1/iam/control/endpoints/{endpoint}/subjects/{subject} - get subject
            let get_subject: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/control/endpoints/{}/subjects/{}", get_base_url(), endpoint_uuid, USER2_ID),
                None,
                None,
            )
            .await
            .expect("Failed to get endpoint RBAC subject");
            assert!(get_subject.is_some(), "Get endpoint RBAC subject should return a response body");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 7. DELETE /api/v1/iam/control/endpoints/{endpoint}/subjects/{subject} - remove subject
            let _: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Delete,
                &format!("{}/iam/control/endpoints/{}/subjects/{}", get_base_url(), endpoint_uuid, USER2_ID),
                None,
                Some(200),
            )
            .await
            .expect("Failed to remove endpoint RBAC subject");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify subject is removed — GET returns 200 with empty perms after DELETE
            let after_delete: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/control/endpoints/{}/subjects/{}", get_base_url(), endpoint_uuid, USER2_ID),
                None,
                None,
            )
            .await
            .expect("Failed to re-fetch endpoint RBAC subject");
            let after_delete = after_delete.expect("Expected response body after delete");
            let perms_str = after_delete.get("data").and_then(|d| d.as_str()).or_else(|| after_delete.as_str()).unwrap_or_default();
            assert!(
                perms_str.is_empty() || perms_str == "-----",
                "Expected empty perms after DELETE, got: {}",
                after_delete
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

// Template and workflow RBAC path coverage is deferred: POST /templates and
// POST /workflows currently reject the simple bodies these tests need (the
// existing templates/workflows CRUD tests have the same problem), so wiring
// the RBAC smoke-tests would require significant setup unrelated to the RBAC
// path change under test. The endpoint case above exercises the same
// /iam/control/{entity}/{id}/subjects/{subject} routing end-to-end.
