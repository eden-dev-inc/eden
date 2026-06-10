#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD, USER2_ID, USER2_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, create_user, get_base_url, make_method_request};
use crate::util::test_server;
use endpoint_core::ep_core::database::schema::user::UserInput;

#[tokio::test]
async fn test_rbac_subject_lifecycle() {
    test_server(
        async || {
            let client = reqwest::Client::default();

            // Create org with superadmin
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Create a second user to use as RBAC subject
            let user2 = UserInput::new(USER2_ID.to_string(), USER2_PWD.to_string(), None, None, None, Default::default());
            create_user(&client, &admin_jwt.token, &user2).await.expect("Failed to create user2");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 1. GET /api/v1/iam/rbac/organizations - get org RBAC info
            let org_rbac: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/rbac/organizations", get_base_url()),
                None,
                None,
            )
            .await
            .expect("Failed to get org RBAC info");
            assert!(org_rbac.is_some(), "Org RBAC info should return a response body");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 2. POST /api/v1/iam/rbac/organizations/subjects - add subject to org
            let add_subject_body = json!({
                "subjects": [[USER2_ID, "RCPGA"]]
            });
            let _: Option<serde_json::Value> = make_method_request::<serde_json::Value, serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Post,
                &format!("{}/iam/rbac/organizations/subjects", get_base_url()),
                Some(&add_subject_body),
                Some(201),
            )
            .await
            .expect("Failed to add RBAC subject");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 3. GET /api/v1/iam/rbac/organizations/subjects/{subject} - get subject from org
            let get_subject: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/rbac/organizations/subjects/{}", get_base_url(), USER2_ID),
                None,
                None,
            )
            .await
            .expect("Failed to get RBAC subject");
            assert!(get_subject.is_some(), "Get RBAC subject should return a response body");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 4. GET /api/v1/iam/rbac/subjects/{subject} - get subject details (superadmin)
            let subject_details: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Get,
                &format!("{}/iam/rbac/subjects/{}", get_base_url(), SUPERADMIN_ID),
                None,
                None,
            )
            .await
            .expect("Failed to get subject details");
            assert!(subject_details.is_some(), "Subject details should return a response body");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // 5. DELETE /api/v1/iam/rbac/organizations/subjects/{subject} - remove subject
            let _: Option<serde_json::Value> = make_method_request::<(), serde_json::Value>(
                &client,
                &admin_jwt.token,
                HttpMethod::Delete,
                &format!("{}/iam/rbac/organizations/subjects/{}", get_base_url(), USER2_ID),
                None,
                Some(200),
            )
            .await
            .expect("Failed to remove RBAC subject");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Verify subject is removed: GET should fail after deletion
            let get_after_delete = client
                .get(format!("{}/iam/rbac/organizations/subjects/{}", get_base_url(), USER2_ID))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed API");

            assert!(
                !get_after_delete.status().is_success(),
                "RBAC subject should not exist after deletion (got status: {})",
                get_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
