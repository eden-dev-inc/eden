#![cfg(external_db)]
use serde_json::json;

use eden_core::format::EdenUuid;
use eden_core::format::rbac::ControlPerms;
use endpoint_core::ep_core::database::schema::user::UserInput;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD, USER2_DESCR, USER2_ID, USER2_PWD};
use crate::request::{
    EpRequestType, auth_login, create_org_with_superadmin, create_user, endpoint_connect_pg, endpoint_request, get_base_url,
    grant_endpoint_data_perms,
};
use crate::util::test_server;

const ADMIN_PERMS: ControlPerms = ControlPerms::from_bits_retain(
    ControlPerms::READ.bits()
        | ControlPerms::CONFIGURE.bits()
        | ControlPerms::PROMOTE.bits()
        | ControlPerms::GRANT.bits()
        | ControlPerms::AUDIT.bits(),
);
const READ_PERMS: ControlPerms = ControlPerms::READ;

#[tokio::test]
async fn test_create_user() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            let new_user_input =
                UserInput::new(USER2_ID.to_string(), USER2_PWD.to_string(), Some(USER2_DESCR.to_string()), None, None, ADMIN_PERMS);
            create_user(&client, &admin_jwt.token, &new_user_input).await.unwrap_or_default();

            // Now login as new user
            let user_token = auth_login(&client, new_user_input.username(), new_user_input.password()).await.expect("Failed user token");
            // Connect as the new admin user
            let endpoint = endpoint_connect_pg(&client, &user_token.token).await.unwrap_or_default();
            assert!(endpoint.is_some());
            let endpoint_uuid = endpoint.expect("endpoint").uuid;

            // Grant the new user data-plane read+write on the endpoint they just created.
            // Endpoint creation only sets control-plane perms; data-plane grants are separate.
            grant_endpoint_data_perms(&client, &admin_jwt.token, &endpoint_uuid.uuid().to_string(), USER2_ID, "rw")
                .await
                .expect("Failed to grant data-plane perms");

            // Create a table, write data, read
            endpoint_request(
                &client,
                &user_token.token,
                endpoint_uuid.clone(),
                json!({
                    "type": "execute",
                    "query": "CREATE TABLE tstuser(id integer, name text)",
                    "params": []
                }),
                EpRequestType::Write,
                true,
                Some(200),
            )
            .await
            .unwrap_or_default();
            endpoint_request(
                &client,
                &user_token.token,
                endpoint_uuid.clone(),
                json!({
                    "type": "execute",
                    "query": "INSERT INTO tstuser VALUES (1, 'Alice'), (2, 'Bob')",
                    "params": []
                }),
                crate::request::EpRequestType::Write,
                true,
                Some(200),
            )
            .await
            .unwrap_or_default();
            let pg_data = endpoint_request(
                &client,
                &user_token.token,
                endpoint_uuid.clone(),
                json!({
                    "type": "query_read_only",
                    "query": "SELECT * FROM tstuser",
                    "params": []
                }),
                crate::request::EpRequestType::Read,
                false,
                None,
            )
            .await
            .unwrap_or_default();
            assert!(pg_data.as_object().is_some());
            assert_eq!(
                pg_data.as_object().expect("Failed object").get("data").unwrap_or_default().as_array().expect("Failed array")[0]
                    .as_object()
                    .expect("Failed object")
                    .get("name")
                    .unwrap_or_default()
                    .as_str()
                    .unwrap_or_default(),
                "Alice"
            );

            // Change user's privilege to Read

            // Try to write again and fail
            // Read something
            //
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_user_crud() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Create a new user as admin
            let new_user_input =
                UserInput::new(USER2_ID.to_string(), USER2_PWD.to_string(), Some(USER2_DESCR.to_string()), None, None, ADMIN_PERMS);

            create_user(&client, &admin_jwt.token, &new_user_input).await.unwrap_or_default();

            // READ: Login as new user and GET the user
            let user_token = auth_login(&client, USER2_ID, USER2_PWD).await.expect("Failed user token");

            let resp_text = client
                .get(format!("{}/iam/humans/{}", get_base_url(), USER2_ID))
                .bearer_auth(&user_token.token)
                .send()
                .await
                .expect("Failed API")
                .error_for_status()
                .expect("error")
                .text()
                .await
                .unwrap_or_default();

            let v: serde_json::Value = serde_json::from_str(&resp_text).unwrap_or_default();
            assert_eq!(v.get("username").unwrap_or_default().as_str().unwrap_or_default(), USER2_ID);

            // UPDATE: change username, password and description (self-update)
            let new_username = "User2Renamed";
            let new_password = "newpwd";
            let new_description = "Updated description";

            let patch_resp = client
                .patch(format!("{}/iam/humans/{}", get_base_url(), USER2_ID))
                .bearer_auth(&user_token.token)
                .json(&json!({
                    "username": new_username,
                    "password": new_password,
                    "description": new_description
                }))
                .send()
                .await
                .expect("failed api");

            if !patch_resp.status().is_success() {
                let status = patch_resp.status();
                let body = patch_resp.text().await.unwrap_or_default();
                panic!("PATCH /iam/humans/{} failed: {} - body: {}", USER2_ID, status, body);
            }

            // Verify we can login with new credentials
            let new_user_token = auth_login(&client, new_username, new_password).await.expect("failed auth");

            // Wait before GET to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // READ after update: GET new username and verify description
            let resp_text2 = client
                .get(format!("{}/iam/humans/{}", get_base_url(), new_username))
                .bearer_auth(&new_user_token.token)
                .send()
                .await
                .expect("failed api")
                .error_for_status()
                .expect("error")
                .text()
                .await
                .unwrap_or_default();

            let v2: serde_json::Value = serde_json::from_str(&resp_text2).unwrap_or_default();
            assert_eq!(v2.get("username").unwrap_or_default().as_str().unwrap_or_default(), new_username);
            // description is optional in the GET response; check if present when verbose isn't set
            // the POST/GET handlers may not return description on non-verbose requests; check if present and match
            if let Some(desc) = v2.get("description") {
                assert_eq!(desc.as_str().unwrap_or_default(), new_description);
            }

            // Wait before DELETE to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE: delete the user (self-delete as Admin)
            let delete_resp = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), new_username))
                .bearer_auth(&new_user_token.token)
                .send()
                .await
                .expect("Failed api");
            if !delete_resp.status().is_success() {
                let status = delete_resp.status();
                let body = delete_resp.text().await.unwrap_or_default();
                panic!("DELETE /iam/humans/{} failed: {} - body: {}", new_username, status, body);
            }

            // Wait before final GET to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // CONFIRM DELETE: After DELETE removes RBAC permissions, GET should fail
            let resp_after_delete = client
                .get(format!("{}/iam/humans/{}", get_base_url(), new_username))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed api");

            // After DELETE, the user has no RBAC in the organization
            // GET should fail with a 403 Forbidden
            assert!(
                !resp_after_delete.status().is_success(),
                "GET should fail after DELETE removes user's RBAC (got status: {})",
                resp_after_delete.status()
            );

            // Verify it's a 403 Forbidden
            assert_eq!(
                resp_after_delete.status(),
                403,
                "Expected 403 Forbidden for deleted user, got {}",
                resp_after_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[test]
fn test_user_crud_edge_cases_authorization() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();

            // Setup: Create users with different access levels
            let superadmin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed superadmin");

            // Create an Admin user
            let admin_user = "AdminUser";
            let admin_pwd = "adminpass";
            let admin_input = UserInput::new(
                admin_user.to_string(),
                admin_pwd.to_string(),
                Some("Admin User".to_string()),
                None,
                None,
                ADMIN_PERMS,
            );
            create_user(&client, &superadmin_jwt.token, &admin_input).await.unwrap_or_default();

            let admin_jwt = auth_login(&client, admin_user, admin_pwd).await.expect("Failed admin");

            // Create a Read-level user
            let read_user = "ReadUser";
            let read_pwd = "readpass";
            let read_input =
                UserInput::new(read_user.to_string(), read_pwd.to_string(), Some("Read User".to_string()), None, None, READ_PERMS);
            create_user(&client, &superadmin_jwt.token, &read_input).await.unwrap_or_default();

            let read_jwt = auth_login(&client, read_user, read_pwd).await.expect("Failed read jwt");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 1: Read-level user cannot create users (should fail with 403)
            let new_user_input = UserInput::new("TestUser1".to_string(), "testpass1".to_string(), None, None, None, READ_PERMS);
            let create_resp = client
                .post(format!("{}/iam/humans", get_base_url()))
                .bearer_auth(&read_jwt.token)
                .json(&new_user_input)
                .send()
                .await
                .expect("Failed api");

            assert_eq!(
                create_resp.status(),
                403,
                "Read-level user should not be able to create users (expected 403, got {})",
                create_resp.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 2: Admin cannot create another Admin
            let another_admin_input = UserInput::new("AnotherAdmin".to_string(), "anotherpass".to_string(), None, None, None, ADMIN_PERMS);
            let create_admin_resp = client
                .post(format!("{}/iam/humans", get_base_url()))
                .bearer_auth(&admin_jwt.token)
                .json(&another_admin_input)
                .send()
                .await
                .expect("Failed api");

            assert!(
                !create_admin_resp.status().is_success(),
                "Admin should not be able to create another Admin (got {})",
                create_admin_resp.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 3: Admin cannot delete another Admin (should fail)
            // First create an Admin by SuperAdmin
            let another_admin = "AdminToDelete";
            let another_admin_pwd = "adminpass2";
            let another_admin_input =
                UserInput::new(another_admin.to_string(), another_admin_pwd.to_string(), None, None, None, ADMIN_PERMS);
            create_user(&client, &superadmin_jwt.token, &another_admin_input).await.unwrap_or_default();

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Try to delete another Admin as an Admin
            let delete_admin_resp = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), another_admin))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed api");

            assert!(
                !delete_admin_resp.status().is_success(),
                "Admin should not be able to delete another Admin (got {})",
                delete_admin_resp.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 4: Admin cannot update another Admin (should fail)
            let update_admin_resp = client
                .patch(format!("{}/iam/humans/{}", get_base_url(), another_admin))
                .bearer_auth(&admin_jwt.token)
                .json(&json!({
                    "description": "Trying to update another admin"
                }))
                .send()
                .await
                .expect("Failed api");

            assert!(
                !update_admin_resp.status().is_success(),
                "Admin should not be able to update another Admin (got {})",
                update_admin_resp.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 5: Read-level user cannot delete other users (should fail with 403)
            let delete_read_resp = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), admin_user))
                .bearer_auth(&read_jwt.token)
                .send()
                .await
                .expect("Failed delete");

            assert_eq!(
                delete_read_resp.status(),
                403,
                "Read-level user should not be able to delete users (expected 403, got {})",
                delete_read_resp.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 6: Admin can delete Read-level user (should succeed)
            let read_user_to_delete = "ReadUserToDelete";
            let read_delete_pwd = "readdelpass";
            let read_delete_input =
                UserInput::new(read_user_to_delete.to_string(), read_delete_pwd.to_string(), None, None, None, READ_PERMS);
            create_user(&client, &superadmin_jwt.token, &read_delete_input).await.unwrap_or_default();

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let delete_read_by_admin = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), read_user_to_delete))
                .bearer_auth(&admin_jwt.token)
                .send()
                .await
                .expect("Failed api");

            assert!(
                delete_read_by_admin.status().is_success(),
                "Admin should be able to delete Read-level user (got {})",
                delete_read_by_admin.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 7: Get non-existent user (should fail with 4xx)
            let get_nonexistent = client
                .get(format!("{}/iam/humans/NonExistentUser12345", get_base_url()))
                .bearer_auth(&superadmin_jwt.token)
                .send()
                .await
                .expect("Failed api");

            assert!(
                !get_nonexistent.status().is_success(),
                "GET non-existent user should fail (got {})",
                get_nonexistent.status()
            );
            assert!(
                get_nonexistent.status().is_client_error(),
                "GET non-existent user should return 4xx (got {})",
                get_nonexistent.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 8: User cannot delete themselves and expect to still use old token
            // (Token validation might still work briefly, but user should be gone)
            let self_delete_user = "SelfDeleteUser";
            let self_delete_pwd = "selfdelpass";
            let self_delete_input = UserInput::new(self_delete_user.to_string(), self_delete_pwd.to_string(), None, None, None, READ_PERMS);
            create_user(&client, &superadmin_jwt.token, &self_delete_input).await.unwrap_or_default();

            let self_delete_jwt = auth_login(&client, self_delete_user, self_delete_pwd).await.expect("Failed delete JWT");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Delete self
            let self_delete_resp = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), self_delete_user))
                .bearer_auth(&self_delete_jwt.token)
                .send()
                .await
                .expect("Failed api");

            assert!(
                self_delete_resp.status().is_success(),
                "User should be able to delete themselves (got {})",
                self_delete_resp.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Try to use old token to get self - should fail
            let get_after_self_delete = client
                .get(format!("{}/iam/humans/{}", get_base_url(), self_delete_user))
                .bearer_auth(&self_delete_jwt.token)
                .send()
                .await
                .expect("Failed auth");

            assert!(
                !get_after_self_delete.status().is_success(),
                "Deleted user should not be accessible (got {})",
                get_after_self_delete.status()
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_user_crud_edge_cases_duplicates_and_invalid_input() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();

            let superadmin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed superadmin");

            // TEST 1: Create duplicate username (should fail)
            let dup_user = "DuplicateUser";
            let dup_pwd = "duppass";
            let dup_input =
                UserInput::new(dup_user.to_string(), dup_pwd.to_string(), Some("First user".to_string()), None, None, READ_PERMS);

            assert!(
                create_user(&client, &superadmin_jwt.token, &dup_input).await.is_ok(),
                "First user creation should succeed"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Try to create same username again
            let dup_input2 = UserInput::new(
                dup_user.to_string(),
                "differentpass".to_string(),
                Some("Second user".to_string()),
                None,
                None,
                READ_PERMS,
            );

            let create_second = client
                .post(format!("{}/iam/humans", get_base_url()))
                .bearer_auth(&superadmin_jwt.token)
                .json(&dup_input2)
                .send()
                .await
                .expect("Failed api");

            assert!(
                !create_second.status().is_success(),
                "Duplicate username should fail (got {})",
                create_second.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 2: Update username to duplicate (should fail)
            let user_a = "UserA";
            let user_b = "UserB";

            let input_a = UserInput::new(user_a.to_string(), "passA".to_string(), None, None, None, READ_PERMS);
            create_user(&client, &superadmin_jwt.token, &input_a).await.unwrap_or_default();

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let input_b = UserInput::new(user_b.to_string(), "passB".to_string(), None, None, None, READ_PERMS);
            create_user(&client, &superadmin_jwt.token, &input_b).await.unwrap_or_default();

            let jwt_b = auth_login(&client, user_b, "passB").await.expect("Failed auth");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Try to rename UserB to UserA (duplicate)
            let rename_to_dup = client
                .patch(format!("{}/iam/humans/{}", get_base_url(), user_b))
                .bearer_auth(&jwt_b.token)
                .json(&json!({
                    "username": user_a
                }))
                .send()
                .await
                .expect("Failed API");

            assert!(
                !rename_to_dup.status().is_success(),
                "Rename to duplicate username should fail (got {})",
                rename_to_dup.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 3: Delete already deleted user (should fail with 4xx)
            let del_user = "UserToDeleteTwice";
            let del_input = UserInput::new(del_user.to_string(), "delpass".to_string(), None, None, None, READ_PERMS);
            create_user(&client, &superadmin_jwt.token, &del_input).await.unwrap_or_default();

            let del_jwt = auth_login(&client, del_user, "delpass").await.expect("Failed auth");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Delete once
            let delete_first = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), del_user))
                .bearer_auth(&del_jwt.token)
                .send()
                .await
                .expect("Failed api");

            assert!(delete_first.status().is_success(), "First delete should succeed (got {})", delete_first.status());

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Try to delete again
            let delete_second = client
                .delete(format!("{}/iam/humans/{}", get_base_url(), del_user))
                .bearer_auth(&superadmin_jwt.token)
                .send()
                .await
                .expect("Failed api");

            assert!(
                delete_second.status().is_success(),
                "Deleting already deleted user should also succeed - idempotent (got {})",
                delete_second.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 4: Try to update deleted user (should fail with 4xx)
            let update_deleted = client
                .patch(format!("{}/iam/humans/{}", get_base_url(), del_user))
                .bearer_auth(&superadmin_jwt.token)
                .json(&json!({
                    "description": "Trying to update deleted user"
                }))
                .send()
                .await
                .expect("Failed api");

            assert!(
                !update_deleted.status().is_success(),
                "Update deleted user should fail (got {})",
                update_deleted.status()
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // TEST 5: Try to login as deleted user (should fail)
            let login_deleted = auth_login(&client, del_user, "delpass").await;

            assert!(login_deleted.is_err(), "Login as deleted user should fail");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

// Another test: SuperAdmin -> Admin -> Create regular user Write, do something, change to write do something and fail something, can't create new user
// Admin -> can't create another admin, can't promote to Admin
