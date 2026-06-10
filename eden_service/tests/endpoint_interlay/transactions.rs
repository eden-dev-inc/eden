#![cfg(external_db)]
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, endpoint_connect_pg, endpoint_request};
use crate::util::test_server;

#[tokio::test]
async fn test_pg_transaction() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");

            // Connect as the new admin user
            let endpoint = endpoint_connect_pg(&client, &admin_jwt.token).await.unwrap_or_default();
            assert!(endpoint.is_some());
            let endpoint_uuid = endpoint.expect("endpoint").uuid;
            // Create a table, write data, read
            endpoint_request(
                &client,
                &admin_jwt.token,
                endpoint_uuid.clone(),
                json!({
                    "type": "execute",
                    "query": "CREATE TABLE tstuser(id integer, name text)",
                    "params": []
                }),
                crate::request::EpRequestType::Write,
                true,
                Some(200),
            )
            .await
            .unwrap_or_default();
            endpoint_request(
                &client,
                &admin_jwt.token,
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
                &admin_jwt.token,
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

// Another test: SuperAdmin -> Admin -> Create regular user Write, do something, change to write do something and fail something, can't create new user
// Admin -> can't create another admin, can't promote to Admin
