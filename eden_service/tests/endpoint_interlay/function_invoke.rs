#![cfg(feature = "postgres")]
#![cfg(external_db)]

use eden_core::format::EdenUuid;
use serde_json::json;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{auth_login, create_org_with_superadmin, endpoint_connect_pg, get_base_url};
use crate::util::test_server;

#[tokio::test]
async fn test_function_invoke_route_exists() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Connect a postgres endpoint to get a valid endpoint UUID
            let ep_response = endpoint_connect_pg(&client, token).await.expect("Failed to connect pg endpoint");
            let ep = ep_response.expect("Expected endpoint response");
            let endpoint_uuid = ep.uuid.uuid();

            let body = json!({
                "function": "test",
                "params": {}
            });

            let resp = client
                .post(format!("{}/functions/{}/invoke", get_base_url(), endpoint_uuid))
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .expect("Failed to send request to /functions/{endpoint}/invoke");

            let status = resp.status();
            assert_ne!(
                status.as_u16(),
                404,
                "POST /api/v1/functions/{{endpoint}}/invoke should not return 404, got {}",
                status
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
