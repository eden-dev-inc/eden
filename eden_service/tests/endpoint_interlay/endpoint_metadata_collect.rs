#![cfg(feature = "postgres")]
#![cfg(external_db)]

use serde_json::Value;

use eden_core::format::EdenUuid;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, endpoint_connect_pg, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_endpoint_metadata_collect() {
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

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // POST /api/v1/endpoints/{endpoint}/metadata/collect - trigger metadata collection
            let collect_resp = client
                .post(format!("{}/endpoints/{}/metadata/collect", get_base_url(), endpoint_uuid))
                .bearer_auth(&jwt.token)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
                .expect("Failed to send metadata collect request");

            let collect_status = collect_resp.status();
            println!("Metadata collect status: {}", collect_status);
            assert_ne!(
                collect_status.as_u16(),
                404,
                "POST /endpoints/{{endpoint}}/metadata/collect should not return 404, got: {}",
                collect_status
            );

            // Cleanup: delete the endpoint
            let _delete_result: Option<Value> = make_method_request(
                &client,
                &jwt.token,
                HttpMethod::Delete,
                &format!("{}/endpoints/{}", get_base_url(), endpoint_uuid),
                None::<&()>,
                Some(200),
            )
            .await
            .expect("Failed to delete endpoint");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
