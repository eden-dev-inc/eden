#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_template_run() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let template_id = "exec-test";
            let template_body = json!({
                "id": template_id,
                "description": "test",
                "template": {
                    "type": "query",
                    "content": "SELECT 1"
                }
            });

            // Create the template first
            let _create_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/templates", get_base_url()),
                Some(&template_body),
                Some(201),
            )
            .await
            .expect("Failed to create template");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // POST /api/v1/templates/{template} (run template)
            // This may fail without a real endpoint, but should not return 404
            let run_body = json!({ "endpoint": "dummy" });
            let run_resp = client
                .post(format!("{}/templates/{}", get_base_url(), template_id))
                .bearer_auth(token)
                .json(&run_body)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
                .expect("Failed to send run template request");

            let run_status = run_resp.status();
            println!("Run template status: {}", run_status);
            assert_ne!(run_status.as_u16(), 404, "POST /templates/{{template}} should not return 404, got: {}", run_status);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[tokio::test]
async fn test_template_render() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let template_id = "exec-test-render";
            let template_body = json!({
                "id": template_id,
                "description": "test",
                "template": {
                    "type": "query",
                    "content": "SELECT 1"
                }
            });

            // Create the template first
            let _create_resp: Option<Value> = make_method_request(
                &client,
                token,
                HttpMethod::Post,
                &format!("{}/templates", get_base_url()),
                Some(&template_body),
                Some(201),
            )
            .await
            .expect("Failed to create template");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // POST /api/v1/templates/{template}/render
            let render_body = json!({ "params": {} });
            let render_resp = client
                .post(format!("{}/templates/{}/render", get_base_url(), template_id))
                .bearer_auth(token)
                .json(&render_body)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
                .expect("Failed to send render template request");

            let render_status = render_resp.status();
            println!("Render template status: {}", render_status);
            assert_ne!(
                render_status.as_u16(),
                404,
                "POST /templates/{{template}}/render should not return 404, got: {}",
                render_status
            );
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
