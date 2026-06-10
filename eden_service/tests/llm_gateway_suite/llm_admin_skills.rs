#![cfg(external_db)]
use serde_json::{Value, json};

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

#[tokio::test]
async fn test_llm_admin_skills_crud() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // CREATE: POST /api/v1/admin/llm/skills
            let create_body = json!({
                "name": "test-skill-crud",
                "display_name": "Test Skill CRUD",
                "description": "A skill created for integration testing",
                "body_markdown": "# Test Skill\n\nThis is a test skill body.",
                "tags": ["test", "integration"],
                "estimated_tokens": 100,
                "source_format": "markdown",
                "is_active": true,
                "skill_tier": "community"
            });

            let create_url = format!("{}/admin/llm/skills", get_base_url());
            let created: Option<Value> = make_method_request(&client, token, HttpMethod::Post, &create_url, Some(&create_body), None)
                .await
                .expect("Failed to create skill");

            let created = created.expect("Expected a response body from create");
            // The response is wrapped in EdenResponse, extract the data
            let skill_data = if let Some(data) = created.get("data") {
                data.clone()
            } else {
                created.clone()
            };

            let skill_id = skill_data.get("id").expect("Created skill should have an id").as_str().expect("id should be a string");
            assert!(!skill_id.is_empty(), "Skill id should not be empty");

            assert_eq!(skill_data.get("name").and_then(Value::as_str).unwrap_or_default(), "test-skill-crud");
            assert_eq!(skill_data.get("display_name").and_then(Value::as_str).unwrap_or_default(), "Test Skill CRUD");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // LIST: GET /api/v1/admin/llm/skills — verify the created skill is present
            let list_url = format!("{}/admin/llm/skills", get_base_url());
            let listed: Option<Value> = make_method_request(&client, token, HttpMethod::Get, &list_url, None::<&Value>, None)
                .await
                .expect("Failed to list skills");

            let listed = listed.expect("Expected a response body from list");
            let skills_array = if let Some(data) = listed.get("data") {
                data.as_array().expect("data should be an array")
            } else {
                listed.as_array().expect("response should be an array")
            };

            let found = skills_array.iter().any(|s| s.get("id").and_then(Value::as_str) == Some(skill_id));
            assert!(found, "Created skill should appear in the list");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // UPDATE: PATCH /api/v1/admin/llm/skills/{skill_id}
            let update_body = json!({
                "display_name": "Updated Test Skill"
            });

            let update_url = format!("{}/admin/llm/skills/{}", get_base_url(), skill_id);
            let updated: Option<Value> = make_method_request(&client, token, HttpMethod::Patch, &update_url, Some(&update_body), None)
                .await
                .expect("Failed to update skill");

            let updated = updated.expect("Expected a response body from update");
            let updated_data = if let Some(data) = updated.get("data") {
                data.clone()
            } else {
                updated.clone()
            };

            assert_eq!(
                updated_data.get("display_name").and_then(Value::as_str).unwrap_or_default(),
                "Updated Test Skill",
                "display_name should be updated"
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // DELETE: DELETE /api/v1/admin/llm/skills/{skill_id}
            let delete_url = format!("{}/admin/llm/skills/{}", get_base_url(), skill_id);
            let _: Option<Value> = make_method_request(&client, token, HttpMethod::Delete, &delete_url, None::<&Value>, None)
                .await
                .expect("Failed to delete skill");

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // VERIFY DELETION: GET /api/v1/admin/llm/skills — verify list no longer contains the skill
            let listed_after: Option<Value> = make_method_request(&client, token, HttpMethod::Get, &list_url, None::<&Value>, None)
                .await
                .expect("Failed to list skills after deletion");

            let listed_after = listed_after.expect("Expected a response body from list after deletion");
            let skills_after = if let Some(data) = listed_after.get("data") {
                data.as_array().expect("data should be an array")
            } else {
                listed_after.as_array().expect("response should be an array")
            };

            let found_after = skills_after.iter().any(|s| s.get("id").and_then(Value::as_str) == Some(skill_id));
            assert!(!found_after, "Deleted skill should not appear in the list");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
