#![cfg(external_db)]
use serde::Deserialize;
use serde_json::from_str;

use crate::common::EDEN_NEW_ORG_TOKEN_VALUE;
use crate::request::create_org;
use crate::util::test_server;

#[derive(Debug, Deserialize)]
struct TestOrgResponse {
    pub id: String,
    #[allow(dead_code)]
    pub uuid: String,
}

#[test]
fn test_create_org() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE)).await {
                Ok(new_org) => {
                    println!("Response: {}", new_org);
                    match from_str::<TestOrgResponse>(&new_org) {
                        Ok(org_response) => {
                            assert_eq!("TestOrg".to_string(), org_response.id);
                        }
                        Err(e) => {
                            eprintln!("Failed to parse response: {}", e);
                            panic!("Failed to parse response: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to create organization: {}", e);
                    panic!("Failed to create organization: {}", e);
                }
            }
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}
