use super::*;
use crate::db::lib::CacheTtl;
use crate::db::lib::mocks::{MockClickhouseConnection, MockPostgresConnection, MockRedisConnection};
use eden_core::format::{EndpointUuid, rbac::ControlPerms};
use ep_core::database::auth::IdAuth;
use std::collections::HashMap;

pub struct MockIdAuth {
    org_perms: ControlPerms,
    endpoint_perms: HashMap<EndpointUuid, ControlPerms>,
}

impl MockIdAuth {
    pub fn new(org_perms: ControlPerms) -> Self {
        Self { org_perms, endpoint_perms: HashMap::new() }
    }
}

impl IdAuth for MockIdAuth {
    fn as_auth(self: Box<Self>) -> Box<dyn IdAuth> {
        self
    }

    fn get_org_perms(&self) -> &ControlPerms {
        &self.org_perms
    }

    fn get_endpoint_perms(&self, endpoint: &EndpointUuid) -> Option<&ControlPerms> {
        self.endpoint_perms.get(endpoint)
    }

    fn get_endpoints_perms(&self) -> Vec<(&EndpointUuid, &ControlPerms)> {
        self.endpoint_perms.iter().collect()
    }

    fn set_org_perms(&mut self, perms: ControlPerms) {
        self.org_perms = perms;
    }

    fn remove_organization_auth(&mut self) {
        self.org_perms = ControlPerms::empty();
    }

    fn insert_endpoint_perms(&mut self, key: EndpointUuid, perms: ControlPerms) {
        self.endpoint_perms.insert(key, perms);
    }

    fn remove_endpoint_auth(&mut self, key: &EndpointUuid) {
        self.endpoint_perms.remove(key);
    }

    fn has_endpoint_perms(&self, key: &EndpointUuid, required: ControlPerms) -> bool {
        self.endpoint_perms.get(key).is_some_and(|perms| perms.contains(required))
    }

    fn has_org_perms(&self, required: ControlPerms) -> bool {
        self.org_perms.contains(required)
    }
}

#[tokio::test]
async fn test_auth_verification() {
    let mock_redis_1 = MockRedisConnection::new(false);
    let mock_redis_2 = MockRedisConnection::new(false);
    let mock_postgres = MockPostgresConnection::new(false);
    let mock_clickhouse = MockClickhouseConnection::new(false);

    let _db_manager =
        DatabaseManager::new_with_connections(mock_redis_1, mock_redis_2, mock_postgres, mock_clickhouse, CacheTtl::from_secs(3600), None);

    let mock_auth = Box::new(MockIdAuth::new(ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::GRANT));

    assert!(mock_auth.has_org_perms(ControlPerms::READ));
    assert!(mock_auth.has_org_perms(ControlPerms::CONFIGURE));
    assert!(mock_auth.has_org_perms(ControlPerms::GRANT));
}
