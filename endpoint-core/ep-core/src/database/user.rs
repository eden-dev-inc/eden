use crate::database::auth::IdAuth;
use auth::password::Password;
use format::EndpointUuid;
use format::{UserUuid, rbac::ControlPerms, timestamp::Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UserValueMasked {
    key: UserUuid,
    org_perms: ControlPerms,
    endpoint_perms: HashMap<EndpointUuid, ControlPerms>,
    password: String,
    created: Timestamp,
    updated: Timestamp,
}

impl From<UserValue> for UserValueMasked {
    fn from(value: UserValue) -> Self {
        Self {
            key: value.key,
            org_perms: value.org_perms,
            endpoint_perms: value.endpoint_perms,
            password: "masked".to_string(),
            created: value.created,
            updated: value.updated,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UserValue {
    key: UserUuid,
    org_perms: ControlPerms,
    endpoint_perms: HashMap<EndpointUuid, ControlPerms>,
    password: Password,
    created: Timestamp,
    updated: Timestamp,
}

impl UserValue {
    pub fn new(key: UserUuid, password: String) -> Self {
        Self {
            key,
            org_perms: ControlPerms::READ,
            endpoint_perms: HashMap::new(),
            password: Password::new(password),
            created: Timestamp::new(),
            updated: Timestamp::new(),
        }
    }
    pub fn verify(&self, password: String) -> bool {
        self.password.verify(password)
    }
}

impl IdAuth for UserValue {
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
        self.updated = Timestamp::new();
    }
    fn remove_organization_auth(&mut self) {
        self.org_perms = ControlPerms::empty();
        self.updated = Timestamp::new();
    }
    fn insert_endpoint_perms(&mut self, key: EndpointUuid, perms: ControlPerms) {
        self.endpoint_perms.insert(key, perms);
        self.updated = Timestamp::new();
    }
    fn remove_endpoint_auth(&mut self, key: &EndpointUuid) {
        self.endpoint_perms.remove(key);
        self.updated = Timestamp::new();
    }
    fn has_endpoint_perms(&self, key: &EndpointUuid, required: ControlPerms) -> bool {
        self.endpoint_perms.get(key).is_some_and(|perms| perms.contains(required))
    }
    fn has_org_perms(&self, required: ControlPerms) -> bool {
        self.org_perms.contains(required)
    }
}
