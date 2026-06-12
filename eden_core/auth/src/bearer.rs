use std::collections::HashMap;

use format::{EndpointId, rbac::ControlPerms, timestamp::Timestamp};
use serde::{Deserialize, Serialize};

/// Bearer token value with organization and endpoint control-plane permissions.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BearerValue {
    pub org_perms: ControlPerms,
    pub endpoint_perms: HashMap<EndpointId, ControlPerms>,
    pub created: Timestamp,
    pub updated: Timestamp,
}

impl BearerValue {
    pub fn new(org_perms: ControlPerms, endpoint_perms: Vec<(EndpointId, ControlPerms)>) -> Self {
        Self {
            org_perms,
            endpoint_perms: HashMap::from_iter(endpoint_perms),
            created: Timestamp::new(),
            updated: Timestamp::new(),
        }
    }
}
