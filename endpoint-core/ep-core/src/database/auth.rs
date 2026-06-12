use crate::database::user::UserValue;
use auth::BearerValue;
use format::EndpointUuid;
use format::rbac::ControlPerms;

pub trait IdAuth {
    fn as_auth(self: Box<Self>) -> Box<dyn IdAuth>;
    fn get_org_perms(&self) -> &ControlPerms;
    fn get_endpoint_perms(&self, endpoint: &EndpointUuid) -> Option<&ControlPerms>;
    fn get_endpoints_perms(&self) -> Vec<(&EndpointUuid, &ControlPerms)>;
    fn set_org_perms(&mut self, perms: ControlPerms);
    fn remove_organization_auth(&mut self);
    fn insert_endpoint_perms(&mut self, key: EndpointUuid, perms: ControlPerms);
    fn remove_endpoint_auth(&mut self, key: &EndpointUuid);
    fn has_endpoint_perms(&self, key: &EndpointUuid, required: ControlPerms) -> bool;
    fn has_org_perms(&self, required: ControlPerms) -> bool;
}

pub enum AuthValue {
    BearerToken(BearerValue),
    UsernamePassword(UserValue),
}
