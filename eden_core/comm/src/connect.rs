use borsh::BorshDeserialize;
use ep_core::ep::EpConfig;
use format::endpoint::EpKind;
use format::{EndpointUuid, OrganizationUuid};

/// Input Data for Endpoint Connection
#[derive(Debug)]
pub struct ConnectInput<C> {
    org_uuid: OrganizationUuid,
    kind: EpKind,
    endpoint_uuid: EndpointUuid,
    endpoint_description: Option<String>,
    endpoint_config: C,
}

impl<C> ConnectInput<C>
where
    C: EpConfig + BorshDeserialize + Clone,
{
    pub fn new(
        org_uuid: OrganizationUuid,
        kind: EpKind,
        endpoint_uuid: EndpointUuid,
        endpoint_description: Option<String>,
        endpoint_config: C,
    ) -> Self {
        Self {
            org_uuid,
            kind,
            endpoint_uuid,
            endpoint_description,
            endpoint_config,
        }
    }
    pub fn org_uuid(&self) -> &OrganizationUuid {
        &self.org_uuid
    }
    pub fn kind(&self) -> &EpKind {
        &self.kind
    }
    pub fn endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }
    pub fn endpoint_description(&self) -> &Option<String> {
        &self.endpoint_description
    }
    pub fn endpoint_config(&self) -> &C {
        &self.endpoint_config
    }
}
