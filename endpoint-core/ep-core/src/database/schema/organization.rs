use super::Row;
use crate::database::schema::{FromRow, Table, user::UserInput};
use chrono::{DateTime, Utc};
use error::EpError;
use format::timestamp::DateTimeWrapper;
pub use format::{
    ApiId, ApiUuid, EdenId, EdenNodeId, EdenNodeUuid, EndpointId, EndpointUuid, OrganizationId, OrganizationUuid, RobotId, RobotUuid,
    TemplateId, TemplateUuid, UserId, UserUuid, WorkflowId, WorkflowUuid,
};
use format::{EndpointGroupId, EndpointGroupUuid, InterlayId, InterlayUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::any::Any;
use utoipa::ToSchema;

/// Input for creating a new organization.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct OrganizationInput {
    id: String,
    /// Optional pre-generated UUID (v4). When provided, the organization is
    /// created with this UUID instead of generating one server-side. This
    /// allows operators to provision per-org encryption keys before the
    /// organization exists.
    uuid: Option<OrganizationUuid>,
    description: Option<String>,
    super_admins: Vec<UserInput>,
}

impl OrganizationInput {
    pub fn new(id: String, description: Option<String>, super_admins: Vec<UserInput>) -> Self {
        Self { id, uuid: None, description, super_admins }
    }

    pub fn with_uuid(id: String, uuid: OrganizationUuid, description: Option<String>, super_admins: Vec<UserInput>) -> Self {
        Self { id, uuid: Some(uuid), description, super_admins }
    }

    pub fn uuid(&self) -> Option<&OrganizationUuid> {
        self.uuid.as_ref()
    }

    pub fn super_admins(&self) -> &[UserInput] {
        &self.super_admins
    }
}

impl TryFrom<OrganizationInput> for OrganizationSchema {
    type Error = EpError;

    fn try_from(organization_input: OrganizationInput) -> Result<Self, Self::Error> {
        if let Some(ref uuid) = organization_input.uuid
            && uuid.get_version() != Some(uuid::Version::Random)
        {
            return Err(EpError::parse("Client-supplied organization UUID must be v4 (random)".to_string()));
        }
        Ok(Self::new(organization_input.id, organization_input.uuid, vec![], organization_input.description))
    }
}

/// Per-organization rate-limiting configuration stored as JSONB.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct RateLimitSettings {
    pub enabled: bool,
    /// Maximum ingress bytes allowed per hour. `None` = unlimited.
    pub bandwidth_ingress_limit_bytes: Option<u64>,
    /// Maximum egress bytes allowed per hour. `None` = unlimited.
    pub bandwidth_egress_limit_bytes: Option<u64>,
    /// Maximum LLM prompt tokens allowed per hour. `None` = unlimited.
    /// Enforced in real-time via Redis counters in the org_rate_limit middleware and chat handlers.
    #[serde(default)]
    pub token_ingress_limit: Option<u64>,
    /// Maximum LLM completion tokens allowed per hour. `None` = unlimited.
    /// Enforced after the LLM response in the chat handlers via Redis counters.
    #[serde(default)]
    pub token_egress_limit: Option<u64>,
}

/// Organization entity with all associated resources.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct OrganizationSchema {
    id: OrganizationId,
    uuid: OrganizationUuid,
    #[serde(default)]
    super_admin_pairs: Vec<(UserId, UserUuid)>,
    #[serde(default)]
    eden_node_pairs: Vec<(EdenNodeId, EdenNodeUuid)>,
    #[serde(default)]
    api_pairs: Vec<(ApiId, ApiUuid)>,
    #[serde(default)]
    endpoint_pairs: Vec<(EndpointId, EndpointUuid)>,
    #[serde(default)]
    endpoint_group_pairs: Vec<(EndpointGroupId, EndpointGroupUuid)>,
    #[serde(default)]
    interlay_pairs: Vec<(InterlayId, InterlayUuid)>,
    #[serde(default)]
    robot_pairs: Vec<(RobotId, RobotUuid)>,
    #[serde(default)]
    template_pairs: Vec<(TemplateId, TemplateUuid)>,
    #[serde(default)]
    user_pairs: Vec<(UserId, UserUuid)>,
    #[serde(default)]
    workflow_pairs: Vec<(WorkflowId, WorkflowUuid)>,
    description: Option<String>,
    #[serde(default)]
    rate_limit_settings: Option<RateLimitSettings>,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl OrganizationSchema {
    pub fn new(
        id: String,
        uuid: Option<OrganizationUuid>,
        eden_node_pairs: Vec<(EdenNodeId, EdenNodeUuid)>,
        description: Option<String>,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id: OrganizationId::new(id),
            uuid: uuid.unwrap_or_else(OrganizationUuid::new_uuid),
            user_pairs: vec![],
            super_admin_pairs: vec![],
            eden_node_pairs,
            api_pairs: vec![],
            endpoint_pairs: vec![],
            endpoint_group_pairs: vec![],
            interlay_pairs: vec![],
            robot_pairs: vec![],
            template_pairs: vec![],
            workflow_pairs: vec![],
            description,
            rate_limit_settings: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    // Getters
    pub fn user_ids(&self) -> Vec<&UserId> {
        self.user_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn user_uuids(&self) -> Vec<&UserUuid> {
        self.user_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn user_pairs(&self) -> &[(UserId, UserUuid)] {
        &self.user_pairs
    }

    pub fn super_admin_ids(&self) -> Vec<&UserId> {
        self.super_admin_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn super_admin_uuids(&self) -> Vec<&UserUuid> {
        self.super_admin_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn super_admin_pairs(&self) -> &[(UserId, UserUuid)] {
        &self.super_admin_pairs
    }

    pub fn api_ids(&self) -> Vec<&ApiId> {
        self.api_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn api_uuids(&self) -> Vec<&ApiUuid> {
        self.api_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn api_pairs(&self) -> &[(ApiId, ApiUuid)] {
        &self.api_pairs
    }

    pub fn endpoint_ids(&self) -> Vec<&EndpointId> {
        self.endpoint_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn endpoint_uuids(&self) -> Vec<&EndpointUuid> {
        self.endpoint_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn endpoint_pairs(&self) -> &[(EndpointId, EndpointUuid)] {
        &self.endpoint_pairs
    }

    pub fn endpoint_group_ids(&self) -> Vec<&EndpointGroupId> {
        self.endpoint_group_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn endpoint_group_uuids(&self) -> Vec<&EndpointGroupUuid> {
        self.endpoint_group_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn endpoint_group_pairs(&self) -> &[(EndpointGroupId, EndpointGroupUuid)] {
        &self.endpoint_group_pairs
    }

    pub fn eden_node_ids(&self) -> Vec<&EdenNodeId> {
        self.eden_node_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn eden_node_uuids(&self) -> Vec<&EdenNodeUuid> {
        self.eden_node_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn eden_node_pairs(&self) -> &[(EdenNodeId, EdenNodeUuid)] {
        &self.eden_node_pairs
    }

    pub fn robot_ids(&self) -> Vec<&RobotId> {
        self.robot_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn robot_uuids(&self) -> Vec<&RobotUuid> {
        self.robot_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn robot_pairs(&self) -> &[(RobotId, RobotUuid)] {
        &self.robot_pairs
    }

    pub fn template_ids(&self) -> Vec<&TemplateId> {
        self.template_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn template_uuids(&self) -> Vec<&TemplateUuid> {
        self.template_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn template_pairs(&self) -> &[(TemplateId, TemplateUuid)] {
        &self.template_pairs
    }

    pub fn workflow_ids(&self) -> Vec<&WorkflowId> {
        self.workflow_pairs.iter().map(|(id, _)| id).collect()
    }

    pub fn workflow_uuids(&self) -> Vec<&WorkflowUuid> {
        self.workflow_pairs.iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn workflow_pairs(&self) -> &[(WorkflowId, WorkflowUuid)] {
        &self.workflow_pairs
    }

    pub fn rate_limit_settings(&self) -> Option<&RateLimitSettings> {
        self.rate_limit_settings.as_ref()
    }

    pub fn update_rate_limit_settings(&mut self, settings: Option<RateLimitSettings>) {
        self.rate_limit_settings = settings;
        self.update_timestamp();
    }

    // Add operations
    pub fn add_super_admin(&mut self, id: UserId, uuid: UserUuid) -> bool {
        let pair = (id, uuid);
        if !self.super_admin_pairs.contains(&pair) {
            self.super_admin_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_super_admins(&mut self, pairs: Vec<(UserId, UserUuid)>) -> usize {
        let initial_len = self.super_admin_pairs.len();
        for pair in pairs {
            if !self.super_admin_pairs.contains(&pair) {
                self.super_admin_pairs.push(pair);
            }
        }
        let added_count = self.super_admin_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_user(&mut self, id: UserId, uuid: UserUuid) -> bool {
        let pair = (id, uuid);
        if !self.user_pairs.contains(&pair) {
            self.user_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_users(&mut self, pairs: Vec<(UserId, UserUuid)>) -> usize {
        let initial_len = self.user_pairs.len();
        for pair in pairs {
            if !self.user_pairs.contains(&pair) {
                self.user_pairs.push(pair);
            }
        }
        let added_count = self.user_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_api(&mut self, id: ApiId, uuid: ApiUuid) -> bool {
        let pair = (id, uuid);
        if !self.api_pairs.contains(&pair) {
            self.api_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_apis(&mut self, pairs: Vec<(ApiId, ApiUuid)>) -> usize {
        let initial_len = self.api_pairs.len();
        for pair in pairs {
            if !self.api_pairs.contains(&pair) {
                self.api_pairs.push(pair);
            }
        }
        let added_count = self.api_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_endpoint(&mut self, id: EndpointId, uuid: EndpointUuid) -> bool {
        let pair = (id, uuid);
        if !self.endpoint_pairs.contains(&pair) {
            self.endpoint_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_endpoints(&mut self, pairs: Vec<(EndpointId, EndpointUuid)>) -> usize {
        let initial_len = self.endpoint_pairs.len();
        for pair in pairs {
            if !self.endpoint_pairs.contains(&pair) {
                self.endpoint_pairs.push(pair);
            }
        }
        let added_count = self.endpoint_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_endpoint_group(&mut self, id: EndpointGroupId, uuid: EndpointGroupUuid) -> bool {
        let pair = (id, uuid);
        if !self.endpoint_group_pairs.contains(&pair) {
            self.endpoint_group_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_endpoint_groups(&mut self, pairs: Vec<(EndpointGroupId, EndpointGroupUuid)>) -> usize {
        let initial_len = self.endpoint_group_pairs.len();
        for pair in pairs {
            if !self.endpoint_group_pairs.contains(&pair) {
                self.endpoint_group_pairs.push(pair);
            }
        }
        let added_count = self.endpoint_group_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_interlay(&mut self, id: InterlayId, uuid: InterlayUuid) -> bool {
        let pair = (id, uuid);
        if !self.interlay_pairs.contains(&pair) {
            self.interlay_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_interlays(&mut self, pairs: Vec<(InterlayId, InterlayUuid)>) -> usize {
        let initial_len = self.interlay_pairs.len();
        for pair in pairs {
            if !self.interlay_pairs.contains(&pair) {
                self.interlay_pairs.push(pair);
            }
        }
        let added_count = self.interlay_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_eden_node(&mut self, id: EdenNodeId, uuid: EdenNodeUuid) -> bool {
        let pair = (id, uuid);
        if !self.eden_node_pairs.contains(&pair) {
            self.eden_node_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_eden_nodes(&mut self, pairs: Vec<(EdenNodeId, EdenNodeUuid)>) -> usize {
        let initial_len = self.eden_node_pairs.len();
        for pair in pairs {
            if !self.eden_node_pairs.contains(&pair) {
                self.eden_node_pairs.push(pair);
            }
        }
        let added_count = self.eden_node_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_robot(&mut self, id: RobotId, uuid: RobotUuid) -> bool {
        let pair = (id, uuid);
        if !self.robot_pairs.contains(&pair) {
            self.robot_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_robots(&mut self, pairs: Vec<(RobotId, RobotUuid)>) -> usize {
        let initial_len = self.robot_pairs.len();
        for pair in pairs {
            if !self.robot_pairs.contains(&pair) {
                self.robot_pairs.push(pair);
            }
        }
        let added_count = self.robot_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_template(&mut self, id: TemplateId, uuid: TemplateUuid) -> bool {
        let pair = (id, uuid);
        if !self.template_pairs.contains(&pair) {
            self.template_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_templates(&mut self, pairs: Vec<(TemplateId, TemplateUuid)>) -> usize {
        let initial_len = self.template_pairs.len();
        for pair in pairs {
            if !self.template_pairs.contains(&pair) {
                self.template_pairs.push(pair);
            }
        }
        let added_count = self.template_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    pub fn add_workflow(&mut self, id: WorkflowId, uuid: WorkflowUuid) -> bool {
        let pair = (id, uuid);
        if !self.workflow_pairs.contains(&pair) {
            self.workflow_pairs.push(pair);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn add_workflows(&mut self, pairs: Vec<(WorkflowId, WorkflowUuid)>) -> usize {
        let initial_len = self.workflow_pairs.len();
        for pair in pairs {
            if !self.workflow_pairs.contains(&pair) {
                self.workflow_pairs.push(pair);
            }
        }
        let added_count = self.workflow_pairs.len() - initial_len;
        if added_count > 0 {
            self.update_timestamp();
        }
        added_count
    }

    // Remove operations (by UUID since they're unique)
    pub fn remove_super_admin_by_uuid(&mut self, uuid: &UserUuid) -> bool {
        if let Some(pos) = self.super_admin_pairs.iter().position(|(_, u)| u == uuid) {
            self.super_admin_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_super_admins_by_uuids(&mut self, uuids: &[UserUuid]) -> usize {
        let initial_len = self.super_admin_pairs.len();
        self.super_admin_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.super_admin_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_user_by_uuid(&mut self, uuid: &UserUuid) -> bool {
        if let Some(pos) = self.user_pairs.iter().position(|(_, u)| u == uuid) {
            self.user_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_users_by_uuids(&mut self, uuids: &[UserUuid]) -> usize {
        let initial_len = self.user_pairs.len();
        self.user_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.user_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_api_by_uuid(&mut self, uuid: &ApiUuid) -> bool {
        if let Some(pos) = self.api_pairs.iter().position(|(_, u)| u == uuid) {
            self.api_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_apis_by_uuids(&mut self, uuids: &[ApiUuid]) -> usize {
        let initial_len = self.api_pairs.len();
        self.api_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.api_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_endpoint_by_uuid(&mut self, uuid: &EndpointUuid) -> bool {
        if let Some(pos) = self.endpoint_pairs.iter().position(|(_, u)| u == uuid) {
            self.endpoint_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_endpoints_by_uuids(&mut self, uuids: &[EndpointUuid]) -> usize {
        let initial_len = self.endpoint_pairs.len();
        self.endpoint_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.endpoint_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_endpoint_group_by_uuid(&mut self, uuid: &EndpointGroupUuid) -> bool {
        if let Some(pos) = self.endpoint_group_pairs.iter().position(|(_, u)| u == uuid) {
            self.endpoint_group_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_endpoint_groups_by_uuids(&mut self, uuids: &[EndpointGroupUuid]) -> usize {
        let initial_len = self.endpoint_group_pairs.len();
        self.endpoint_group_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.endpoint_group_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_eden_node_by_uuid(&mut self, uuid: &EdenNodeUuid) -> bool {
        if let Some(pos) = self.eden_node_pairs.iter().position(|(_, u)| u == uuid) {
            self.eden_node_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_eden_nodes_by_uuids(&mut self, uuids: &[EdenNodeUuid]) -> usize {
        let initial_len = self.eden_node_pairs.len();
        self.eden_node_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.eden_node_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_interlay_by_uuid(&mut self, uuid: &InterlayUuid) -> bool {
        if let Some(pos) = self.interlay_pairs.iter().position(|(_, u)| u == uuid) {
            self.interlay_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_interlays_by_uuids(&mut self, uuids: &[InterlayUuid]) -> usize {
        let initial_len = self.interlay_pairs.len();
        self.interlay_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.interlay_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_robot_by_uuid(&mut self, uuid: &RobotUuid) -> bool {
        if let Some(pos) = self.robot_pairs.iter().position(|(_, u)| u == uuid) {
            self.robot_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_robots_by_uuids(&mut self, uuids: &[RobotUuid]) -> usize {
        let initial_len = self.robot_pairs.len();
        self.robot_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.robot_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_template_by_uuid(&mut self, uuid: &TemplateUuid) -> bool {
        if let Some(pos) = self.template_pairs.iter().position(|(_, u)| u == uuid) {
            self.template_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_templates_by_uuids(&mut self, uuids: &[TemplateUuid]) -> usize {
        let initial_len = self.template_pairs.len();
        self.template_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.template_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    pub fn remove_workflow_by_uuid(&mut self, uuid: &WorkflowUuid) -> bool {
        if let Some(pos) = self.workflow_pairs.iter().position(|(_, u)| u == uuid) {
            self.workflow_pairs.swap_remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn remove_workflows_by_uuids(&mut self, uuids: &[WorkflowUuid]) -> usize {
        let initial_len = self.workflow_pairs.len();
        self.workflow_pairs.retain(|(_, u)| !uuids.contains(u));
        let removed_count = initial_len - self.workflow_pairs.len();
        if removed_count > 0 {
            self.update_timestamp();
        }
        removed_count
    }

    // Update ID operations (UUIDs remain immutable)
    pub fn update_user_id(&mut self, uuid: &UserUuid, new_id: UserId) -> bool {
        if let Some(pos) = self.user_pairs.iter().position(|(_, u)| u == uuid) {
            self.user_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_super_admin_id(&mut self, uuid: &UserUuid, new_id: UserId) -> bool {
        if let Some(pos) = self.super_admin_pairs.iter().position(|(_, u)| u == uuid) {
            self.super_admin_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_api_id(&mut self, uuid: &ApiUuid, new_id: ApiId) -> bool {
        if let Some(pos) = self.api_pairs.iter().position(|(_, u)| u == uuid) {
            self.api_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_endpoint_id(&mut self, uuid: &EndpointUuid, new_id: EndpointId) -> bool {
        if let Some(pos) = self.endpoint_pairs.iter().position(|(_, u)| u == uuid) {
            self.endpoint_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_endpoint_group_id(&mut self, uuid: &EndpointGroupUuid, new_id: EndpointGroupId) -> bool {
        if let Some(pos) = self.endpoint_group_pairs.iter().position(|(_, u)| u == uuid) {
            self.endpoint_group_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_eden_node_id(&mut self, uuid: &EdenNodeUuid, new_id: EdenNodeId) -> bool {
        if let Some(pos) = self.eden_node_pairs.iter().position(|(_, u)| u == uuid) {
            self.eden_node_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_robot_id(&mut self, uuid: &RobotUuid, new_id: RobotId) -> bool {
        if let Some(pos) = self.robot_pairs.iter().position(|(_, u)| u == uuid) {
            self.robot_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_template_id(&mut self, uuid: &TemplateUuid, new_id: TemplateId) -> bool {
        if let Some(pos) = self.template_pairs.iter().position(|(_, u)| u == uuid) {
            self.template_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    pub fn update_workflow_id(&mut self, uuid: &WorkflowUuid, new_id: WorkflowId) -> bool {
        if let Some(pos) = self.workflow_pairs.iter().position(|(_, u)| u == uuid) {
            self.workflow_pairs[pos].0 = new_id;
            self.update_timestamp();
            true
        } else {
            false
        }
    }
}

impl Table for OrganizationSchema {
    type I = OrganizationId;
    type U = OrganizationUuid;

    fn id(&self) -> OrganizationId {
        self.id.clone()
    }

    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }

    fn uuid(&self) -> OrganizationUuid {
        self.uuid.clone()
    }

    fn description(&self) -> Option<String> {
        self.description.clone()
    }

    fn update_description(&mut self, description: String) -> Option<String> {
        let out = self.description.replace(description);
        self.update_timestamp();
        out
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.created_at.as_datetime()
    }

    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at.as_datetime()
    }

    fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for OrganizationSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,

            super_admin_pairs: {
                let ids = row.try_get::<_, Vec<UserId>>("admin_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<UserUuid>>("admin_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },
            api_pairs: {
                let ids = row.try_get::<_, Vec<ApiId>>("api_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<ApiUuid>>("api_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            eden_node_pairs: {
                let ids = row.try_get::<_, Vec<EdenNodeId>>("eden_node_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<EdenNodeUuid>>("eden_node_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            endpoint_pairs: {
                let ids = row.try_get::<_, Vec<EndpointId>>("endpoint_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<EndpointUuid>>("endpoint_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            endpoint_group_pairs: {
                let ids = row.try_get::<_, Vec<EndpointGroupId>>("endpoint_group_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<EndpointGroupUuid>>("endpoint_group_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            interlay_pairs: {
                let ids = row.try_get::<_, Vec<InterlayId>>("interlay_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<InterlayUuid>>("interlay_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            robot_pairs: {
                let ids = row.try_get::<_, Vec<RobotId>>("robot_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<RobotUuid>>("robot_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            template_pairs: {
                let ids = row.try_get::<_, Vec<TemplateId>>("template_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<TemplateUuid>>("template_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            user_pairs: {
                let ids = row.try_get::<_, Vec<UserId>>("user_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<UserUuid>>("user_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            workflow_pairs: {
                let ids = row.try_get::<_, Vec<WorkflowId>>("workflow_ids").map_err(EpError::database)?;
                let uuids = row.try_get::<_, Vec<WorkflowUuid>>("workflow_uuids").map_err(EpError::database)?;

                ids.into_iter().zip(uuids).collect()
            },

            description: row.try_get("description").map_err(EpError::database)?,
            rate_limit_settings: row
                .try_get::<&str, Option<serde_json::Value>>("rate_limit_settings")
                .ok()
                .flatten()
                .and_then(|v| serde_json::from_value(v).ok()),
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for OrganizationSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let serialized = serde_json::to_vec(self).unwrap_or_default();
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for OrganizationSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting OrganizationSchema",
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateOrganizationSchema {
    id: Option<OrganizationId>,
    description: Option<String>,
    rate_limit_settings: Option<RateLimitSettings>,
}

impl UpdateOrganizationSchema {
    pub fn new(id: Option<String>, description: Option<String>, rate_limit_settings: Option<RateLimitSettings>) -> Self {
        Self {
            id: id.map(OrganizationId::new),
            description,
            rate_limit_settings,
        }
    }

    pub fn id(&self) -> Option<&OrganizationId> {
        self.id.as_ref()
    }

    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    pub fn rate_limit_settings(&self) -> Option<&RateLimitSettings> {
        self.rate_limit_settings.as_ref()
    }

    pub fn update(&self, schema: &mut OrganizationSchema) {
        if let Some(id) = self.id() {
            schema.update_id(id.to_string());
        }
        if let Some(description) = self.description() {
            schema.update_description(description.to_string());
        }
        if self.rate_limit_settings.is_some() {
            schema.update_rate_limit_settings(self.rate_limit_settings.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_json_test() {
        let input = OrganizationInput {
            id: "".to_string(),
            uuid: None,
            description: None,
            super_admins: vec![UserInput::new(
                "admin".to_string(),
                "password".to_string(),
                None,
                None,
                None,
                format::rbac::ControlPerms::all(),
            )],
        };

        print!("{}", serde_json::to_value(&input).unwrap_or_default());
    }

    #[test]
    fn try_from_with_v4_uuid_succeeds() {
        let uuid = OrganizationUuid::new_uuid(); // v4
        let input = OrganizationInput::with_uuid("test-org".to_string(), uuid.clone(), None, vec![]);
        let schema = OrganizationSchema::try_from(input).expect("v4 UUID should be accepted");
        assert_eq!(schema.uuid(), uuid);
    }

    #[test]
    fn try_from_without_uuid_generates_one() {
        let input = OrganizationInput::new("test-org".to_string(), None, vec![]);
        let schema = OrganizationSchema::try_from(input).expect("missing UUID should auto-generate");
        assert_eq!(schema.uuid().get_version(), Some(uuid::Version::Random));
    }

    #[test]
    fn try_from_with_non_v4_uuid_fails() {
        use format::EdenUuid;
        // v5 (SHA-1 name-based) UUID
        let v5 = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, b"example.com");
        let input = OrganizationInput {
            id: "test-org".to_string(),
            uuid: Some(OrganizationUuid::new(v5)),
            description: None,
            super_admins: vec![],
        };
        let err = OrganizationSchema::try_from(input).unwrap_err();
        assert!(err.to_string().contains("v4"), "error should mention v4: {err}");
    }
}
