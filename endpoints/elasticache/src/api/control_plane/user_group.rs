use serde::{Deserialize, Serialize};

use super::ElasticacheTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserGroupInput {
    pub user_group_id: String,
    pub engine: String,
    pub user_ids: Option<Vec<String>>,
    pub tags: Option<Vec<ElasticacheTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeUserGroupsInput {
    pub user_group_id: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyUserGroupInput {
    pub user_group_id: String,
    pub user_ids_to_add: Option<Vec<String>>,
    pub user_ids_to_remove: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteUserGroupInput {
    pub user_group_id: String,
}
