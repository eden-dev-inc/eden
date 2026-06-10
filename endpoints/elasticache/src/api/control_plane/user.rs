use serde::{Deserialize, Serialize};

use super::ElasticacheTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserInput {
    pub user_id: String,
    pub user_name: String,
    pub engine: String,
    pub access_string: String,
    pub passwords: Option<Vec<String>>,
    pub no_password_required: Option<bool>,
    pub tags: Option<Vec<ElasticacheTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeUsersInput {
    pub user_id: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyUserInput {
    pub user_id: String,
    pub access_string: Option<String>,
    pub append_passwords: Option<Vec<String>>,
    pub remove_passwords: Option<Vec<String>>,
    pub no_password_required: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteUserInput {
    pub user_id: String,
}
