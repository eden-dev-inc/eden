use serde::{Deserialize, Serialize};

use super::RdsTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDbParameterGroupInput {
    pub db_parameter_group_name: String,
    pub db_parameter_group_family: String,
    pub description: String,
    pub tags: Option<Vec<RdsTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDbParameterGroupsInput {
    pub db_parameter_group_name: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyDbParameterGroupInput {
    pub db_parameter_group_name: String,
    pub parameters: Vec<RdsParameter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdsParameter {
    pub parameter_name: String,
    pub parameter_value: String,
    pub apply_method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDbParameterGroupInput {
    pub db_parameter_group_name: String,
}
