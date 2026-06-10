use serde::{Deserialize, Serialize};

use super::RdsTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDbSubnetGroupInput {
    pub db_subnet_group_name: String,
    pub db_subnet_group_description: String,
    pub subnet_ids: Vec<String>,
    pub tags: Option<Vec<RdsTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDbSubnetGroupsInput {
    pub db_subnet_group_name: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyDbSubnetGroupInput {
    pub db_subnet_group_name: String,
    pub db_subnet_group_description: Option<String>,
    pub subnet_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDbSubnetGroupInput {
    pub db_subnet_group_name: String,
}
