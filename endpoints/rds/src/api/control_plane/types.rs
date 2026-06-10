use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RdsTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub enum RdsApi {
    /// PostgreSQL data-plane operations (wrapped from ep-postgres).
    Postgres,
    CreateDbInstance,
    DescribeDbInstances,
    ModifyDbInstance,
    DeleteDbInstance,
    RebootDbInstance,
    StartDbInstance,
    StopDbInstance,
    CreateDbCluster,
    DescribeDbClusters,
    ModifyDbCluster,
    DeleteDbCluster,
    CreateDbSnapshot,
    DescribeDbSnapshots,
    DeleteDbSnapshot,
    CopyDbSnapshot,
    CreateDbSubnetGroup,
    DescribeDbSubnetGroups,
    ModifyDbSubnetGroup,
    DeleteDbSubnetGroup,
    CreateDbParameterGroup,
    DescribeDbParameterGroups,
    ModifyDbParameterGroup,
    DeleteDbParameterGroup,
}

impl RdsApi {
    pub fn name() -> String {
        "RdsApi".to_string()
    }

    pub fn db_kind() -> String {
        "rds".to_string()
    }
}

impl Display for RdsApi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres => write!(f, "postgres"),
            Self::CreateDbInstance => write!(f, "create_db_instance"),
            Self::DescribeDbInstances => write!(f, "describe_db_instances"),
            Self::ModifyDbInstance => write!(f, "modify_db_instance"),
            Self::DeleteDbInstance => write!(f, "delete_db_instance"),
            Self::RebootDbInstance => write!(f, "reboot_db_instance"),
            Self::StartDbInstance => write!(f, "start_db_instance"),
            Self::StopDbInstance => write!(f, "stop_db_instance"),
            Self::CreateDbCluster => write!(f, "create_db_cluster"),
            Self::DescribeDbClusters => write!(f, "describe_db_clusters"),
            Self::ModifyDbCluster => write!(f, "modify_db_cluster"),
            Self::DeleteDbCluster => write!(f, "delete_db_cluster"),
            Self::CreateDbSnapshot => write!(f, "create_db_snapshot"),
            Self::DescribeDbSnapshots => write!(f, "describe_db_snapshots"),
            Self::DeleteDbSnapshot => write!(f, "delete_db_snapshot"),
            Self::CopyDbSnapshot => write!(f, "copy_db_snapshot"),
            Self::CreateDbSubnetGroup => write!(f, "create_db_subnet_group"),
            Self::DescribeDbSubnetGroups => write!(f, "describe_db_subnet_groups"),
            Self::ModifyDbSubnetGroup => write!(f, "modify_db_subnet_group"),
            Self::DeleteDbSubnetGroup => write!(f, "delete_db_subnet_group"),
            Self::CreateDbParameterGroup => write!(f, "create_db_parameter_group"),
            Self::DescribeDbParameterGroups => write!(f, "describe_db_parameter_groups"),
            Self::ModifyDbParameterGroup => write!(f, "modify_db_parameter_group"),
            Self::DeleteDbParameterGroup => write!(f, "delete_db_parameter_group"),
        }
    }
}
