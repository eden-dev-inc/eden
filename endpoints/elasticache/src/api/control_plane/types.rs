use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ElasticacheTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub enum ElasticacheApi {
    /// Redis data-plane operations (wrapped from ep-redis).
    Redis,
    CreateCacheCluster,
    DescribeCacheClusters,
    ModifyCacheCluster,
    DeleteCacheCluster,
    RebootCacheCluster,
    CreateReplicationGroup,
    DescribeReplicationGroups,
    ModifyReplicationGroup,
    DeleteReplicationGroup,
    CreateUser,
    DescribeUsers,
    ModifyUser,
    DeleteUser,
    CreateUserGroup,
    DescribeUserGroups,
    ModifyUserGroup,
    DeleteUserGroup,
    CreateSnapshot,
    DescribeSnapshots,
    DeleteSnapshot,
    CopySnapshot,
}

impl ElasticacheApi {
    pub fn name() -> String {
        "ElasticacheApi".to_string()
    }

    pub fn db_kind() -> String {
        "elasticache".to_string()
    }
}

impl Display for ElasticacheApi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redis => write!(f, "redis"),
            Self::CreateCacheCluster => write!(f, "create_cache_cluster"),
            Self::DescribeCacheClusters => write!(f, "describe_cache_clusters"),
            Self::ModifyCacheCluster => write!(f, "modify_cache_cluster"),
            Self::DeleteCacheCluster => write!(f, "delete_cache_cluster"),
            Self::RebootCacheCluster => write!(f, "reboot_cache_cluster"),
            Self::CreateReplicationGroup => write!(f, "create_replication_group"),
            Self::DescribeReplicationGroups => write!(f, "describe_replication_groups"),
            Self::ModifyReplicationGroup => write!(f, "modify_replication_group"),
            Self::DeleteReplicationGroup => write!(f, "delete_replication_group"),
            Self::CreateUser => write!(f, "create_user"),
            Self::DescribeUsers => write!(f, "describe_users"),
            Self::ModifyUser => write!(f, "modify_user"),
            Self::DeleteUser => write!(f, "delete_user"),
            Self::CreateUserGroup => write!(f, "create_user_group"),
            Self::DescribeUserGroups => write!(f, "describe_user_groups"),
            Self::ModifyUserGroup => write!(f, "modify_user_group"),
            Self::DeleteUserGroup => write!(f, "delete_user_group"),
            Self::CreateSnapshot => write!(f, "create_snapshot"),
            Self::DescribeSnapshots => write!(f, "describe_snapshots"),
            Self::DeleteSnapshot => write!(f, "delete_snapshot"),
            Self::CopySnapshot => write!(f, "copy_snapshot"),
        }
    }
}
