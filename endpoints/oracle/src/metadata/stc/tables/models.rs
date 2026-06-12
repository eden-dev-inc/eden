mod core;
mod detailed;
mod summary;

pub use core::OracleTableInfo;
pub use detailed::{
    OracleConstraintDetails, OracleIndexDetails, OracleLobDetails, OraclePartitionDetails, OracleTableDetailedMetrics, OracleTableDetails,
    OracleTableGrowth, OracleTableStatistics,
};
pub use summary::OracleTableHealthSummary;
