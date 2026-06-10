use super::*;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleIndexPartitionInfo {
    pub partition_name: String,
    pub partition_position: u32,
    pub tablespace_name: String,
    pub partition_size_bytes: u64,
    pub num_rows: u64,
    pub last_analyzed: Option<DateTimeWrapper>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum IndexHealthStatus {
    Healthy,
    StaleStats,
    PerformanceIssues,
    NeedsRebuild,
    DropCandidate,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RebuildTimeEstimate {
    Fast,
    Medium,
    Slow,
    VerySlow,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum UsageFrequency {
    Never,
    Rarely,
    Sometimes,
    Often,
    Frequently,
}
