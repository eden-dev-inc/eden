use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct RedisStructureSamples {
    pub sampled_at_unix_secs: u64,
    pub sample_count: u32,
    pub scan_budget_used: u32,
    pub distinct_patterns_observed: u32,
    pub patterns: Vec<RedisPatternSample>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct RedisPatternSample {
    pub raw_pattern: String,
    pub value_kind: String,
    pub sample_size: u32,
    pub ttl_millis_samples: Vec<i64>,
    pub size_bytes_samples: Vec<u32>,
    pub attributes: Vec<RedisAttributeSample>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct RedisAttributeSample {
    pub name: String,
    pub presence_count: u32,
    pub sample_values: Vec<String>,
}

impl MetadataCollection for RedisStructureSamples {
    type Request = ();

    fn request(&self) -> Self::Request {}

    fn description(&self) -> &'static str {
        "Return Redis key structure samples discovered through SCAN-based sampling"
    }

    fn category(&self) -> &'static str {
        "structure_sampling"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}
