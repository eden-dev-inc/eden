use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct DatadogLastSyncTimestamps {
    pub monitor_summary_last_sync: u64,
    pub host_info_last_sync: u64,
}
