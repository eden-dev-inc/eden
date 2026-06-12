use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct AwsLastSyncTimestamps {
    pub identity_last_sync: u64,
    pub iam_summary_last_sync: u64,
    pub account_aliases_last_sync: u64,
}
