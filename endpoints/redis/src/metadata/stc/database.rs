use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisDatabaseStats {
    pub db_id: u32,
    pub keys: u64,
    pub expires: u64,
    pub avg_ttl: u64,
}

impl MetadataCollection for RedisDatabaseStats {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("keyspace".to_string())]))
    }
    fn description(&self) -> &'static str {
        "Return the database information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "database"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}
