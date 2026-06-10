use crate::api::{ConfigGetInput, Deserialize, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct RedisConfigInfo {
    pub config: HashMap<String, String>,
}

impl MetadataCollection for RedisConfigInfo {
    type Request = ConfigGetInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(vec![RedisJsonValue::String("*".to_string())])
    }
    fn description(&self) -> &'static str {
        "Return the config information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "config"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

impl RedisConfigInfo {
    pub fn new(map: HashMap<String, String>) -> Self {
        Self { config: map }
    }
}
