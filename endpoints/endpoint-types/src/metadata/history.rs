use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub struct SyncHistory {
    entries: BTreeMap<String, DateTimeWrapper>,
}

impl SyncHistory {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, job: String, moment: DateTime<Utc>) {
        self.entries.insert(job, DateTimeWrapper::from(moment));
    }

    pub fn get(&self, job: &str) -> Option<DateTime<Utc>> {
        self.entries
            .get(job)
            .map(|wrapper| DateTime::<Utc>::from(wrapper))
    }

    pub fn entries(&self) -> &BTreeMap<String, DateTimeWrapper> {
        &self.entries
    }
}
