//! Operational memory configuration.
//!
//! Maps to `[memory]` in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Maps to `[memory]` in `eden.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    pub enabled: bool,
    pub surreal_url: String,
    pub surreal_ns: String,
    pub surreal_db: String,
    /// Ignored for embedded backends.
    pub surreal_username: String,
    #[serde(skip_serializing)]
    pub surreal_password: String,
    /// Below this confidence, memories are not auto-injected.
    pub admission_confidence_threshold: f64,
    pub admission_max_unresolved: u32,
    pub max_memories_per_query: usize,
    pub org_sharing_enabled: bool,
    pub save_memory_tool_enabled: bool,
    pub save_memory_max_per_turn: u32,
    /// 0 = disabled.
    pub staleness_sweep_interval_secs: u64,
    pub review_after_conversational_secs: u64,
    pub review_after_approval_secs: u64,
    pub review_after_quirk_secs: u64,
    pub review_after_incident_secs: u64,
    pub review_after_runbook_secs: u64,
    /// Opt-in post-turn extraction.
    pub background_extraction_enabled: bool,
    pub background_extraction_max_messages: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            surreal_url: String::new(),
            surreal_ns: "operational".into(),
            surreal_db: "memory".into(),
            surreal_username: String::new(),
            surreal_password: String::new(),
            admission_confidence_threshold: 0.5,
            admission_max_unresolved: 2,
            max_memories_per_query: 5,
            org_sharing_enabled: false,
            save_memory_tool_enabled: true,
            save_memory_max_per_turn: 3,
            staleness_sweep_interval_secs: 3600,
            review_after_conversational_secs: 604_800,
            review_after_approval_secs: 604_800,
            review_after_quirk_secs: 2_592_000,
            review_after_incident_secs: 2_592_000,
            review_after_runbook_secs: 7_776_000,
            background_extraction_enabled: false,
            background_extraction_max_messages: 10,
        }
    }
}

impl MemoryConfig {
    /// SurrealDB URL.
    pub fn effective_surreal_url(&self) -> String {
        self.surreal_url.clone()
    }

    /// SurrealDB username.
    pub fn effective_surreal_username(&self) -> String {
        self.surreal_username.clone()
    }

    /// SurrealDB password.
    pub fn effective_surreal_password(&self) -> String {
        self.surreal_password.clone()
    }
}
