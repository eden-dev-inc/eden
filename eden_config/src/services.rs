//! Service endpoint configuration (Eden, Engine, LLM).
//!
//! Maps to the `[services]` section in `eden.toml`.

mod eden;
mod engine;
mod llm;

pub use eden::{EdenServiceConfig, GatewayCpuAffinityMode};
pub use engine::EngineServiceConfig;
pub use llm::{InternalLlmConfig, LlmTier, ResolvedLlmTier};

use serde::{Deserialize, Serialize};

/// Aggregated service configurations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ServicesConfig {
    pub eden: EdenServiceConfig,
    pub engine: EngineServiceConfig,
    pub llm: InternalLlmConfig,
}
