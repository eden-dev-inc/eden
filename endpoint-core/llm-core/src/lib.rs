#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # LLM (Large Language Model) Endpoint Core
//!
//! LLM API integration for treating AI models as queryable "databases".
//!
//! ## Usage
//!
//! ```ignore
//! use llm_core::config::LlmConfig;
//! use llm_core::connection::{LlmConnectionDefaults, LlmCredentials, LlmTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = LlmConfig {
//!     target: LlmTarget::OpenAI {
//!         defaults: LlmConnectionDefaults {
//!             model: "gpt-4".to_string(),
//!             max_tokens: Some(1000),
//!             ..Default::default()
//!         },
//!     },
//!     read_credentials: Some(LlmCredentials {
//!         inline_api_key: Some("sk-...".to_string()),
//!         ..Default::default()
//!     }),
//!     write_credentials: Some(LlmCredentials {
//!         inline_api_key: Some("sk-...".to_string()),
//!         ..Default::default()
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```

pub mod analytics;
pub mod azure_openai_classic;
pub mod comm;
pub mod config;
pub mod connection;
pub mod credential;
pub mod gateway;
pub mod governance;
pub mod openai_types;
pub mod pii;
pub mod pricing;
pub mod tool_result_projection;
pub mod tools;
pub mod types;

use comm::LlmClient;
use deadpool::unmanaged::Pool;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub use config::LlmConfig;
pub use credential::{LlmCredential, ResolvedLlmConnection};
pub use gateway::{
    LEGACY_LLM_PROXY_KEY_PREFIX, LLM_GATEWAY_AGENT_FINGERPRINT_HEADER, LLM_GATEWAY_AGENT_ID_HEADER, LLM_GATEWAY_AGENT_PRINCIPAL_HEADER,
    LLM_GATEWAY_AGENT_SESSION_HEADER, LLM_GATEWAY_AGENT_TAGS_HEADER, LLM_GATEWAY_KEY_PREFIX, LlmGatewayAgentIdentity, LlmGatewayAuthScheme,
    LlmGatewayControlPlaneAuthMode, LlmGatewayControlPlaneSnapshot, LlmGatewayCredential, LlmGatewayKeyKind, LlmGatewayKeyPolicy,
    LlmGatewayModelCatalog, LlmGatewayPolicy, LlmGatewayRouteDecision, LlmGatewayRouteObservation, LlmGatewayRouteSelector,
    LlmGatewayRouteStats, LlmModelCatalogEntry, LlmModelCatalogPricing, LlmModelLifecycle, LlmModelModality, LlmModelOperation,
    clear_llm_gateway_route_stats, hydrate_llm_gateway_route_stats, llm_gateway_route_stats_snapshot, record_llm_gateway_route_observation,
};
pub use governance::{GovernanceDecision, LlmGovernancePolicy, apply_pre_egress_policy, evaluate_pre_egress_policy};
pub use openai_types::*;
pub use pii::{CustomPiiMatch, CustomPiiMatcher, CustomPiiTerm, LlmPiiScanner, PiiFinding, PiiScanResult, PiiType};
pub use pricing::{
    LlmPriceSnapshot, ModelPricing, PriceArbitrageDecision, PriceArbitrageMode, PriceEstimate, PriceRouteCandidate, PriceSource,
    choose_openrouter_price_route, estimate_cost_micros, estimate_price, openrouter_price_route_candidates, static_model_pricings,
};
pub use tool_result_projection::{
    CompactTable, DEFAULT_TOOL_RESULT_MAX_BYTES, DEFAULT_TOOL_RESULT_MAX_CELLS, DEFAULT_TOOL_RESULT_MAX_ROWS, ToolResultProjection,
    project_tool_result,
};
pub use tools::{SafetyFn, TOOL_DISCOVERY_CACHE_TTL_SECS, ToolDiscoveryCache, ToolName, ToolRuntime};
pub use types::{
    CacheHint, LlmCacheStatus, LlmChatResponse, LlmCompletionTokensDetails, LlmFunctionCall, LlmInvocation, LlmKvCacheMode,
    LlmKvCacheStatus, LlmMessage, LlmMessageKind, LlmMessageRole, LlmOperationEvent, LlmProviderMetadata, LlmRequestOverrides,
    LlmRouteOptimizationMode, LlmStructuredOutputFormat, LlmToolBinding, LlmToolCall, LlmToolChoice, LlmToolConnection, LlmToolDefinition,
    LlmUsage, PolicyAction, SystemPromptBlock, SystemPromptBlockKind, ToolAnnotations, ToolApprovalMode, ToolSafety, TrafficSource,
};

/// Type alias for LLM async client pool (read operations).
pub type LlmAsync = Pool<LlmClient>;

/// Type alias for LLM client pool (write operations).
pub type LlmTx = Pool<LlmClient>;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema, Default)]
pub enum LlmParam {
    #[default]
    Model,
    Temperature,
    MaxTokens,
    TopP,
    TopK,
    BaseUrlOverride,
}
