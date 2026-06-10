//! LLM endpoint metadata collector.
//!
//! Collects the live model catalogue from the endpoint's provider by calling its
//! authenticated model-listing API with the resolved credential (see
//! [`sync::fetch_models`]). One Low-frequency job ("llm.models") — model lists
//! are slow-moving and the provider APIs are rate-limited. A fetch failure is
//! recorded as `source = "unavailable"` (with a note) and never fails the batch.

mod sync;

use crate::ep::LlmAsync;
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{EpMetadata, MetadataJob, SyncFrequency, SyncMetadata};
use ep_core::define_metadata_serializer_stuff;
use format::endpoint::EpKind;
use llm_core::comm::LlmClient;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::time::{SystemTime, UNIX_EPOCH};

/// Per-model pricing (micro-USD per 1M tokens), populated only by OpenRouter.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelPricingInfo {
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

/// A single model exposed by the provider. Only `id` is guaranteed; richer
/// fields are populated where the provider's API supplies them (OpenRouter).
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct LlmModelInfo {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modalities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing: Option<ModelPricingInfo>,
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct LlmMetadata {
    /// Provider identifier (`openai`, `anthropic`, `openrouter`, `ollama`, `azureopenai`).
    pub provider: String,
    /// Discovered models (empty when the provider call failed).
    pub models: Vec<LlmModelInfo>,
    /// Number of models discovered (convenience for consumers).
    pub model_count: u32,
    /// `live` when the provider returned a model list; `unavailable` on failure;
    /// empty before the first collection.
    pub source: String,
    /// Diagnostic note (e.g. the fetch error) when `source = "unavailable"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Unix epoch seconds of the last collection attempt.
    pub collected_at: u64,
}

impl LlmMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    fn now_epoch_secs() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
    }
}

impl SyncMetadata<LlmAsync> for LlmMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<LlmAsync, Self>> {
        if !matches!(frequency, SyncFrequency::Low) {
            return Vec::new();
        }

        vec![MetadataJob::new(
            "llm.models".to_string(),
            SyncFrequency::Low,
            move |metadata: &mut Self, pool: LlmAsync, _telemetry, _capabilities| {
                Box::pin(async move {
                    metadata.collected_at = Self::now_epoch_secs();

                    // Pull a client from the pool and read its resolved connection
                    // (provider + api_key + base_url).
                    let client = match pool.get().await {
                        Ok(client) => client,
                        Err(e) => {
                            metadata.source = "unavailable".to_string();
                            metadata.note = Some(format!("no LLM client available: {e}"));
                            metadata.models.clear();
                            metadata.model_count = 0;
                            return Ok(());
                        }
                    };
                    let client_ref: &LlmClient = &client;
                    let resolved = match client_ref.resolved_connection() {
                        Ok(resolved) => resolved,
                        Err(e) => {
                            metadata.source = "unavailable".to_string();
                            metadata.note = Some(format!("failed to resolve connection: {e}"));
                            metadata.models.clear();
                            metadata.model_count = 0;
                            return Ok(());
                        }
                    };
                    metadata.provider = resolved.provider.as_str().to_string();

                    match sync::fetch_models(&resolved).await {
                        Ok(models) => {
                            metadata.model_count = models.len() as u32;
                            metadata.models = models;
                            metadata.source = "live".to_string();
                            metadata.note = None;
                        }
                        Err(e) => {
                            metadata.source = "unavailable".to_string();
                            metadata.note = Some(e);
                            metadata.models.clear();
                            metadata.model_count = 0;
                        }
                    }
                    Ok(())
                })
            },
        )]
    }
    // discover_capabilities: default impl returns UnknownCapabilities, which is
    // correct here — provider model-listing has no capability gates.
}

impl EpMetadata for LlmMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Llm
    }
    fn as_metadata(self: Box<Self>) -> Box<dyn EpMetadata> {
        self
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn clone_box(&self) -> Box<dyn EpMetadata> {
        Box::new(self.clone())
    }

    fn to_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        borsh::to_writer(writer, self)
    }
}

define_metadata_serializer_stuff!(EpKind::Llm => LlmMetadata);
