use super::features::LlmGatewayFeatureEngine;
use endpoint_core::llm_core::{
    LlmGatewayModelCatalog, LlmModelCatalogEntry, LlmModelCatalogPricing, LlmModelLifecycle, LlmModelModality, LlmModelOperation,
};
use serde_json::{Value, json};

pub(super) struct LlmGatewayModelCatalogResponder;

impl LlmGatewayModelCatalogResponder {
    pub(super) fn openai_models_response(feature_engine: &LlmGatewayFeatureEngine, catalog: &LlmGatewayModelCatalog) -> Value {
        let catalog = catalog.filter_allowed(feature_engine.allowed_models());
        let data = catalog.entries().iter().map(Self::openai_model_entry).collect::<Vec<_>>();

        json!({
            "object": "list",
            "data": data
        })
    }

    fn openai_model_entry(entry: &LlmModelCatalogEntry) -> Value {
        json!({
            "id": entry.id,
            "object": "model",
            "created": 0,
            "owned_by": entry.provider,
            "eden": {
                "provider": entry.provider,
                "model": entry.model,
                "aliases": entry.aliases,
                "regions": entry.regions,
                "context_window_tokens": entry.context_window_tokens,
                "modalities": Self::modalities(&entry.modalities),
                "operations": Self::operations(&entry.operations),
                "supports_tools": entry.supports_tools,
                "supports_streaming": entry.supports_streaming,
                "supports_json_schema": entry.supports_json_schema,
                "fallback_group": entry.fallback_group,
                "lifecycle": Self::lifecycle(entry.lifecycle),
                "pricing": entry.pricing.as_ref().map(Self::pricing),
            }
        })
    }

    fn modalities(modalities: &[LlmModelModality]) -> Vec<&'static str> {
        modalities.iter().map(|modality| modality.as_str()).collect()
    }

    fn operations(operations: &[LlmModelOperation]) -> Vec<&'static str> {
        operations.iter().map(|operation| operation.as_str()).collect()
    }

    fn lifecycle(lifecycle: LlmModelLifecycle) -> &'static str {
        lifecycle.as_str()
    }

    fn pricing(pricing: &LlmModelCatalogPricing) -> Value {
        json!({
            "source": pricing.source.as_str(),
            "input_micros_per_million": pricing.input_micros_per_million,
            "output_micros_per_million": pricing.output_micros_per_million,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn openai_models_response_includes_eden_catalog_metadata() {
        let response = LlmGatewayModelCatalogResponder::openai_models_response(
            &LlmGatewayFeatureEngine::default(),
            &LlmGatewayModelCatalog::builtin(),
        );
        let data = response["data"].as_array().expect("data should be an array");
        let gpt_4_1 = data.iter().find(|entry| entry["id"] == json!("gpt-4.1")).expect("gpt-4.1 should be listed");

        assert_eq!(gpt_4_1["object"], json!("model"));
        assert_eq!(gpt_4_1["owned_by"], json!("openai"));
        assert_eq!(gpt_4_1["eden"]["provider"], json!("openai"));
        assert_eq!(gpt_4_1["eden"]["context_window_tokens"], json!(1_047_576));
        assert_eq!(gpt_4_1["eden"]["pricing"]["source"], json!("static_fallback"));
    }
}
