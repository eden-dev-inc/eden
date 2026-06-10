use super::LlmFeatureMode;
use crate::analysis::LlmPayloadAnalysis;
use serde_json::Value;

pub(super) struct LlmRouteClassifier;

impl LlmRouteClassifier {
    pub(super) fn classify(analysis: &LlmPayloadAnalysis, body: &Value, mode: LlmFeatureMode) -> String {
        if mode == LlmFeatureMode::Disabled {
            return "disabled".to_string();
        }

        if analysis.contains_pii() {
            return "privacy_sensitive".to_string();
        }

        if analysis.tool_definition_count > 0 || analysis.tool_call_count > 0 {
            return "tool_capable".to_string();
        }

        if body.get("response_format").is_some() {
            return "structured_output".to_string();
        }

        if analysis.image_part_count > 0 {
            return "multimodal".to_string();
        }

        if analysis.prompt_characters > 32_000 {
            return "large_context".to_string();
        }

        "default".to_string()
    }
}

pub(super) struct LlmResponseCacheClassifier;

impl LlmResponseCacheClassifier {
    pub(super) fn classify(analysis: &LlmPayloadAnalysis, body: &Value, mode: LlmFeatureMode) -> &'static str {
        if mode == LlmFeatureMode::Disabled {
            return "disabled";
        }

        if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
            return "bypass_streaming";
        }

        if analysis.contains_pii() {
            return "bypass_pii";
        }

        if analysis.tool_definition_count > 0 || analysis.tool_call_count > 0 {
            return "bypass_tools";
        }

        if analysis.prompt_characters < 256 {
            return "bypass_small_prompt";
        }

        "eligible"
    }
}
