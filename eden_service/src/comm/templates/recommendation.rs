use eden_core::error::EpError;
#[cfg(feature = "llm")]
use eden_core::request::InternalLlmSettings;
use eden_core::request::ServerData;
#[cfg(feature = "llm")]
use eden_core::telemetry::FastSpanAttribute;
use eden_core::telemetry::TelemetryWrapper;
#[cfg(feature = "llm")]
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
#[cfg(feature = "llm")]
use endpoint_core::ep_core::database::template::handlebars::{ConditionalBlock, FieldInfo, FieldRequirement, FieldSource};
#[cfg(feature = "llm")]
use endpoint_core::llm_core::comm::LlmClient;
#[cfg(feature = "llm")]
use endpoint_core::llm_core::connection::{LlmConnection, LlmConnectionDefaults, LlmProvider, LlmTarget};
#[cfg(feature = "llm")]
use endpoint_core::llm_core::types::{LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole, LlmRequestOverrides};
#[cfg(feature = "llm")]
use log::{debug, warn};
#[cfg(feature = "llm")]
use serde_json::{Value, json};
#[cfg(feature = "llm")]
use std::sync::{Arc, RwLock};

#[cfg(feature = "llm")]
const DEFAULT_SYSTEM_PROMPT: &str = "You are Eden's internal template advisor. Analyze template metadata and return concise, actionable recommendations (max 5 bullet points). Focus on data validation, caching impact, endpoint behavior, and opportunities to leverage LLM capabilities. If no issues are found, respond with a single sentence noting that the template looks solid and suggest documenting any assumptions.";

#[cfg(feature = "llm")]
pub async fn generate_llm_recommendation(
    server_data: &ServerData,
    template_schema: &TemplateSchema,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<Option<String>, EpError> {
    let settings = match server_data.internal_llm() {
        Some(settings) => settings,
        None => {
            debug!("Internal LLM settings not configured; skipping recommendation generation");
            return Ok(None);
        }
    };

    let mut span = telemetry_wrapper.client_tracer("template.generate_llm_recommendation");

    span.add_event(
        "prepare_llm_recommendation".to_string(),
        vec![FastSpanAttribute::new("template_id", template_schema.id().to_string())],
    );

    let provider = match parse_provider(&settings.provider) {
        Ok(provider) => provider,
        Err(err) => {
            warn!("Unsupported internal LLM provider '{}': {}", settings.provider, err);
            return Ok(None);
        }
    };

    let overrides = build_overrides(settings);
    let connection = build_connection(settings, provider)?;
    let resolved = match connection.resolve(None) {
        Ok(resolved) => resolved,
        Err(err) => {
            warn!("Failed to resolve internal LLM connection: {}", err);
            return Ok(None);
        }
    };

    let client = match LlmClient::new(Arc::new(RwLock::new(resolved)), endpoint_core::llm_core::config::DEFAULT_MAX_TOOL_PASSES) {
        Ok(client) => client,
        Err(err) => {
            warn!("Failed to initialize LLM client: {}", err);
            return Ok(None);
        }
    };

    let prompt_payload = build_prompt_payload(template_schema, settings);
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: prompt_payload,
            kind: LlmMessageKind::Text,
        }],
        system_prompt: Some(settings.system_prompt.clone().unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_string())),
        overrides,
        turn_context: None,
        ..Default::default()
    };

    match client.chat(&invocation).await {
        Ok(response) => {
            let content = response.message.content.trim();
            if content.is_empty() {
                Ok(None)
            } else {
                Ok(Some(content.to_string()))
            }
        }
        Err(err) => {
            warn!("Internal LLM recommendation call failed: {}", err);
            Ok(None)
        }
    }
}

#[cfg(not(feature = "llm"))]
pub async fn generate_llm_recommendation(
    _server_data: &ServerData,
    _template_schema: &TemplateSchema,
    _telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<Option<String>, EpError> {
    Ok(None)
}

#[cfg(feature = "llm")]
fn parse_provider(raw: &str) -> Result<LlmProvider, EpError> {
    raw.parse::<LlmProvider>().map_err(|_| EpError::request(format!("unknown internal LLM provider '{}'", raw.trim())))
}

#[cfg(feature = "llm")]
fn build_connection(settings: &InternalLlmSettings, provider: LlmProvider) -> Result<LlmConnection, EpError> {
    let defaults = LlmConnectionDefaults {
        model: settings.model.clone(),
        temperature: settings.temperature,
        max_tokens: settings.max_tokens,
        top_p: None,
        top_k: None,
        base_url_override: settings.base_url.clone(),
    };

    Ok(LlmConnection {
        target: LlmTarget::new(provider, defaults),
        credential_id: None,
        inline_api_key: settings.api_key.clone(),
    })
}

#[cfg(feature = "llm")]
fn build_overrides(settings: &InternalLlmSettings) -> LlmRequestOverrides {
    LlmRequestOverrides {
        temperature: settings.temperature,
        max_tokens: settings.max_tokens,
        ..Default::default()
    }
}

#[cfg(feature = "llm")]
fn build_prompt_payload(template_schema: &TemplateSchema, settings: &InternalLlmSettings) -> String {
    let template = template_schema.template();
    let summary = json!({
        "template": {
            "id": template_schema.id().to_string(),
            "uuid": template_schema.template_uuid().to_string(),
            "description": template_schema.description(),
            "llm_recommendation": template_schema.llm_recommendation(),
        },
        "binding": {
            "endpoint_uuid": template.endpoint_uuid().to_string(),
            "endpoint_kind": template.ep_kind(),
            "template_kind": template.kind(),
        },
        "handlebars": {
            "template": template.handlebars().template(),
            "fields": summarize_fields(template.handlebars().fields()),
            "conditions": summarize_conditions(template.handlebars().conditional_blocks()),
        },
        "cache": template.cache(),
        "internal_llm": {
            "provider": settings.provider,
            "model": settings.model,
        }
    });

    let pretty = serde_json::to_string_pretty(&summary).unwrap_or_else(|_| summary.to_string());

    format!("Provide Eden template recommendations for the following definition:\n{}", pretty)
}

#[cfg(feature = "llm")]
fn summarize_fields(fields: &[FieldInfo]) -> Value {
    Value::Array(
        fields
            .iter()
            .map(|field| {
                json!({
                    "name": field.name,
                    "type": field.field_type.to_string(),
                    "required": matches!(field.requirement, FieldRequirement::Required),
                    "default_value": field.default_value,
                    "occurrences": field.occurrences.iter().map(|occurrence| {
                        json!({
                            "json_path": occurrence.json_path,
                            "context": format!("{:?}", occurrence.context_type),
                            "source": summarize_field_source(&occurrence.source),
                        })
                    }).collect::<Vec<_>>()
                })
            })
            .collect(),
    )
}

#[cfg(feature = "llm")]
fn summarize_field_source(source: &FieldSource) -> Value {
    match source {
        FieldSource::BaseTemplate => Value::String("base_template".to_string()),
        FieldSource::ConditionalBlock(index) => Value::String(format!("conditional_block_{}", index)),
    }
}

#[cfg(feature = "llm")]
fn summarize_conditions(blocks: &[ConditionalBlock]) -> Value {
    Value::Array(
        blocks
            .iter()
            .map(|block| {
                json!({
                    "trigger_field": block.trigger_field,
                    "merge_at_path": block.merge_at_path,
                    "template_addition": block.template_addition,
                })
            })
            .collect(),
    )
}
