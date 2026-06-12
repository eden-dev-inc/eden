//! Per-provider model-metadata fetchers.
//!
//! Each provider exposes an authenticated model-listing API. We call it with the
//! endpoint's resolved credential and map the response into [`LlmModelInfo`].
//! Only OpenRouter returns rich metadata (context window, modalities, pricing);
//! the others return little more than model ids.
//!
//! Fetch failures are surfaced as `Err` to the caller, which records the batch as
//! `source = "unavailable"` rather than failing the whole metadata run.

use std::time::Duration;

use llm_core::ResolvedLlmConnection;
use llm_core::connection::LlmProvider;
use serde::Deserialize;

use super::{LlmModelInfo, ModelPricingInfo};

const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Build a reqwest client mirroring the pricing refresh client.
fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder().timeout(FETCH_TIMEOUT).build().map_err(|e| format!("http client: {e}"))
}

/// Fetch the model list for the resolved connection's provider.
///
/// Returns the parsed models on success. Errors are strings describing the
/// failure (network, auth, parse) so the collector can record them without
/// failing the batch.
pub async fn fetch_models(resolved: &ResolvedLlmConnection) -> Result<Vec<LlmModelInfo>, String> {
    let base = resolved.base_url().map_err(|e| format!("base url: {e}"))?;
    let api_key = resolved.api_key.clone();
    let client = http_client()?;

    match resolved.provider {
        LlmProvider::OpenAI => fetch_openai(&client, &base, api_key.as_deref()).await,
        LlmProvider::OpenRouter => fetch_openrouter(&client, &base, api_key.as_deref()).await,
        LlmProvider::Anthropic => fetch_anthropic(&client, &base, api_key.as_deref()).await,
        LlmProvider::Ollama => fetch_ollama(&client, &base).await,
        LlmProvider::AzureOpenAI => fetch_azure(&client, &base, api_key.as_deref(), resolved).await,
    }
}

// ── OpenAI: GET {base}/models → { data: [{ id, owned_by, created }] } ──

#[derive(Deserialize)]
struct OpenAiModelsResponse {
    data: Vec<OpenAiModel>,
}
#[derive(Deserialize)]
struct OpenAiModel {
    id: String,
    #[serde(default)]
    owned_by: Option<String>,
}

async fn fetch_openai(client: &reqwest::Client, base: &str, api_key: Option<&str>) -> Result<Vec<LlmModelInfo>, String> {
    let key = api_key.ok_or("OpenAI requires an API key")?;
    let resp = client.get(format!("{base}/models")).bearer_auth(key).send().await.map_err(|e| format!("fetch: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let body: OpenAiModelsResponse = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok(parse_openai_models(body))
}

fn parse_openai_models(body: OpenAiModelsResponse) -> Vec<LlmModelInfo> {
    body.data
        .into_iter()
        .map(|m| LlmModelInfo {
            display_name: m.owned_by.map(|o| format!("{} ({o})", m.id)),
            id: m.id,
            ..Default::default()
        })
        .collect()
}

// ── OpenRouter: GET {base}/models → rich metadata ──

#[derive(Deserialize)]
struct OpenRouterModelsResponse {
    data: Vec<OpenRouterModel>,
}
#[derive(Deserialize)]
struct OpenRouterModel {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    context_length: Option<u64>,
    #[serde(default)]
    architecture: Option<OpenRouterArchitecture>,
    #[serde(default)]
    top_provider: Option<OpenRouterTopProvider>,
    #[serde(default)]
    pricing: Option<OpenRouterPricing>,
}
#[derive(Deserialize)]
struct OpenRouterArchitecture {
    #[serde(default)]
    input_modalities: Option<Vec<String>>,
    #[serde(default)]
    output_modalities: Option<Vec<String>>,
}
#[derive(Deserialize)]
struct OpenRouterTopProvider {
    #[serde(default)]
    max_completion_tokens: Option<u64>,
}
#[derive(Deserialize)]
struct OpenRouterPricing {
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    completion: Option<String>,
}

async fn fetch_openrouter(client: &reqwest::Client, base: &str, api_key: Option<&str>) -> Result<Vec<LlmModelInfo>, String> {
    // OpenRouter's /models is public, but send the key when present.
    let mut req = client.get(format!("{base}/models")).header("User-Agent", "Eve/1.0");
    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }
    let resp = req.send().await.map_err(|e| format!("fetch: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let body: OpenRouterModelsResponse = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok(parse_openrouter_models(body))
}

/// OpenRouter prices are decimal USD per token strings (e.g. "0.000003").
/// Convert to micro-USD per 1M tokens to match the rest of Eden's pricing.
fn per_token_usd_to_micros_per_million(raw: Option<&str>) -> Option<u64> {
    let parsed: f64 = raw?.trim().parse().ok()?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return None;
    }
    // usd/token * 1e6 tokens * 1e6 micros/usd
    let micros = (parsed * 1e12).round();
    if !micros.is_finite() || micros <= 0.0 || micros > u64::MAX as f64 {
        return None;
    }
    Some(micros as u64)
}

fn parse_openrouter_models(body: OpenRouterModelsResponse) -> Vec<LlmModelInfo> {
    body.data
        .into_iter()
        .map(|m| {
            let mut modalities = Vec::new();
            if let Some(arch) = &m.architecture {
                if let Some(inp) = &arch.input_modalities {
                    modalities.extend(inp.iter().map(|s| format!("in:{s}")));
                }
                if let Some(out) = &arch.output_modalities {
                    modalities.extend(out.iter().map(|s| format!("out:{s}")));
                }
            }
            let pricing = m.pricing.as_ref().and_then(|p| {
                let input = per_token_usd_to_micros_per_million(p.prompt.as_deref());
                let output = per_token_usd_to_micros_per_million(p.completion.as_deref());
                if input.is_some() || output.is_some() {
                    Some(ModelPricingInfo {
                        input_micros_per_million: input.unwrap_or(0),
                        output_micros_per_million: output.unwrap_or(0),
                    })
                } else {
                    None
                }
            });
            LlmModelInfo {
                id: m.id,
                display_name: m.name,
                context_length: m.context_length,
                max_output_tokens: m.top_provider.and_then(|t| t.max_completion_tokens),
                modalities,
                pricing,
            }
        })
        .collect()
}

// ── Anthropic: GET {base}/v1/models → { data: [{ id, display_name }] } ──

#[derive(Deserialize)]
struct AnthropicModelsResponse {
    data: Vec<AnthropicModel>,
}
#[derive(Deserialize)]
struct AnthropicModel {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
}

async fn fetch_anthropic(client: &reqwest::Client, base: &str, api_key: Option<&str>) -> Result<Vec<LlmModelInfo>, String> {
    let key = api_key.ok_or("Anthropic requires an API key")?;
    // Anthropic's base URL is the host root; the models path is /v1/models.
    let url = format!("{base}/v1/models");
    let resp = client
        .get(url)
        .header("x-api-key", key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .send()
        .await
        .map_err(|e| format!("fetch: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let body: AnthropicModelsResponse = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok(parse_anthropic_models(body))
}

fn parse_anthropic_models(body: AnthropicModelsResponse) -> Vec<LlmModelInfo> {
    body.data.into_iter().map(|m| LlmModelInfo { id: m.id, display_name: m.display_name, ..Default::default() }).collect()
}

// ── Ollama: GET {base}/api/tags → { models: [{ name, details }] } ──

#[derive(Deserialize)]
struct OllamaTagsResponse {
    models: Vec<OllamaModel>,
}
#[derive(Deserialize)]
struct OllamaModel {
    name: String,
    #[serde(default)]
    details: Option<OllamaDetails>,
}
#[derive(Deserialize)]
struct OllamaDetails {
    #[serde(default)]
    parameter_size: Option<String>,
    #[serde(default)]
    quantization_level: Option<String>,
}

async fn fetch_ollama(client: &reqwest::Client, base: &str) -> Result<Vec<LlmModelInfo>, String> {
    // Ollama is local + unauthenticated.
    let resp = client.get(format!("{base}/api/tags")).send().await.map_err(|e| format!("fetch: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let body: OllamaTagsResponse = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok(parse_ollama_models(body))
}

fn parse_ollama_models(body: OllamaTagsResponse) -> Vec<LlmModelInfo> {
    body.models
        .into_iter()
        .map(|m| {
            let display_name = m.details.and_then(|d| {
                let parts: Vec<String> = [d.parameter_size, d.quantization_level].into_iter().flatten().collect();
                if parts.is_empty() { None } else { Some(parts.join(" · ")) }
            });
            LlmModelInfo { id: m.name, display_name, ..Default::default() }
        })
        .collect()
}

// ── Azure OpenAI: list deployments → { data: [{ id|model }] } ──

#[derive(Deserialize)]
struct AzureDeploymentsResponse {
    data: Vec<AzureDeployment>,
}
#[derive(Deserialize)]
struct AzureDeployment {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

async fn fetch_azure(
    client: &reqwest::Client,
    base: &str,
    api_key: Option<&str>,
    resolved: &ResolvedLlmConnection,
) -> Result<Vec<LlmModelInfo>, String> {
    let key = api_key.ok_or("Azure OpenAI requires an API key")?;
    let api_version = azure_api_version(resolved).unwrap_or_else(|| "2024-02-01".to_string());
    let url = format!("{base}/openai/deployments?api-version={api_version}");
    let resp = client.get(url).header("api-key", key).send().await.map_err(|e| format!("fetch: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let body: AzureDeploymentsResponse = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok(parse_azure_models(body))
}

/// Pull the configured api-version out of the resolved Azure classic config.
fn azure_api_version(resolved: &ResolvedLlmConnection) -> Option<String> {
    match &resolved.provider_config {
        llm_core::credential::ResolvedProviderConfig::AzureClassic(cfg) => {
            let v = cfg.api_version.trim();
            if v.is_empty() { None } else { Some(v.to_string()) }
        }
        _ => None,
    }
}

fn parse_azure_models(body: AzureDeploymentsResponse) -> Vec<LlmModelInfo> {
    body.data
        .into_iter()
        .filter_map(|d| {
            let id = d.id.or_else(|| d.model.clone())?;
            Some(LlmModelInfo {
                display_name: d.model.filter(|m| *m != id),
                id,
                ..Default::default()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_maps_ids() {
        let body: OpenAiModelsResponse =
            serde_json::from_str(r#"{"data":[{"id":"gpt-4o","owned_by":"openai"},{"id":"gpt-4o-mini"}]}"#).expect("parse");
        let models = parse_openai_models(body);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "gpt-4o");
        assert_eq!(models[0].display_name.as_deref(), Some("gpt-4o (openai)"));
        assert!(models[1].display_name.is_none());
    }

    #[test]
    fn openrouter_maps_rich_metadata() {
        let json = r#"{"data":[{
            "id":"anthropic/claude-3.5-sonnet",
            "name":"Claude 3.5 Sonnet",
            "context_length":200000,
            "architecture":{"input_modalities":["text","image"],"output_modalities":["text"]},
            "top_provider":{"max_completion_tokens":8192},
            "pricing":{"prompt":"0.000003","completion":"0.000015"}
        }]}"#;
        let body: OpenRouterModelsResponse = serde_json::from_str(json).expect("parse");
        let models = parse_openrouter_models(body);
        assert_eq!(models.len(), 1);
        let m = &models[0];
        assert_eq!(m.context_length, Some(200_000));
        assert_eq!(m.max_output_tokens, Some(8192));
        assert!(m.modalities.contains(&"in:image".to_string()));
        assert!(m.modalities.contains(&"out:text".to_string()));
        let p = m.pricing.as_ref().expect("pricing");
        // 0.000003 usd/token -> 3 usd / 1M tokens -> 3_000_000 micros / 1M
        assert_eq!(p.input_micros_per_million, 3_000_000);
        assert_eq!(p.output_micros_per_million, 15_000_000);
    }

    #[test]
    fn anthropic_maps_display_name() {
        let body: AnthropicModelsResponse =
            serde_json::from_str(r#"{"data":[{"id":"claude-opus-4","display_name":"Claude Opus 4"}]}"#).expect("parse");
        let models = parse_anthropic_models(body);
        assert_eq!(models[0].id, "claude-opus-4");
        assert_eq!(models[0].display_name.as_deref(), Some("Claude Opus 4"));
    }

    #[test]
    fn ollama_joins_details() {
        let json = r#"{"models":[{"name":"llama3:8b","details":{"parameter_size":"8B","quantization_level":"Q4_0"}}]}"#;
        let body: OllamaTagsResponse = serde_json::from_str(json).expect("parse");
        let models = parse_ollama_models(body);
        assert_eq!(models[0].id, "llama3:8b");
        assert_eq!(models[0].display_name.as_deref(), Some("8B · Q4_0"));
    }

    #[test]
    fn azure_prefers_deployment_id() {
        let body: AzureDeploymentsResponse =
            serde_json::from_str(r#"{"data":[{"id":"my-gpt4o","model":"gpt-4o"},{"model":"gpt-4o-mini"}]}"#).expect("parse");
        let models = parse_azure_models(body);
        assert_eq!(models[0].id, "my-gpt4o");
        assert_eq!(models[0].display_name.as_deref(), Some("gpt-4o"));
        assert_eq!(models[1].id, "gpt-4o-mini");
    }

    #[test]
    fn price_parse_rejects_zero_and_garbage() {
        assert_eq!(per_token_usd_to_micros_per_million(Some("0")), None);
        assert_eq!(per_token_usd_to_micros_per_million(Some("")), None);
        assert_eq!(per_token_usd_to_micros_per_million(Some("abc")), None);
        assert_eq!(per_token_usd_to_micros_per_million(Some("NaN")), None);
        assert_eq!(per_token_usd_to_micros_per_million(Some("inf")), None);
        assert_eq!(per_token_usd_to_micros_per_million(Some("1e309")), None);
        assert_eq!(per_token_usd_to_micros_per_million(Some("1e100")), None);
        assert_eq!(per_token_usd_to_micros_per_million(None), None);
    }
}
