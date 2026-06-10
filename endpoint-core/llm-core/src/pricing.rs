//! LLM cost estimation using dynamic OpenRouter pricing + static fallback.
//!
//! The primary source is OpenRouter's public `/api/v1/models` endpoint, which
//! returns per-token pricing for 400+ models across all providers. A background
//! task refreshes the cache periodically. When OpenRouter is unreachable or the
//! model is unknown, we fall back to a hardcoded table of OpenAI and Anthropic
//! prices (updated March 2026).

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use utoipa::ToSchema;

// ── Dynamic pricing cache ────────────────────────────────────────────────

/// Per-token prices in microdollars per 1M tokens.
#[derive(Debug, Clone, Copy)]
pub struct TokenPrice {
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum PriceArbitrageMode {
    #[default]
    Disabled,
    SameModelCheapest,
    AllowedModelsCheapest,
}

impl PriceArbitrageMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::SameModelCheapest => "same_model_cheapest",
            Self::AllowedModelsCheapest => "allowed_models_cheapest",
        }
    }
}

impl std::fmt::Display for PriceArbitrageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PriceSource {
    DynamicOpenRouter,
    StaticFallback,
}

impl PriceSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DynamicOpenRouter => "dynamic_openrouter",
            Self::StaticFallback => "static_fallback",
        }
    }
}

impl std::fmt::Display for PriceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceEstimate {
    pub provider: String,
    pub model: String,
    pub source: PriceSource,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
    pub estimated_cost_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceRouteCandidate {
    pub provider: String,
    pub model: String,
    pub source: PriceSource,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
    pub estimated_cost_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceArbitrageDecision {
    pub mode: PriceArbitrageMode,
    pub requested_model: String,
    pub selected_model: String,
    pub price_source: Option<PriceSource>,
    pub baseline_estimated_cost_micros: u64,
    pub selected_estimated_cost_micros: u64,
    pub estimated_savings_micros: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmPriceSnapshot {
    pub fetched_at: DateTime<Utc>,
    pub provider: String,
    pub model: String,
    pub source: PriceSource,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

struct PricingCache {
    prices: HashMap<String, TokenPrice>,
    last_refresh: Option<Instant>,
}

static DYNAMIC_CACHE: Lazy<RwLock<PricingCache>> = Lazy::new(|| RwLock::new(PricingCache { prices: HashMap::new(), last_refresh: None }));
static PRICE_SNAPSHOT_SENDER: Lazy<RwLock<Option<mpsc::Sender<LlmPriceSnapshot>>>> = Lazy::new(|| RwLock::new(None));

const REFRESH_INTERVAL: Duration = Duration::from_secs(6 * 3600); // 6 hours
const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";

/// OpenRouter /api/v1/models response shape (only the fields we need).
#[derive(Deserialize)]
struct OpenRouterModelsResponse {
    data: Vec<OpenRouterModel>,
}

#[derive(Deserialize)]
struct OpenRouterModel {
    id: String,
    pricing: Option<OpenRouterPricing>,
}

#[derive(Deserialize)]
struct OpenRouterPricing {
    prompt: Option<String>,
    completion: Option<String>,
}

/// Fetch pricing from OpenRouter and update the cache. Call this on startup
/// and periodically from a background task.
pub async fn refresh_openrouter_pricing() -> Result<usize, String> {
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| format!("http client: {e}"))?
        .get(OPENROUTER_MODELS_URL)
        .header("User-Agent", "Eve/1.0")
        .send()
        .await
        .map_err(|e| format!("fetch failed: {e}"))?;

    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }

    let body: OpenRouterModelsResponse = response.json().await.map_err(|e| format!("parse: {e}"))?;

    let fetched_at = Utc::now();
    let mut prices = HashMap::with_capacity(body.data.len());
    let mut snapshots = Vec::with_capacity(body.data.len());
    for model in &body.data {
        if let Some(ref pricing) = model.pricing {
            let input = parse_per_token_to_micros_per_million(pricing.prompt.as_deref());
            let output = parse_per_token_to_micros_per_million(pricing.completion.as_deref());
            if input > 0 || output > 0 {
                let model_id = model.id.to_lowercase();
                let (provider, canonical_model) = split_openrouter_model_id(&model_id);
                let price = TokenPrice {
                    input_micros_per_million: input,
                    output_micros_per_million: output,
                };
                snapshots.push(LlmPriceSnapshot {
                    fetched_at,
                    provider,
                    model: canonical_model,
                    source: PriceSource::DynamicOpenRouter,
                    input_micros_per_million: input,
                    output_micros_per_million: output,
                });
                prices.insert(model_id, price);
            }
        }
    }

    let count = prices.len();
    if let Ok(mut cache) = DYNAMIC_CACHE.write() {
        cache.prices = prices;
        cache.last_refresh = Some(Instant::now());
    }
    publish_price_snapshots(snapshots);

    Ok(count)
}

pub fn set_llm_price_snapshot_sender(sender: Option<mpsc::Sender<LlmPriceSnapshot>>) {
    if let Ok(mut guard) = PRICE_SNAPSHOT_SENDER.write() {
        *guard = sender;
    }
}

pub fn clear_llm_price_snapshot_sender() {
    set_llm_price_snapshot_sender(None);
}

fn publish_price_snapshots(snapshots: Vec<LlmPriceSnapshot>) {
    let sender = match PRICE_SNAPSHOT_SENDER.read() {
        Ok(guard) => guard.clone(),
        Err(_) => None,
    };

    let Some(sender) = sender else {
        return;
    };

    for snapshot in snapshots {
        let _ = sender.try_send(snapshot);
    }
}

/// Check if the cache needs refresh.
pub fn pricing_cache_stale() -> bool {
    DYNAMIC_CACHE.read().map(|c| c.last_refresh.is_none_or(|t| t.elapsed() > REFRESH_INTERVAL)).unwrap_or(true)
}

/// Spawn a background task that refreshes OpenRouter pricing every 6 hours.
pub fn spawn_pricing_refresh_task() {
    tokio::spawn(async {
        loop {
            match refresh_openrouter_pricing().await {
                Ok(count) => {
                    tracing::info!(models = count, "refreshed OpenRouter pricing cache");
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to refresh OpenRouter pricing; using fallback");
                }
            }
            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    });
}

/// Convert OpenRouter's per-token price string (e.g. "0.000003") to
/// microdollars per 1M tokens.
fn parse_per_token_to_micros_per_million(value: Option<&str>) -> u64 {
    let Some(s) = value else { return 0 };
    let per_token: f64 = s.parse().unwrap_or(0.0);
    // per_token is in dollars per 1 token. Multiply by 1M to get $/1M tokens,
    // then by 1M to get microdollars/1M tokens.
    (per_token * 1_000_000.0 * 1_000_000.0).round() as u64
}

fn lookup_dynamic(model: &str) -> Option<TokenPrice> {
    let cache = DYNAMIC_CACHE.read().ok()?;
    // Try exact match first
    if let Some(price) = cache.prices.get(model) {
        return Some(*price);
    }
    // Try without vendor prefix (e.g., "gpt-4o" might be stored as "openai/gpt-4o")
    for (key, price) in &cache.prices {
        if key.ends_with(&format!("/{model}")) {
            return Some(*price);
        }
    }
    None
}

fn dynamic_route_candidates(model: &str, prompt_tokens: u32, completion_tokens: u32) -> Vec<PriceRouteCandidate> {
    let normalized_model = normalize_model(model);
    let cache = match DYNAMIC_CACHE.read() {
        Ok(cache) => cache,
        Err(_) => return Vec::new(),
    };

    let mut candidates = Vec::new();
    for (key, price) in &cache.prices {
        let route_model = key.as_str();
        if route_model == normalized_model || route_model.ends_with(&format!("/{normalized_model}")) {
            candidates.push(route_candidate_from_price(
                "openrouter",
                route_model,
                *price,
                PriceSource::DynamicOpenRouter,
                prompt_tokens,
                completion_tokens,
            ));
        }
    }
    candidates
        .sort_by(|left, right| left.estimated_cost_micros.cmp(&right.estimated_cost_micros).then_with(|| left.model.cmp(&right.model)));
    candidates.dedup_by(|left, right| left.model == right.model);
    candidates
}

// ── Static fallback table ────────────────────────────────────────────────

/// Static pricing entry for when OpenRouter is unreachable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelPricing {
    pub provider: &'static str,
    pub canonical_model: &'static str,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
    pub aliases: &'static [&'static str],
}

/// Static fallback pricing entries bundled with Eden.
pub fn static_model_pricings() -> impl Iterator<Item = &'static ModelPricing> {
    OPENAI_MODELS.iter().chain(ANTHROPIC_MODELS.iter())
}

/// A current per-model price, resolved from the live dynamic cache when present,
/// otherwise the bundled static fallback table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentModelPrice {
    pub provider: String,
    pub model: String,
    pub source: PriceSource,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

/// Snapshot of current per-model prices for display/reference.
///
/// Merges the live OpenRouter `DYNAMIC_CACHE` over the static fallback table:
/// dynamic entries win, and static entries fill any gaps. Keyed by
/// `provider/model` so an OpenRouter id and a static entry for the same model
/// don't duplicate. Sorted by `(provider, model)` for stable output.
pub fn current_pricings() -> Vec<CurrentModelPrice> {
    let mut by_key: HashMap<String, CurrentModelPrice> = HashMap::new();

    // Static fallback first (lowest priority).
    for pricing in static_model_pricings() {
        let key = format!("{}/{}", pricing.provider, pricing.canonical_model);
        by_key.insert(
            key,
            CurrentModelPrice {
                provider: pricing.provider.to_string(),
                model: pricing.canonical_model.to_string(),
                source: PriceSource::StaticFallback,
                input_micros_per_million: pricing.input_micros_per_million,
                output_micros_per_million: pricing.output_micros_per_million,
            },
        );
    }

    // Live dynamic cache overrides. Cache keys are OpenRouter model ids, which
    // may be `provider/model` or a bare model; `split_openrouter_model_id`
    // normalizes both into a (provider, model) pair.
    if let Ok(cache) = DYNAMIC_CACHE.read() {
        for (model_id, price) in cache.prices.iter() {
            let (provider, model) = split_openrouter_model_id(model_id);
            let key = format!("{provider}/{model}");
            by_key.insert(
                key,
                CurrentModelPrice {
                    provider,
                    model,
                    source: PriceSource::DynamicOpenRouter,
                    input_micros_per_million: price.input_micros_per_million,
                    output_micros_per_million: price.output_micros_per_million,
                },
            );
        }
    }

    let mut out: Vec<CurrentModelPrice> = by_key.into_values().collect();
    out.sort_by(|a, b| a.provider.cmp(&b.provider).then_with(|| a.model.cmp(&b.model)));
    out
}

// Prices as of March 18, 2026 from https://pricepertoken.com/pricing-page/provider/openai
const OPENAI_MODELS: &[ModelPricing] = &[
    // GPT-5.4 series
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.4-pro",
        input_micros_per_million: 30_000_000,
        output_micros_per_million: 180_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.4",
        input_micros_per_million: 2_500_000,
        output_micros_per_million: 15_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.4-mini",
        input_micros_per_million: 750_000,
        output_micros_per_million: 4_500_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.4-nano",
        input_micros_per_million: 200_000,
        output_micros_per_million: 1_250_000,
        aliases: &[],
    },
    // GPT-5.3 series
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.3",
        input_micros_per_million: 1_750_000,
        output_micros_per_million: 14_000_000,
        aliases: &["gpt-5.3-chat", "gpt-5.3-codex"],
    },
    // GPT-5.2 series
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.2-pro",
        input_micros_per_million: 10_500_000,
        output_micros_per_million: 84_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.2",
        input_micros_per_million: 1_750_000,
        output_micros_per_million: 14_000_000,
        aliases: &["gpt-5.2-chat", "gpt-5.2-codex"],
    },
    // GPT-5.1 series
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.1",
        input_micros_per_million: 1_250_000,
        output_micros_per_million: 10_000_000,
        aliases: &["gpt-5.1-chat", "gpt-5.1-codex", "gpt-5.1-codex-max"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5.1-codex-mini",
        input_micros_per_million: 250_000,
        output_micros_per_million: 2_000_000,
        aliases: &[],
    },
    // GPT-5 series
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5-pro",
        input_micros_per_million: 15_000_000,
        output_micros_per_million: 120_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5",
        input_micros_per_million: 1_250_000,
        output_micros_per_million: 10_000_000,
        aliases: &["gpt-5-chat", "gpt-5-codex"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5-mini",
        input_micros_per_million: 250_000,
        output_micros_per_million: 2_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-5-nano",
        input_micros_per_million: 50_000,
        output_micros_per_million: 400_000,
        aliases: &[],
    },
    // O-series reasoning models
    ModelPricing {
        provider: "openai",
        canonical_model: "o4-mini",
        input_micros_per_million: 550_000,
        output_micros_per_million: 2_200_000,
        aliases: &["o4-mini-deep-research"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "o3-pro",
        input_micros_per_million: 20_000_000,
        output_micros_per_million: 80_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "o3",
        input_micros_per_million: 2_000_000,
        output_micros_per_million: 8_000_000,
        aliases: &["o3-2025-04-16"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "o3-mini",
        input_micros_per_million: 1_100_000,
        output_micros_per_million: 4_400_000,
        aliases: &["o3-mini-2025-01-31", "o3-mini-high"],
    },
    // GPT-4.x series
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4.1",
        input_micros_per_million: 2_000_000,
        output_micros_per_million: 8_000_000,
        aliases: &["gpt-4.1-2025-04-14"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4.1-mini",
        input_micros_per_million: 200_000,
        output_micros_per_million: 800_000,
        aliases: &["gpt-4.1-mini-2025-04-14"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4.1-nano",
        input_micros_per_million: 50_000,
        output_micros_per_million: 200_000,
        aliases: &["gpt-4.1-nano-2025-04-14"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4o",
        input_micros_per_million: 2_500_000,
        output_micros_per_million: 10_000_000,
        aliases: &["gpt-4o-2024-11-20", "gpt-4o-2024-08-06", "gpt-4o-2024-05-13"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4o-mini",
        input_micros_per_million: 150_000,
        output_micros_per_million: 600_000,
        aliases: &["gpt-4o-mini-2024-07-18"],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4-turbo",
        input_micros_per_million: 5_000_000,
        output_micros_per_million: 15_000_000,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "gpt-4",
        input_micros_per_million: 30_000_000,
        output_micros_per_million: 60_000_000,
        aliases: &["gpt-4-0613"],
    },
    // Embeddings
    ModelPricing {
        provider: "openai",
        canonical_model: "text-embedding-3-large",
        input_micros_per_million: 130_000,
        output_micros_per_million: 0,
        aliases: &[],
    },
    ModelPricing {
        provider: "openai",
        canonical_model: "text-embedding-3-small",
        input_micros_per_million: 20_000,
        output_micros_per_million: 0,
        aliases: &[],
    },
];

// Prices as of March 18, 2026 from https://pricepertoken.com/pricing-page/provider/anthropic
const ANTHROPIC_MODELS: &[ModelPricing] = &[
    // Opus series
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-opus-4.6",
        input_micros_per_million: 5_000_000,
        output_micros_per_million: 25_000_000,
        aliases: &["claude-opus-4-6"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-opus-4.5",
        input_micros_per_million: 5_000_000,
        output_micros_per_million: 25_000_000,
        aliases: &["claude-opus-4-5"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-opus-4.1",
        input_micros_per_million: 15_000_000,
        output_micros_per_million: 75_000_000,
        aliases: &["claude-opus-4-1"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-opus-4",
        input_micros_per_million: 15_000_000,
        output_micros_per_million: 75_000_000,
        aliases: &["claude-opus-4-20250514"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-3-opus",
        input_micros_per_million: 15_000_000,
        output_micros_per_million: 75_000_000,
        aliases: &["claude-3-opus-latest"],
    },
    // Sonnet series
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-sonnet-4.6",
        input_micros_per_million: 3_000_000,
        output_micros_per_million: 15_000_000,
        aliases: &["claude-sonnet-4-6"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-sonnet-4.5",
        input_micros_per_million: 3_000_000,
        output_micros_per_million: 15_000_000,
        aliases: &["claude-sonnet-4-5"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-sonnet-4",
        input_micros_per_million: 3_000_000,
        output_micros_per_million: 15_000_000,
        aliases: &[
            "claude-sonnet-4-20250514",
            "claude-sonnet-3.7",
            "claude-sonnet-3-7-latest",
            "claude-3-7-sonnet-latest",
            "claude-sonnet-3.5",
            "claude-sonnet-3-5-latest",
            "claude-3-5-sonnet-latest",
        ],
    },
    // Haiku series
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-haiku-4.5",
        input_micros_per_million: 1_000_000,
        output_micros_per_million: 5_000_000,
        aliases: &["claude-haiku-4-5-latest"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-haiku-3.5",
        input_micros_per_million: 800_000,
        output_micros_per_million: 4_000_000,
        aliases: &["claude-haiku-3-5-latest", "claude-3-5-haiku-latest"],
    },
    ModelPricing {
        provider: "anthropic",
        canonical_model: "claude-haiku-3",
        input_micros_per_million: 250_000,
        output_micros_per_million: 1_250_000,
        aliases: &["claude-3-haiku-latest"],
    },
];

// ── Public API ───────────────────────────────────────────────────────────

/// Estimate provider cost in microdollars.
///
/// Resolution order:
/// 1. Dynamic OpenRouter cache (covers 400+ models from all providers)
/// 2. Static fallback table (OpenAI + Anthropic hardcoded)
/// 3. Returns 0 for unknown models
pub fn estimate_cost_micros(provider: &str, model: &str, prompt_tokens: u32, completion_tokens: u32) -> u64 {
    estimate_price(provider, model, prompt_tokens, completion_tokens)
        .map(|estimate| estimate.estimated_cost_micros)
        .unwrap_or_default()
}

pub fn estimate_price(provider: &str, model: &str, prompt_tokens: u32, completion_tokens: u32) -> Option<PriceEstimate> {
    let normalized_provider = normalize_provider(provider);
    if normalized_provider == "ollama" {
        return None;
    }

    let normalized_model = normalize_model(model);

    // 1. Try dynamic OpenRouter cache
    if let Some(price) = lookup_dynamic(&normalized_model) {
        return Some(price_estimate_from_price(
            &normalized_provider,
            &normalized_model,
            price,
            PriceSource::DynamicOpenRouter,
            prompt_tokens,
            completion_tokens,
        ));
    }

    // Also try with vendor prefix for OpenRouter-style IDs
    let prefixed = format!("{normalized_provider}/{normalized_model}");
    if let Some(price) = lookup_dynamic(&prefixed) {
        return Some(price_estimate_from_price(
            &normalized_provider,
            &normalized_model,
            price,
            PriceSource::DynamicOpenRouter,
            prompt_tokens,
            completion_tokens,
        ));
    }

    // 2. Static fallback
    let pricing = lookup_static(&normalized_provider, &normalized_model).or_else(|| {
        if normalized_provider == "openrouter" {
            lookup_static_any_provider(&normalized_model)
        } else {
            None
        }
    });

    let pricing = pricing?;

    Some(PriceEstimate {
        provider: pricing.provider.to_string(),
        model: pricing.canonical_model.to_string(),
        source: PriceSource::StaticFallback,
        input_micros_per_million: pricing.input_micros_per_million,
        output_micros_per_million: pricing.output_micros_per_million,
        estimated_cost_micros: scale_cost(prompt_tokens, pricing.input_micros_per_million)
            + scale_cost(completion_tokens, pricing.output_micros_per_million),
    })
}

pub fn choose_openrouter_price_route(
    mode: PriceArbitrageMode,
    requested_model: &str,
    allowed_models: Option<&[String]>,
    prompt_tokens: u32,
    completion_tokens: u32,
) -> PriceArbitrageDecision {
    let normalized_requested = normalize_model(requested_model);
    if mode == PriceArbitrageMode::Disabled {
        return PriceArbitrageDecision {
            mode,
            requested_model: requested_model.to_string(),
            selected_model: requested_model.to_string(),
            price_source: None,
            baseline_estimated_cost_micros: 0,
            selected_estimated_cost_micros: 0,
            estimated_savings_micros: 0,
            reason: "disabled".to_string(),
        };
    }

    let baseline = estimate_price("openrouter", requested_model, prompt_tokens, completion_tokens);
    let baseline_cost = baseline.as_ref().map(|estimate| estimate.estimated_cost_micros).unwrap_or_default();
    let mut candidates = match mode {
        PriceArbitrageMode::Disabled => Vec::new(),
        PriceArbitrageMode::SameModelCheapest => dynamic_route_candidates(requested_model, prompt_tokens, completion_tokens),
        PriceArbitrageMode::AllowedModelsCheapest => {
            openrouter_price_route_candidates(requested_model, allowed_models, prompt_tokens, completion_tokens)
        }
    };

    if candidates.is_empty()
        && let Some(estimate) = baseline
    {
        candidates.push(PriceRouteCandidate {
            provider: "openrouter".to_string(),
            model: requested_model.to_string(),
            source: estimate.source,
            input_micros_per_million: estimate.input_micros_per_million,
            output_micros_per_million: estimate.output_micros_per_million,
            estimated_cost_micros: estimate.estimated_cost_micros,
        });
    }

    candidates.retain(|candidate| candidate.estimated_cost_micros > 0);
    candidates
        .sort_by(|left, right| left.estimated_cost_micros.cmp(&right.estimated_cost_micros).then_with(|| left.model.cmp(&right.model)));
    candidates.dedup_by(|left, right| left.model == right.model);

    let Some(selected) = candidates.first() else {
        return PriceArbitrageDecision {
            mode,
            requested_model: requested_model.to_string(),
            selected_model: requested_model.to_string(),
            price_source: None,
            baseline_estimated_cost_micros: baseline_cost,
            selected_estimated_cost_micros: baseline_cost,
            estimated_savings_micros: 0,
            reason: "no_price_available".to_string(),
        };
    };

    let savings = baseline_cost.saturating_sub(selected.estimated_cost_micros);
    let reason = if selected.model == requested_model || normalize_model(&selected.model) == normalized_requested {
        if savings > 0 {
            "same_model_cheaper_provider"
        } else {
            "no_cheaper_route"
        }
    } else if savings > 0 {
        "allowed_model_cheaper"
    } else {
        "allowed_model_selected_without_savings"
    };

    PriceArbitrageDecision {
        mode,
        requested_model: requested_model.to_string(),
        selected_model: selected.model.clone(),
        price_source: Some(selected.source),
        baseline_estimated_cost_micros: baseline_cost,
        selected_estimated_cost_micros: selected.estimated_cost_micros,
        estimated_savings_micros: savings,
        reason: reason.to_string(),
    }
}

/// Return OpenRouter route candidates for the requested model plus any allowed alternates.
///
/// Candidates include dynamic OpenRouter prices when available and static fallback
/// estimates for known OpenAI/Anthropic models. The list is sorted by estimated
/// cost, then model id, and is safe to feed into higher-level routing objectives.
pub fn openrouter_price_route_candidates(
    requested_model: &str,
    allowed_models: Option<&[String]>,
    prompt_tokens: u32,
    completion_tokens: u32,
) -> Vec<PriceRouteCandidate> {
    let normalized_requested = normalize_model(requested_model);
    let mut routes = Vec::new();
    let mut models = allowed_models
        .map(|models| models.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap_or_else(|| vec![requested_model]);
    if !models.iter().any(|model| normalize_model(model) == normalized_requested) {
        models.push(requested_model);
    }

    for model in models {
        routes.extend(dynamic_route_candidates(model, prompt_tokens, completion_tokens));
        if let Some(static_estimate) = estimate_price("openrouter", model, prompt_tokens, completion_tokens) {
            routes.push(PriceRouteCandidate {
                provider: "openrouter".to_string(),
                model: model.to_string(),
                source: static_estimate.source,
                input_micros_per_million: static_estimate.input_micros_per_million,
                output_micros_per_million: static_estimate.output_micros_per_million,
                estimated_cost_micros: static_estimate.estimated_cost_micros,
            });
        }
    }

    routes.retain(|candidate| candidate.estimated_cost_micros > 0);
    routes.sort_by(|left, right| left.estimated_cost_micros.cmp(&right.estimated_cost_micros).then_with(|| left.model.cmp(&right.model)));
    routes.dedup_by(|left, right| left.model == right.model);
    routes
}

fn price_estimate_from_price(
    provider: &str,
    model: &str,
    price: TokenPrice,
    source: PriceSource,
    prompt_tokens: u32,
    completion_tokens: u32,
) -> PriceEstimate {
    PriceEstimate {
        provider: provider.to_string(),
        model: model.to_string(),
        source,
        input_micros_per_million: price.input_micros_per_million,
        output_micros_per_million: price.output_micros_per_million,
        estimated_cost_micros: scale_cost(prompt_tokens, price.input_micros_per_million)
            + scale_cost(completion_tokens, price.output_micros_per_million),
    }
}

fn route_candidate_from_price(
    provider: &str,
    model: &str,
    price: TokenPrice,
    source: PriceSource,
    prompt_tokens: u32,
    completion_tokens: u32,
) -> PriceRouteCandidate {
    PriceRouteCandidate {
        provider: provider.to_string(),
        model: model.to_string(),
        source,
        input_micros_per_million: price.input_micros_per_million,
        output_micros_per_million: price.output_micros_per_million,
        estimated_cost_micros: scale_cost(prompt_tokens, price.input_micros_per_million)
            + scale_cost(completion_tokens, price.output_micros_per_million),
    }
}

fn normalize_provider(provider: &str) -> String {
    provider.trim().to_ascii_lowercase()
}

fn normalize_model(model: &str) -> String {
    let normalized = model.trim().to_ascii_lowercase();
    if let Some((_, suffix)) = normalized.split_once('/') {
        return suffix.to_string();
    }
    normalized
}

fn split_openrouter_model_id(model_id: &str) -> (String, String) {
    let normalized = model_id.trim().to_ascii_lowercase();
    if let Some((provider, model)) = normalized.split_once('/') {
        return (provider.to_string(), model.to_string());
    }
    ("openrouter".to_string(), normalized)
}

fn lookup_static(provider: &str, model: &str) -> Option<&'static ModelPricing> {
    best_static_match(
        OPENAI_MODELS.iter().chain(ANTHROPIC_MODELS.iter()).filter(|pricing| pricing.provider == provider),
        model,
    )
}

fn lookup_static_any_provider(model: &str) -> Option<&'static ModelPricing> {
    best_static_match(OPENAI_MODELS.iter().chain(ANTHROPIC_MODELS.iter()), model)
}

fn best_static_match<I>(candidates: I, model: &str) -> Option<&'static ModelPricing>
where
    I: Iterator<Item = &'static ModelPricing>,
{
    let mut best = None;
    for pricing in candidates {
        if let Some(score) = model_match_score(pricing, model) {
            match best {
                Some((best_score, _)) if score <= best_score => {}
                _ => best = Some((score, pricing)),
            }
        }
    }
    best.map(|(_, pricing)| pricing)
}

fn model_match_score(pricing: &ModelPricing, model: &str) -> Option<usize> {
    let mut best = None;
    if pricing.canonical_model == model || model.starts_with(pricing.canonical_model) {
        best = Some(pricing.canonical_model.len());
    }
    for alias in pricing.aliases {
        if *alias == model || model.starts_with(alias) {
            let score = alias.len();
            if best.is_none_or(|best_score| score > best_score) {
                best = Some(score);
            }
        }
    }
    best
}

fn scale_cost(tokens: u32, micros_per_million: u64) -> u64 {
    let scaled = (tokens as u128) * (micros_per_million as u128);
    ((scaled + 500_000) / 1_000_000) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_openai_chat_costs() {
        let cost = estimate_cost_micros("openai", "gpt-4o", 1_000, 2_000);
        assert_eq!(cost, 22_500);
    }

    #[test]
    fn estimates_gpt5_costs() {
        let cost = estimate_cost_micros("openai", "gpt-5.4", 1_000, 1_000);
        // 1K * 2.5M / 1M + 1K * 15M / 1M = 2500 + 15000 = 17500
        assert_eq!(cost, 17_500);
    }

    #[test]
    fn estimates_claude_opus_4_6() {
        let cost = estimate_cost_micros("anthropic", "claude-opus-4.6", 1_000, 1_000);
        // 1K * 5M / 1M + 1K * 25M / 1M = 5000 + 25000 = 30000
        assert_eq!(cost, 30_000);
    }

    #[test]
    fn estimates_embedding_costs_without_output_charge() {
        let cost = estimate_cost_micros("openai", "text-embedding-3-small", 500_000, 99_999);
        assert_eq!(cost, 10_000);
    }

    #[test]
    fn resolves_snapshot_aliases() {
        let cost = estimate_cost_micros("anthropic", "claude-sonnet-4-20250514", 1_000, 1_000);
        assert_eq!(cost, 18_000);
    }

    #[test]
    fn strips_openrouter_vendor_prefixes() {
        let cost = estimate_cost_micros("openrouter", "openai/gpt-4.1-mini", 10_000, 10_000);
        assert_eq!(cost, 10_000);
    }

    #[test]
    fn prefers_specific_static_models_over_family_prefixes() {
        let cost = estimate_cost_micros("openai", "gpt-4.1-mini", 10_000, 10_000);
        assert_eq!(cost, 10_000);
    }

    #[test]
    fn estimates_include_price_source() {
        let estimate = estimate_price("openai", "gpt-4.1-mini", 10_000, 10_000).expect("static price should exist");
        assert_eq!(estimate.source, PriceSource::StaticFallback);
        assert_eq!(estimate.estimated_cost_micros, 10_000);
    }

    #[test]
    fn arbitrage_can_select_allowed_cheaper_model() {
        let allowed = vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string()];
        let decision = choose_openrouter_price_route(PriceArbitrageMode::AllowedModelsCheapest, "gpt-4.1", Some(&allowed), 10_000, 10_000);

        assert_eq!(decision.selected_model, "gpt-4.1-mini");
        assert_eq!(decision.estimated_savings_micros, 90_000);
        assert_eq!(decision.reason, "allowed_model_cheaper");
    }

    #[test]
    fn route_candidates_include_allowed_static_fallbacks() {
        let allowed = vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string()];
        let candidates = openrouter_price_route_candidates("gpt-4.1", Some(&allowed), 10_000, 10_000);

        assert!(candidates.iter().any(|candidate| candidate.model == "gpt-4.1"));
        assert!(candidates.iter().any(|candidate| candidate.model == "gpt-4.1-mini"));
        assert_eq!(candidates.first().map(|candidate| candidate.model.as_str()), Some("gpt-4.1-mini"));
    }

    #[test]
    fn returns_zero_for_unknown_models() {
        assert_eq!(estimate_cost_micros("openai", "mystery-model", 10_000, 10_000), 0);
    }

    #[test]
    fn returns_zero_for_ollama() {
        assert_eq!(estimate_cost_micros("ollama", "llama3", 100_000, 100_000), 0);
    }

    #[test]
    fn parse_per_token_conversion() {
        // OpenRouter returns "0.000003" meaning $0.000003 per token = $3 per 1M tokens = 3_000_000 microdollars per 1M tokens
        assert_eq!(parse_per_token_to_micros_per_million(Some("0.000003")), 3_000_000);
        // GPT-4o-mini input: "0.00000015" = $0.15 per 1M = 150_000 microdollars per 1M
        assert_eq!(parse_per_token_to_micros_per_million(Some("0.00000015")), 150_000);
        assert_eq!(parse_per_token_to_micros_per_million(None), 0);
        assert_eq!(parse_per_token_to_micros_per_million(Some("0")), 0);
    }
}
