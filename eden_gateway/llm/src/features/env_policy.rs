use super::{LlmDetectionMode, LlmFeatureMode, LlmPiiPolicyMode};
use endpoint_core::llm_core::{LlmKvCacheMode, LlmRouteOptimizationMode, PriceArbitrageMode};
use std::collections::BTreeSet;
use std::env;

pub(super) struct LlmEnvPolicy;

impl LlmEnvPolicy {
    pub(super) fn csv_set(name: &str) -> Option<BTreeSet<String>> {
        let raw = env::var(name).ok()?;
        let values = raw.split(',').map(str::trim).filter(|value| !value.is_empty()).map(str::to_string).collect::<BTreeSet<_>>();

        (!values.is_empty()).then_some(values)
    }

    pub(super) fn u32(name: &str) -> Option<u32> {
        env::var(name).ok().and_then(|value| value.trim().parse::<u32>().ok())
    }

    pub(super) fn pii_mode(name: &str, default: LlmPiiPolicyMode, legacy_block_bool: Option<&str>) -> LlmPiiPolicyMode {
        if legacy_block_bool.is_some_and(Self::bool_enabled) {
            return LlmPiiPolicyMode::Block;
        }

        match env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("block") => LlmPiiPolicyMode::Block,
            Some("redact") => LlmPiiPolicyMode::Redact,
            Some("detect") => LlmPiiPolicyMode::Detect,
            Some("disabled" | "off" | "false") => LlmPiiPolicyMode::Disabled,
            _ => default,
        }
    }

    pub(super) fn detection_mode(name: &str, default: LlmDetectionMode) -> LlmDetectionMode {
        match env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("block") => LlmDetectionMode::Block,
            Some("detect") => LlmDetectionMode::Detect,
            Some("disabled" | "off" | "false") => LlmDetectionMode::Disabled,
            _ => default,
        }
    }

    pub(super) fn feature_mode(name: &str, default: LlmFeatureMode) -> LlmFeatureMode {
        match env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("observe") | Some("enabled") | Some("true") => LlmFeatureMode::Observe,
            Some("disabled") | Some("off") | Some("false") => LlmFeatureMode::Disabled,
            _ => default,
        }
    }

    pub(super) fn price_arbitrage_mode(name: &str, default: PriceArbitrageMode) -> PriceArbitrageMode {
        match env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("same_model_cheapest" | "same_model") => PriceArbitrageMode::SameModelCheapest,
            Some("allowed_models_cheapest" | "allowed_models" | "cheapest") => PriceArbitrageMode::AllowedModelsCheapest,
            Some("disabled" | "off" | "false") => PriceArbitrageMode::Disabled,
            _ => default,
        }
    }

    pub(super) fn route_optimization_mode(name: &str, default: LlmRouteOptimizationMode) -> LlmRouteOptimizationMode {
        match env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("latency") => LlmRouteOptimizationMode::Latency,
            Some("throughput") => LlmRouteOptimizationMode::Throughput,
            Some("balanced") => LlmRouteOptimizationMode::Balanced,
            Some("cost") => LlmRouteOptimizationMode::Cost,
            _ => default,
        }
    }

    pub(super) fn kv_cache_mode(name: &str, default: LlmKvCacheMode) -> LlmKvCacheMode {
        match env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("affinity") => LlmKvCacheMode::Affinity,
            Some("adaptive") => LlmKvCacheMode::Adaptive,
            Some("disabled" | "off" | "false") => LlmKvCacheMode::Disabled,
            _ => default,
        }
    }

    fn bool_enabled(name: &str) -> bool {
        matches!(
            env::var(name).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref(),
            Some("1" | "true" | "yes" | "on")
        )
    }
}
