use super::HttpRequest;
use super::auth::LlmGatewayAuthPolicy;
use super::features::LlmGatewayFeatureEngine;
use endpoint_core::llm_core::{
    LlmGatewayControlPlaneSnapshot, LlmGatewayCredential, LlmGatewayModelCatalog, hydrate_llm_gateway_route_stats,
};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime};

const SNAPSHOT_JSON_ENV: &str = "EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_JSON";
const SNAPSHOT_PATH_ENV: &str = "EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PATH";
const SNAPSHOT_RELOAD_SECS_ENV: &str = "EDEN_LLM_GATEWAY_CONTROL_PLANE_RELOAD_SECS";
const DEFAULT_SNAPSHOT_RELOAD_SECS: u64 = 5;

pub(super) struct LlmGatewayResolvedControlPlane {
    pub(super) auth_policy: LlmGatewayAuthPolicy,
    pub(super) feature_engine: LlmGatewayFeatureEngine,
    pub(super) model_catalog: Arc<LlmGatewayModelCatalog>,
    pub(super) source: &'static str,
}

pub(super) struct LlmGatewayControlPlane {
    source: LlmGatewayControlPlaneSource,
    cache: RwLock<LlmGatewayControlPlaneCache>,
}

impl LlmGatewayControlPlane {
    pub(super) fn global() -> &'static Self {
        static CONTROL_PLANE: OnceLock<LlmGatewayControlPlane> = OnceLock::new();
        CONTROL_PLANE.get_or_init(Self::from_env)
    }

    fn from_env() -> Self {
        Self {
            source: LlmGatewayControlPlaneSource::from_env(),
            cache: RwLock::new(LlmGatewayControlPlaneCache::default()),
        }
    }

    pub(super) fn resolve(&self, request: &HttpRequest) -> LlmGatewayResolvedControlPlane {
        let Some(snapshot) = self.snapshot() else {
            return LlmGatewayResolvedControlPlane {
                auth_policy: LlmGatewayAuthPolicy::from_env(),
                feature_engine: LlmGatewayFeatureEngine::from_env(),
                model_catalog: builtin_model_catalog(),
                source: "env",
            };
        };

        let key_hash = Self::request_key_hash(request);
        let key_policy = key_hash
            .as_deref()
            .and_then(|key_hash| snapshot.key_policies.iter().find(|policy| policy.enabled && policy.key_hash == key_hash))
            .map(|key_policy| &key_policy.policy);
        let policy = key_policy.or(snapshot.default_policy.as_ref());
        let feature_engine = policy.map(LlmGatewayFeatureEngine::from_gateway_policy).unwrap_or_else(LlmGatewayFeatureEngine::from_env);
        let source = if key_policy.is_some() {
            "control_plane_key"
        } else if policy.is_some() {
            "control_plane_default"
        } else {
            "control_plane_auth_only"
        };

        LlmGatewayResolvedControlPlane {
            auth_policy: LlmGatewayAuthPolicy::from_control_plane(snapshot.auth_mode, snapshot.enabled_key_hashes()),
            feature_engine,
            model_catalog: snapshot.model_catalog.as_ref().map(|catalog| Arc::new(catalog.clone())).unwrap_or_else(builtin_model_catalog),
            source,
        }
    }

    fn request_key_hash(request: &HttpRequest) -> Option<String> {
        let api_key = LlmGatewayCredential::api_key_from_parts(
            request.header("authorization"),
            request.header("x-api-key"),
            request.header("api-key"),
        )?;
        Some(LlmGatewayCredential::hash_api_key(api_key))
    }

    fn snapshot(&self) -> Option<Arc<LlmGatewayControlPlaneSnapshot>> {
        match &self.source {
            LlmGatewayControlPlaneSource::Disabled => None,
            LlmGatewayControlPlaneSource::Inline { snapshot } => Some(Arc::clone(snapshot)),
            LlmGatewayControlPlaneSource::File { path, reload_interval } => self.file_snapshot(path, *reload_interval),
        }
    }

    fn file_snapshot(&self, path: &PathBuf, reload_interval: Duration) -> Option<Arc<LlmGatewayControlPlaneSnapshot>> {
        if let Ok(cache) = self.cache.read()
            && !cache.should_reload(reload_interval)
        {
            return cache.snapshot.clone();
        }

        let Ok(mut cache) = self.cache.write() else {
            return None;
        };
        if !cache.should_reload(reload_interval) {
            return cache.snapshot.clone();
        }

        cache.last_checked = Some(Instant::now());
        let Ok(metadata) = std::fs::metadata(path) else {
            return cache.snapshot.clone();
        };
        let modified = metadata.modified().ok();
        if cache.snapshot.is_some() && modified.is_some() && modified == cache.last_modified {
            return cache.snapshot.clone();
        }

        let Ok(raw) = std::fs::read_to_string(path) else {
            return cache.snapshot.clone();
        };
        let Some(snapshot) = parse_snapshot(raw.as_str()) else {
            return cache.snapshot.clone();
        };
        cache.last_modified = modified;
        cache.snapshot = Some(Arc::clone(&snapshot));
        Some(snapshot)
    }
}

enum LlmGatewayControlPlaneSource {
    Disabled,
    Inline { snapshot: Arc<LlmGatewayControlPlaneSnapshot> },
    File { path: PathBuf, reload_interval: Duration },
}

impl LlmGatewayControlPlaneSource {
    fn from_env() -> Self {
        if let Ok(raw) = std::env::var(SNAPSHOT_JSON_ENV)
            && let Some(snapshot) = parse_snapshot(raw.as_str())
        {
            return Self::Inline { snapshot };
        }

        if let Ok(path) = std::env::var(SNAPSHOT_PATH_ENV) {
            let path = PathBuf::from(path.trim());
            if !path.as_os_str().is_empty() {
                return Self::File {
                    path,
                    reload_interval: Duration::from_secs(env_u64(SNAPSHOT_RELOAD_SECS_ENV, DEFAULT_SNAPSHOT_RELOAD_SECS).max(1)),
                };
            }
        }

        Self::Disabled
    }
}

#[derive(Default)]
struct LlmGatewayControlPlaneCache {
    snapshot: Option<Arc<LlmGatewayControlPlaneSnapshot>>,
    last_checked: Option<Instant>,
    last_modified: Option<SystemTime>,
}

impl LlmGatewayControlPlaneCache {
    fn should_reload(&self, reload_interval: Duration) -> bool {
        self.last_checked.is_none_or(|last_checked| last_checked.elapsed() >= reload_interval)
    }
}

fn parse_snapshot(raw: &str) -> Option<Arc<LlmGatewayControlPlaneSnapshot>> {
    let snapshot = serde_json::from_str::<LlmGatewayControlPlaneSnapshot>(raw).ok()?.normalized();
    hydrate_llm_gateway_route_stats(snapshot.route_stats.clone());
    Some(Arc::new(snapshot))
}

fn builtin_model_catalog() -> Arc<LlmGatewayModelCatalog> {
    static CATALOG: OnceLock<Arc<LlmGatewayModelCatalog>> = OnceLock::new();
    Arc::clone(CATALOG.get_or_init(|| Arc::new(LlmGatewayModelCatalog::builtin())))
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|value| value.trim().parse::<u64>().ok()).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_model_catalog_is_cached() {
        let first = builtin_model_catalog();
        let second = builtin_model_catalog();

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!first.entries().is_empty());
    }
}
