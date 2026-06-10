use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eden_logger_internal::{LogAudience, log_debug, log_info, log_warn, trace_context};
use endpoint_core::llm_core::LlmGatewayControlPlaneSnapshot;
use tokio::task::JoinHandle;

use super::state::ProxyGatewayState;

const DEFAULT_GATEWAY_SNAPSHOT_PUBLISH_INTERVAL_SECS: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewaySnapshotPublisherConfig {
    path: PathBuf,
    interval: Duration,
}

impl GatewaySnapshotPublisherConfig {
    pub fn new(path: impl Into<PathBuf>, interval_secs: Option<u64>) -> Option<Self> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return None;
        }

        Some(Self {
            path,
            interval: Duration::from_secs(interval_secs.unwrap_or(DEFAULT_GATEWAY_SNAPSHOT_PUBLISH_INTERVAL_SECS).max(1)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

pub fn spawn_gateway_snapshot_publisher(proxy_state: Arc<ProxyGatewayState>, config: GatewaySnapshotPublisherConfig) -> JoinHandle<()> {
    let ctx = trace_context().with_feature("llm.gateway.control_plane.publisher");
    log_info!(
        ctx.clone(),
        "LLM gateway control-plane snapshot publisher enabled",
        audience = LogAudience::Internal,
        path = config.path().display().to_string(),
        interval_secs = config.interval().as_secs()
    );

    tokio::spawn(async move {
        let publisher = GatewaySnapshotPublisher { proxy_state, config };
        publisher.run().await;
    })
}

struct GatewaySnapshotPublisher {
    proxy_state: Arc<ProxyGatewayState>,
    config: GatewaySnapshotPublisherConfig,
}

impl GatewaySnapshotPublisher {
    async fn run(self) {
        let ctx = trace_context().with_feature("llm.gateway.control_plane.publisher");
        loop {
            let snapshot = self.proxy_state.control_plane_snapshot_all_orgs();
            match Self::write_snapshot_file(self.config.path(), &snapshot).await {
                Ok(_bytes_written) => {
                    log_debug!(
                        ctx.clone(),
                        "Published LLM gateway control-plane snapshot",
                        audience = LogAudience::Internal,
                        path = self.config.path().display().to_string(),
                        version = snapshot.version,
                        key_policies = snapshot.key_policies.len(),
                        route_stats = snapshot.route_stats.len(),
                        bytes_written = _bytes_written
                    );
                }
                Err(err) => {
                    log_warn!(
                        ctx.clone(),
                        "Failed to publish LLM gateway control-plane snapshot",
                        audience = LogAudience::Internal,
                        path = self.config.path().display().to_string(),
                        error = err.to_string()
                    );
                }
            }

            tokio::time::sleep(self.config.interval()).await;
        }
    }

    async fn write_snapshot_file(path: &Path, snapshot: &LlmGatewayControlPlaneSnapshot) -> std::io::Result<usize> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut bytes = serde_json::to_vec_pretty(snapshot).map_err(std::io::Error::other)?;
        bytes.push(b'\n');

        let temp_path = temp_snapshot_path(path);
        tokio::fs::write(&temp_path, &bytes).await?;
        if let Err(err) = tokio::fs::rename(&temp_path, path).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err);
        }

        Ok(bytes.len())
    }
}

fn temp_snapshot_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(|name| name.to_str()).unwrap_or("snapshot.json");
    let timestamp_nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_nanos()).unwrap_or_default();
    path.with_file_name(format!(".{file_name}.tmp-{}-{timestamp_nanos}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_core::llm_core::{LlmGatewayControlPlaneAuthMode, LlmGatewayPolicy, LlmGatewayRouteStats};

    #[tokio::test]
    async fn writes_snapshot_atomically_as_json() {
        let dir = std::env::temp_dir().join(format!("eden-gateway-snapshot-publisher-{}-{}", std::process::id(), uuid::Uuid::new_v4()));
        let path = dir.join("snapshot.json");
        let snapshot = LlmGatewayControlPlaneSnapshot {
            version: 42,
            auth_mode: LlmGatewayControlPlaneAuthMode::Enforce,
            default_policy: Some(LlmGatewayPolicy::default()),
            key_policies: Vec::new(),
            model_catalog: None,
            route_stats: vec![LlmGatewayRouteStats {
                provider: "openai".to_string(),
                model: "gpt-4.1".to_string(),
                route_class: "default".to_string(),
                success_count: 1,
                error_count: 0,
                total_latency_ms: 100,
                min_latency_ms: 100,
                max_latency_ms: 100,
                total_output_tokens: 20,
                total_duration_ms: 100,
                first_observed_unix_ms: 1,
                last_observed_unix_ms: 2,
            }],
            updated_at_unix_ms: Some(123),
        };

        let bytes_written = GatewaySnapshotPublisher::write_snapshot_file(&path, &snapshot).await.expect("snapshot write should succeed");
        let contents = tokio::fs::read(&path).await.expect("snapshot file should be readable");
        assert_eq!(bytes_written, contents.len());
        let decoded: LlmGatewayControlPlaneSnapshot = serde_json::from_slice(&contents).expect("snapshot should decode");
        assert_eq!(decoded.version, 42);
        assert_eq!(decoded.route_stats.len(), 1);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn ignores_empty_publish_path() {
        assert!(GatewaySnapshotPublisherConfig::new("", Some(5)).is_none());
    }

    #[test]
    fn defaults_publish_interval() {
        let config = GatewaySnapshotPublisherConfig::new("/tmp/eden-gateway-snapshot.json", Some(0)).expect("config");
        assert_eq!(config.interval(), Duration::from_secs(1));

        let config = GatewaySnapshotPublisherConfig::new("/tmp/eden-gateway-snapshot.json", None).expect("config");
        assert_eq!(config.interval(), Duration::from_secs(DEFAULT_GATEWAY_SNAPSHOT_PUBLISH_INTERVAL_SECS));
    }
}
