use crate::api::lib::QueryInput;
use crate::output::ClickhouseRow;
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::{EpError, ResultEP};
use std::time::Duration;
use tokio::time::timeout;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub const CLICKHOUSE_VERSION_21: CapabilityId = CapabilityId("clickhouse.version.21");
pub const CLICKHOUSE_VERSION_22: CapabilityId = CapabilityId("clickhouse.version.22");
pub const CLICKHOUSE_VERSION_23: CapabilityId = CapabilityId("clickhouse.version.23");
pub const CLICKHOUSE_VERSION_24: CapabilityId = CapabilityId("clickhouse.version.24");

// Feature capabilities discovered at runtime.
pub const CLICKHOUSE_HAS_REPLICATION: CapabilityId = CapabilityId("clickhouse.has_replication");
pub const CLICKHOUSE_HAS_ZOOKEEPER: CapabilityId = CapabilityId("clickhouse.has_zookeeper");
pub const CLICKHOUSE_HAS_CLUSTERS: CapabilityId = CapabilityId("clickhouse.has_clusters");
pub const CLICKHOUSE_HAS_DICTIONARIES: CapabilityId = CapabilityId("clickhouse.has_dictionaries");

/// Discovered ClickHouse server capabilities used to gate collectors and queries.
#[derive(Debug, Clone)]
pub struct ClickhouseCapabilities {
    pub version_major: u32,
    pub version_minor: u32,
    pub version_full: String,
    pub has_replication: bool,
    pub has_zookeeper: bool,
    pub has_clusters: bool,
    pub has_dictionaries: bool,
}

impl ClickhouseCapabilities {
    /// Connect to the server and discover version, replication, keeper, cluster
    /// and dictionary capabilities. Errors are propagated so the caller can
    /// decide whether to fall back to `UnknownCapabilities`.
    pub async fn discover(context: ClickhouseAsync) -> ResultEP<Self> {
        let version_query = QueryInput::new("SELECT version() AS version".into(), Vec::new(), Vec::new());
        let version_result = timeout(DISCOVERY_TIMEOUT, version_query.run_query(context.clone()))
            .await
            .map_err(|_| EpError::metadata("Capability discovery timed out (version)"))?;

        let (version_major, version_minor, version_full) = match version_result {
            Ok(rows) => rows
                .first()
                .and_then(|row| row.get("version"))
                .and_then(|v| v.as_str())
                .map(|s| {
                    let (major, minor) = parse_clickhouse_version(s);
                    (major, minor, s.to_string())
                })
                .unwrap_or((0, 0, String::new())),
            Err(_) => (0, 0, String::new()),
        };

        let has_replication = probe_count(&context, "SELECT count() AS cnt FROM system.replicas").await;
        let has_zookeeper = probe_exists(&context, "SELECT count() AS cnt FROM system.zookeeper WHERE path = '/'").await;
        let has_clusters = probe_count(&context, "SELECT count() AS cnt FROM system.clusters").await;
        let has_dictionaries = probe_count(&context, "SELECT count() AS cnt FROM system.dictionaries").await;

        Ok(Self {
            version_major,
            version_minor,
            version_full,
            has_replication,
            has_zookeeper,
            has_clusters,
            has_dictionaries,
        })
    }
}

impl CapabilityChecker for ClickhouseCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            s if s.starts_with("clickhouse.version.") => s
                .strip_prefix("clickhouse.version.")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|required| self.version_major >= required),
            "clickhouse.has_replication" => self.has_replication,
            "clickhouse.has_zookeeper" => self.has_zookeeper,
            "clickhouse.has_clusters" => self.has_clusters,
            "clickhouse.has_dictionaries" => self.has_dictionaries,
            _ => false,
        }
    }
}

/// Parse a ClickHouse version string like `"23.8.2.7"` into `(major, minor)`.
///
/// ClickHouse versions use the format `major.minor.patch.build`.  Pre-release
/// suffixes (e.g. `"-testing"`) are stripped before parsing.
fn parse_clickhouse_version(version: &str) -> (u32, u32) {
    let parts: Vec<&str> = version.split('.').collect();
    let major = parts.first().and_then(|p| p.split('-').next()).and_then(|p| p.parse().ok()).unwrap_or(0);
    let minor = parts.get(1).and_then(|p| p.split('-').next()).and_then(|p| p.parse().ok()).unwrap_or(0);
    (major, minor)
}

/// Run a `SELECT count() AS cnt` probe and return `true` when cnt > 0.
/// Returns `false` on any error (timeout, missing table, permission denied etc.).
async fn probe_count(context: &ClickhouseAsync, sql: &str) -> bool {
    let q = QueryInput::new(sql.into(), Vec::new(), Vec::new());
    match timeout(DISCOVERY_TIMEOUT, q.run_query(context.clone())).await {
        Ok(Ok(rows)) => count_from_rows(&rows).is_some_and(|cnt| cnt > 0),
        _ => false,
    }
}

/// Run a probe query and return `true` when the query succeeds (regardless of
/// count).  Returns `false` on any error.  Used for probes where table
/// existence itself signals the capability (e.g. `system.zookeeper`).
async fn probe_exists(context: &ClickhouseAsync, sql: &str) -> bool {
    let q = QueryInput::new(sql.into(), Vec::new(), Vec::new());
    matches!(timeout(DISCOVERY_TIMEOUT, q.run_query(context.clone())).await, Ok(Ok(_)))
}

/// Extract the `cnt` column from the first row of a result set.
fn count_from_rows(rows: &[ClickhouseRow]) -> Option<u64> {
    rows.first().and_then(|row| row.get("cnt")).and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_standard() {
        assert_eq!(parse_clickhouse_version("23.8.2.7"), (23, 8));
        assert_eq!(parse_clickhouse_version("24.1.0.1"), (24, 1));
    }

    #[test]
    fn parse_version_short() {
        assert_eq!(parse_clickhouse_version("22.3"), (22, 3));
        assert_eq!(parse_clickhouse_version("21"), (21, 0));
    }

    #[test]
    fn parse_version_prerelease() {
        assert_eq!(parse_clickhouse_version("24.3.1.5-testing"), (24, 3));
        assert_eq!(parse_clickhouse_version("23.0-lts"), (23, 0));
    }

    #[test]
    fn parse_version_empty() {
        assert_eq!(parse_clickhouse_version(""), (0, 0));
    }

    #[test]
    fn capability_version_check() {
        let caps = ClickhouseCapabilities {
            version_major: 23,
            version_minor: 8,
            version_full: "23.8.2.7".to_string(),
            has_replication: true,
            has_zookeeper: true,
            has_clusters: false,
            has_dictionaries: false,
        };

        assert!(caps.has(&CLICKHOUSE_VERSION_21));
        assert!(caps.has(&CLICKHOUSE_VERSION_22));
        assert!(caps.has(&CLICKHOUSE_VERSION_23));
        assert!(!caps.has(&CLICKHOUSE_VERSION_24));
    }

    #[test]
    fn capability_feature_checks() {
        let caps = ClickhouseCapabilities {
            version_major: 24,
            version_minor: 1,
            version_full: "24.1.0.1".to_string(),
            has_replication: true,
            has_zookeeper: false,
            has_clusters: true,
            has_dictionaries: false,
        };

        assert!(caps.has(&CLICKHOUSE_HAS_REPLICATION));
        assert!(!caps.has(&CLICKHOUSE_HAS_ZOOKEEPER));
        assert!(caps.has(&CLICKHOUSE_HAS_CLUSTERS));
        assert!(!caps.has(&CLICKHOUSE_HAS_DICTIONARIES));
    }

    #[test]
    fn capability_unknown_id_returns_false() {
        let caps = ClickhouseCapabilities {
            version_major: 24,
            version_minor: 1,
            version_full: "24.1.0.1".to_string(),
            has_replication: true,
            has_zookeeper: true,
            has_clusters: true,
            has_dictionaries: true,
        };

        assert!(!caps.has(&CapabilityId("unknown.capability")));
    }

    #[test]
    fn count_from_rows_numeric() {
        let row = ClickhouseRow::from(vec![("cnt".to_string(), serde_json::json!(5))]);
        assert_eq!(count_from_rows(&[row]), Some(5));
    }

    #[test]
    fn count_from_rows_string() {
        let row = ClickhouseRow::from(vec![("cnt".to_string(), serde_json::json!("42"))]);
        assert_eq!(count_from_rows(&[row]), Some(42));
    }

    #[test]
    fn count_from_rows_empty() {
        assert_eq!(count_from_rows(&[]), None);
    }
}
