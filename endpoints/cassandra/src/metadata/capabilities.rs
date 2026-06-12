use crate::api::lib::QueryUnpagedInput;
use crate::output::CassandraQueryOutput;
use cassandra_core::CassandraAsync;
use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use ep_core::ToOutput;
use error::{EpError, ResultEP};
use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub const CASSANDRA_VERSION_3: CapabilityId = CapabilityId("cassandra.version.3");
pub const CASSANDRA_VERSION_4: CapabilityId = CapabilityId("cassandra.version.4");
pub const CASSANDRA_VERSION_5: CapabilityId = CapabilityId("cassandra.version.5");

// System table availability (varies across Apache Cassandra, DSE, ScyllaDB).
pub const CASSANDRA_HAS_COMPACTION_HISTORY: CapabilityId = CapabilityId("cassandra.table.compaction_history");
pub const CASSANDRA_HAS_SSTABLE_ACTIVITY: CapabilityId = CapabilityId("cassandra.table.sstable_activity");
pub const CASSANDRA_HAS_SNAPSHOTS_TABLE: CapabilityId = CapabilityId("cassandra.table.snapshots");
pub const CASSANDRA_HAS_SIZE_ESTIMATES: CapabilityId = CapabilityId("cassandra.table.size_estimates");

// Cassandra 4.0+ virtual tables (`system_views` keyspace).
pub const CASSANDRA_HAS_VIRTUAL_TABLES: CapabilityId = CapabilityId("cassandra.virtual_tables");

/// Discovered Cassandra cluster capabilities used to gate collectors and queries.
#[derive(Debug, Clone)]
pub struct CassandraCapabilities {
    pub version_major: u32,
    pub version_minor: u32,
    pub version_patch: u32,
    pub cluster_name: String,
    pub partitioner: String,
    /// Names of tables present in the `system` keyspace.
    pub available_system_tables: Vec<String>,
    /// Whether the `system_views` keyspace exists (Cassandra 4.0+).
    pub has_virtual_tables: bool,
}

impl CassandraCapabilities {
    /// Connect to the local node and discover version, topology and available
    /// system tables. Errors are propagated so the caller can decide whether to
    /// fall back to `PermissiveCapabilities`.
    pub async fn discover(context: CassandraAsync) -> ResultEP<Self> {
        let local_query = QueryUnpagedInput::new("SELECT release_version, cluster_name, partitioner FROM system.local".to_string());

        let local_result = timeout(DISCOVERY_TIMEOUT, local_query.run_query(context.clone()))
            .await
            .map_err(|_| EpError::metadata("Capability discovery timed out"))?;

        let local_data: Value = CassandraQueryOutput(local_result?).try_serde_serialize()?;

        let first_row = local_data.as_array().and_then(|rows| rows.first());

        let (version_major, version_minor, version_patch) = first_row
            .and_then(|row| row.get("release_version"))
            .and_then(|v| v.as_str())
            .map(parse_cassandra_version)
            .unwrap_or((0, 0, 0));

        let cluster_name = first_row.and_then(|row| row.get("cluster_name")).and_then(|v| v.as_str()).unwrap_or_default().to_string();

        let partitioner = first_row.and_then(|row| row.get("partitioner")).and_then(|v| v.as_str()).unwrap_or_default().to_string();

        // Discover available system tables (best-effort); if this fails
        // we proceed with an empty list and all table-presence caps will be false.
        let tables_query = QueryUnpagedInput::new("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system'".to_string());

        let available_system_tables = match timeout(DISCOVERY_TIMEOUT, tables_query.run_query(context.clone())).await {
            Ok(Ok(result)) => {
                let data: Value = CassandraQueryOutput(result).try_serde_serialize()?;
                match data {
                    Value::Array(rows) => {
                        rows.iter().filter_map(|row| row.get("table_name").and_then(|v| v.as_str()).map(|s| s.to_string())).collect()
                    }
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        };

        // Probe for Cassandra 4.0+ virtual tables by checking if the
        // `system_views` keyspace exists.
        let views_query =
            QueryUnpagedInput::new("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'system_views'".to_string());

        let has_virtual_tables = match timeout(DISCOVERY_TIMEOUT, views_query.run_query(context)).await {
            Ok(Ok(result)) => {
                let data: Value = CassandraQueryOutput(result).try_serde_serialize()?;
                data.as_array().is_some_and(|rows| !rows.is_empty())
            }
            _ => false,
        };

        Ok(Self {
            version_major,
            version_minor,
            version_patch,
            cluster_name,
            partitioner,
            available_system_tables,
            has_virtual_tables,
        })
    }

    fn has_system_table(&self, table_name: &str) -> bool {
        self.available_system_tables.iter().any(|t| t == table_name)
    }
}

impl CapabilityChecker for CassandraCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            s if s.starts_with("cassandra.version.") => s
                .strip_prefix("cassandra.version.")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|required| self.version_major >= required),
            "cassandra.table.compaction_history" => self.has_system_table("compaction_history"),
            "cassandra.table.sstable_activity" => self.has_system_table("sstable_activity"),
            "cassandra.table.snapshots" => self.has_system_table("snapshots"),
            "cassandra.table.size_estimates" => self.has_system_table("size_estimates"),
            "cassandra.virtual_tables" => self.has_virtual_tables,
            _ => false,
        }
    }
}

/// Parse a Cassandra version string like `"4.1.3"` or `"5.0-beta1"`.
fn parse_cassandra_version(version: &str) -> (u32, u32, u32) {
    let parts: Vec<&str> = version.split('.').collect();
    let major = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
    let minor = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    let patch = parts.get(2).and_then(|p| p.split('-').next()).and_then(|p| p.parse().ok()).unwrap_or(0);
    (major, minor, patch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_standard() {
        assert_eq!(parse_cassandra_version("4.1.3"), (4, 1, 3));
        assert_eq!(parse_cassandra_version("3.11.15"), (3, 11, 15));
    }

    #[test]
    fn parse_version_prerelease() {
        assert_eq!(parse_cassandra_version("5.0-beta1"), (5, 0, 0));
        assert_eq!(parse_cassandra_version("5.0.0-SNAPSHOT"), (5, 0, 0));
    }

    #[test]
    fn parse_version_empty() {
        assert_eq!(parse_cassandra_version(""), (0, 0, 0));
    }

    #[test]
    fn capability_version_check() {
        let caps = CassandraCapabilities {
            version_major: 4,
            version_minor: 1,
            version_patch: 3,
            cluster_name: "test".to_string(),
            partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
            available_system_tables: vec!["compaction_history".to_string(), "size_estimates".to_string()],
            has_virtual_tables: true,
        };

        assert!(caps.has(&CASSANDRA_VERSION_3));
        assert!(caps.has(&CASSANDRA_VERSION_4));
        assert!(!caps.has(&CASSANDRA_VERSION_5));
        assert!(caps.has(&CASSANDRA_HAS_COMPACTION_HISTORY));
        assert!(caps.has(&CASSANDRA_HAS_SIZE_ESTIMATES));
        assert!(!caps.has(&CASSANDRA_HAS_SSTABLE_ACTIVITY));
        assert!(!caps.has(&CASSANDRA_HAS_SNAPSHOTS_TABLE));
        assert!(caps.has(&CASSANDRA_HAS_VIRTUAL_TABLES));
    }

    #[test]
    fn capability_virtual_tables_v3() {
        let caps = CassandraCapabilities {
            version_major: 3,
            version_minor: 11,
            version_patch: 15,
            cluster_name: "legacy".to_string(),
            partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
            available_system_tables: vec!["compaction_history".to_string()],
            has_virtual_tables: false,
        };

        assert!(caps.has(&CASSANDRA_VERSION_3));
        assert!(!caps.has(&CASSANDRA_VERSION_4));
        assert!(!caps.has(&CASSANDRA_HAS_VIRTUAL_TABLES));
    }
}
