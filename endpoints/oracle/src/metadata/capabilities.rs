use crate::metadata::stc::utils::{query, run_query_with_timeout};
use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::ResultEP;
use log::warn;
use oracle_core::OracleAsync;
use std::time::Duration;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

// I love Oracle's versioning numbering system 😻
pub const ORACLE_VERSION_12: CapabilityId = CapabilityId("oracle.version.12");
pub const ORACLE_VERSION_18: CapabilityId = CapabilityId("oracle.version.18");
pub const ORACLE_VERSION_19: CapabilityId = CapabilityId("oracle.version.19");
pub const ORACLE_VERSION_21: CapabilityId = CapabilityId("oracle.version.21");
pub const ORACLE_VERSION_23: CapabilityId = CapabilityId("oracle.version.23");

// Edition capabilities.
pub const ORACLE_EDITION_ENTERPRISE: CapabilityId = CapabilityId("oracle.edition.enterprise");

// Multitenant / CDB architecture (12c+).
pub const ORACLE_CDB: CapabilityId = CapabilityId("oracle.cdb");

// Diagnostics Pack licensed (needed for V$ACTIVE_SESSION_HISTORY, DBA_HIST_* views).
pub const ORACLE_DIAG_PACK: CapabilityId = CapabilityId("oracle.diag_pack");

// DBA_ prefixed views accessible (vs only ALL_ views).
pub const ORACLE_HAS_DBA_VIEWS: CapabilityId = CapabilityId("oracle.has_dba_views");

/// Discovered Oracle instance capabilities used to gate collectors and queries.
#[derive(Debug, Clone)]
pub struct OracleCapabilities {
    pub version_major: u32,
    pub version_full: String,
    pub edition: String,
    pub is_cdb: bool,
    pub has_diagnostics_pack: bool,
    pub has_dba_views: bool,
}

impl OracleCapabilities {
    /// Connect to the instance and discover version, edition, CDB status,
    /// diagnostics pack availability and DBA view access.
    /// Errors are propagated so the caller can decide whether to fall back
    /// to `UnknownCapabilities`.
    pub async fn discover(context: OracleAsync) -> ResultEP<Self> {
        let (version_major, version_full, edition) = discover_version_and_edition(context.clone()).await;
        let is_cdb = discover_cdb_status(context.clone()).await;
        let has_diagnostics_pack = discover_diagnostics_pack(context.clone()).await;
        let has_dba_views = discover_dba_views(context).await;

        Ok(Self {
            version_major,
            version_full,
            edition,
            is_cdb,
            has_diagnostics_pack,
            has_dba_views,
        })
    }
}

impl CapabilityChecker for OracleCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            s if s.starts_with("oracle.version.") => s
                .strip_prefix("oracle.version.")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|required| self.version_major >= required),
            "oracle.edition.enterprise" => self.edition.to_uppercase().contains("ENTERPRISE"),
            "oracle.cdb" => self.is_cdb,
            "oracle.diag_pack" => self.has_diagnostics_pack,
            "oracle.has_dba_views" => self.has_dba_views,
            _ => false,
        }
    }
}

/// Discover the Oracle major version, full version string and edition.
///
/// Tries `version_full` first (available 18c+), then falls back to `version`.
async fn discover_version_and_edition(context: OracleAsync) -> (u32, String, String) {
    // Try version_full first (Oracle 18c+).
    let version_query = query("SELECT version_full, edition FROM v$instance");
    match run_query_with_timeout(&version_query, context.clone(), DISCOVERY_TIMEOUT, "capabilities.version_full").await {
        Ok(rows) if !rows.is_empty() => {
            let row = &rows[0];
            let version_full: String = row.get::<_, Option<String>>("VERSION_FULL").unwrap_or(None).unwrap_or_default();
            let edition: String = row.get::<_, Option<String>>("EDITION").unwrap_or(None).unwrap_or_default();
            let major = parse_oracle_major_version(&version_full);
            return (major, version_full, edition);
        }
        _ => {}
    }

    // Fallback: older Oracle versions only have `version`.
    let fallback_query = query("SELECT version, edition FROM v$instance");
    match run_query_with_timeout(&fallback_query, context, DISCOVERY_TIMEOUT, "capabilities.version").await {
        Ok(rows) if !rows.is_empty() => {
            let row = &rows[0];
            let version: String = row.get::<_, Option<String>>("VERSION").unwrap_or(None).unwrap_or_default();
            let edition: String = row.get::<_, Option<String>>("EDITION").unwrap_or(None).unwrap_or_default();
            let major = parse_oracle_major_version(&version);
            (major, version, edition)
        }
        Ok(_) => {
            warn!("capabilities: v$instance returned no rows");
            (0, String::new(), String::new())
        }
        Err(e) => {
            warn!("capabilities: failed to query v$instance: {e}");
            (0, String::new(), String::new())
        }
    }
}

/// Discover whether the database is a Container Database (CDB).
async fn discover_cdb_status(context: OracleAsync) -> bool {
    let cdb_query = query("SELECT cdb FROM v$database");
    match run_query_with_timeout(&cdb_query, context, DISCOVERY_TIMEOUT, "capabilities.cdb").await {
        Ok(rows) if !rows.is_empty() => {
            let value: String = rows[0].get::<_, Option<String>>("CDB").unwrap_or(None).unwrap_or_default();
            value.eq_ignore_ascii_case("YES")
        }
        Ok(_) => false,
        Err(e) => {
            warn!("capabilities: failed to query v$database for CDB status: {e}");
            false
        }
    }
}

/// Discover whether the Diagnostics Pack is licensed.
async fn discover_diagnostics_pack(context: OracleAsync) -> bool {
    let diag_query = query("SELECT value FROM v$parameter WHERE name = 'control_management_pack_access'");
    match run_query_with_timeout(&diag_query, context, DISCOVERY_TIMEOUT, "capabilities.diag_pack").await {
        Ok(rows) if !rows.is_empty() => {
            let value: String = rows[0].get::<_, Option<String>>("VALUE").unwrap_or(None).unwrap_or_default().to_uppercase();
            value.contains("DIAGNOSTIC")
        }
        Ok(_) => false,
        Err(e) => {
            warn!("capabilities: failed to query diagnostics pack parameter: {e}");
            false
        }
    }
}

/// Probe whether DBA_ views are accessible.
async fn discover_dba_views(context: OracleAsync) -> bool {
    let probe_query = query("SELECT 1 AS ok FROM dba_tables WHERE ROWNUM = 1");
    match run_query_with_timeout(&probe_query, context, DISCOVERY_TIMEOUT, "capabilities.dba_views").await {
        Ok(_) => true,
        Err(e) => {
            warn!("capabilities: DBA views not accessible (will use ALL_ views): {e}");
            false
        }
    }
}

/// Parse the major version number from an Oracle version string.
///
/// Oracle version strings look like `"19.21.0.0.0"` or `"12.2.0.1.0"`.
fn parse_oracle_major_version(version: &str) -> u32 {
    version.split('.').next().and_then(|p| p.parse().ok()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_major_version_standard() {
        assert_eq!(parse_oracle_major_version("19.21.0.0.0"), 19);
        assert_eq!(parse_oracle_major_version("12.2.0.1.0"), 12);
        assert_eq!(parse_oracle_major_version("21.3.0.0.0"), 21);
        assert_eq!(parse_oracle_major_version("23.4.0.24.05"), 23);
    }

    #[test]
    fn parse_major_version_short() {
        assert_eq!(parse_oracle_major_version("19"), 19);
        assert_eq!(parse_oracle_major_version("19.3"), 19);
    }

    #[test]
    fn parse_major_version_empty() {
        assert_eq!(parse_oracle_major_version(""), 0);
    }

    #[test]
    fn parse_major_version_invalid() {
        assert_eq!(parse_oracle_major_version("abc"), 0);
        assert_eq!(parse_oracle_major_version("abc.19.0"), 0);
    }

    #[test]
    fn capability_version_check() {
        let caps = OracleCapabilities {
            version_major: 19,
            version_full: "19.21.0.0.0".to_string(),
            edition: "Enterprise Edition".to_string(),
            is_cdb: true,
            has_diagnostics_pack: true,
            has_dba_views: true,
        };

        assert!(caps.has(&ORACLE_VERSION_12));
        assert!(caps.has(&ORACLE_VERSION_18));
        assert!(caps.has(&ORACLE_VERSION_19));
        assert!(!caps.has(&ORACLE_VERSION_21));
        assert!(!caps.has(&ORACLE_VERSION_23));
        assert!(caps.has(&ORACLE_EDITION_ENTERPRISE));
        assert!(caps.has(&ORACLE_CDB));
        assert!(caps.has(&ORACLE_DIAG_PACK));
        assert!(caps.has(&ORACLE_HAS_DBA_VIEWS));
    }

    #[test]
    fn capability_version_check_old() {
        let caps = OracleCapabilities {
            version_major: 12,
            version_full: "12.2.0.1.0".to_string(),
            edition: "Standard Edition 2".to_string(),
            is_cdb: false,
            has_diagnostics_pack: false,
            has_dba_views: false,
        };

        assert!(caps.has(&ORACLE_VERSION_12));
        assert!(!caps.has(&ORACLE_VERSION_18));
        assert!(!caps.has(&ORACLE_VERSION_19));
        assert!(!caps.has(&ORACLE_EDITION_ENTERPRISE));
        assert!(!caps.has(&ORACLE_CDB));
        assert!(!caps.has(&ORACLE_DIAG_PACK));
        assert!(!caps.has(&ORACLE_HAS_DBA_VIEWS));
    }

    #[test]
    fn capability_version_check_23ai() {
        let caps = OracleCapabilities {
            version_major: 23,
            version_full: "23.4.0.24.05".to_string(),
            edition: "Enterprise Edition".to_string(),
            is_cdb: true,
            has_diagnostics_pack: true,
            has_dba_views: true,
        };

        assert!(caps.has(&ORACLE_VERSION_12));
        assert!(caps.has(&ORACLE_VERSION_18));
        assert!(caps.has(&ORACLE_VERSION_19));
        assert!(caps.has(&ORACLE_VERSION_21));
        assert!(caps.has(&ORACLE_VERSION_23));
    }

    #[test]
    fn capability_unknown_id_returns_false() {
        let caps = OracleCapabilities {
            version_major: 19,
            version_full: "19.21.0.0.0".to_string(),
            edition: "Enterprise Edition".to_string(),
            is_cdb: true,
            has_diagnostics_pack: true,
            has_dba_views: true,
        };

        assert!(!caps.has(&CapabilityId("oracle.nonexistent")));
        assert!(!caps.has(&CapabilityId("cassandra.version.4")));
    }

    #[test]
    fn capability_edition_case_insensitive() {
        let caps = OracleCapabilities {
            version_major: 19,
            version_full: "19.0.0.0.0".to_string(),
            edition: "enterprise edition".to_string(),
            is_cdb: false,
            has_diagnostics_pack: false,
            has_dba_views: false,
        };

        assert!(caps.has(&ORACLE_EDITION_ENTERPRISE));
    }
}
