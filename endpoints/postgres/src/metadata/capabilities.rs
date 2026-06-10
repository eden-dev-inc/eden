use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::{EpError, ResultEP};
use postgres_core::{PostgresAsync, parse_simple_query_response};

pub const PG_VERSION_14: CapabilityId = CapabilityId("pg.version.14");
pub const PG_VERSION_15: CapabilityId = CapabilityId("pg.version.15");
pub const PG_VERSION_16: CapabilityId = CapabilityId("pg.version.16");
pub const PG_VERSION_17: CapabilityId = CapabilityId("pg.version.17");
pub const PG_VERSION_18: CapabilityId = CapabilityId("pg.version.18");

pub const PG_ROLE_PRIMARY: CapabilityId = CapabilityId("pg.role.primary");

// Also supports dynamic `pg.extension.<name>` lookup.
pub const PG_HAS_PG_STAT_STATEMENTS: CapabilityId = CapabilityId("pg.extension.pg_stat_statements");

#[derive(Debug, Clone)]
pub struct PostgresCapabilities {
    pub version_major: u32,
    pub version_minor: u32,
    pub is_primary: bool,
    pub is_in_recovery: bool,
    pub installed_extensions: Vec<String>,
}

impl PostgresCapabilities {
    pub async fn discover(context: PostgresAsync) -> ResultEP<Self> {
        let mut conn = context.get().await.map_err(|e| EpError::connect(e.to_string()))?;

        let version_raw = conn.simple_query_raw("SHOW server_version").await?;
        let version_rows = parse_simple_query_response(&version_raw)?;
        let version_str = version_rows.first().and_then(|r| r.get_idx(0)).unwrap_or("");
        let (version_major, version_minor) = parse_pg_version(version_str);

        let recovery_raw = conn.simple_query_raw("SELECT pg_is_in_recovery()").await?;
        let recovery_rows = parse_simple_query_response(&recovery_raw)?;
        let is_in_recovery = recovery_rows.first().and_then(|r| r.get_idx(0)).unwrap_or("f") == "t";
        let is_primary = !is_in_recovery;

        let ext_raw = conn.simple_query_raw("SELECT extname FROM pg_extension").await?;
        let ext_rows = parse_simple_query_response(&ext_raw)?;
        let installed_extensions: Vec<String> = ext_rows.iter().filter_map(|r| r.get_idx(0).map(|s| s.to_string())).collect();

        Ok(Self {
            version_major,
            version_minor,
            is_primary,
            is_in_recovery,
            installed_extensions,
        })
    }
}

fn parse_pg_version(version: &str) -> (u32, u32) {
    let mut parts = version.split(|c: char| !c.is_ascii_digit()).filter(|s| !s.is_empty());
    let major = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor)
}

impl CapabilityChecker for PostgresCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            "pg.role.primary" => self.is_primary,
            s if s.starts_with("pg.version.") => {
                s.strip_prefix("pg.version.").and_then(|v| v.parse::<u32>().ok()).is_some_and(|required| self.version_major >= required)
            }
            s if s.starts_with("pg.extension.") => {
                if let Some(name) = s.strip_prefix("pg.extension.") {
                    self.installed_extensions.iter().any(|ext| ext == name)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version() {
        assert_eq!(parse_pg_version("16.2"), (16, 2));
        assert_eq!(parse_pg_version("16.2 (Debian 16.2-1.pgdg120+2)"), (16, 2));
        assert_eq!(parse_pg_version("17"), (17, 0));
        assert_eq!(parse_pg_version(""), (0, 0));
    }

    fn make_caps(version_major: u32, is_primary: bool, extensions: Vec<String>) -> PostgresCapabilities {
        PostgresCapabilities {
            version_major,
            version_minor: 0,
            is_primary,
            is_in_recovery: !is_primary,
            installed_extensions: extensions,
        }
    }

    #[test]
    fn version_and_role_checks() {
        let pg16 = make_caps(16, true, Vec::new());
        assert!(pg16.has(&PG_VERSION_14));
        assert!(pg16.has(&PG_VERSION_15));
        assert!(pg16.has(&PG_VERSION_16));
        assert!(!pg16.has(&PG_VERSION_17));
        assert!(pg16.has(&PG_ROLE_PRIMARY));

        let replica = make_caps(16, false, Vec::new());
        assert!(!replica.has(&PG_ROLE_PRIMARY));
    }

    #[test]
    fn extension_checks() {
        let caps = make_caps(16, true, vec!["plpgsql".to_string(), "pg_stat_statements".to_string()]);
        assert!(caps.has(&PG_HAS_PG_STAT_STATEMENTS));
        assert!(caps.has(&CapabilityId("pg.extension.plpgsql")));
        assert!(!caps.has(&CapabilityId("pg.extension.postgis")));

        let no_ext = make_caps(16, true, Vec::new());
        assert!(!no_ext.has(&PG_HAS_PG_STAT_STATEMENTS));
        assert!(!no_ext.has(&CapabilityId("pg.nonexistent")));
    }
}
