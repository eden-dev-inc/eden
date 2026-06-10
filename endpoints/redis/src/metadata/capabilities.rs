use crate::metadata::parser::common::{fetch_info_section, parse_bool};
use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::ResultEP;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

pub const REDIS_VERSION_6: CapabilityId = CapabilityId("redis.version.6");
pub const REDIS_VERSION_7: CapabilityId = CapabilityId("redis.version.7");

pub const REDIS_CLUSTER: CapabilityId = CapabilityId("redis.cluster");

#[derive(Debug, Clone)]
pub struct RedisCapabilities {
    pub version_major: u32,
    pub version_minor: u32,
    pub server_version: Option<String>,
    pub cluster_enabled: bool,
    pub loaded_modules: Vec<String>,
}

impl RedisCapabilities {
    pub async fn discover(context: RedisAsync, telemetry: &mut TelemetryWrapper) -> ResultEP<Self> {
        let cluster_section = fetch_info_section(context.clone(), telemetry, "cluster").await?;
        let cluster_enabled = parse_bool(cluster_section.map.get("cluster_enabled"));

        let server_section = fetch_info_section(context.clone(), telemetry, "server").await?;
        let server_version = server_section.map.get("redis_version").cloned();
        let (version_major, version_minor) = server_version.as_deref().map(parse_redis_version).unwrap_or((0, 0));

        let modules_section = fetch_info_section(context, telemetry, "modules").await?;
        // Parse module names from raw INFO text. The parsed HashMap loses
        // duplicate `module:` keys (all map to key "module" after split on first ':').
        let loaded_modules: Vec<String> = modules_section
            .raw
            .lines()
            .filter_map(|line| {
                let rest = line.trim().strip_prefix("module:")?;
                rest.split(',').find_map(|part| {
                    let name = part.strip_prefix("name=")?;
                    Some(name.to_lowercase())
                })
            })
            .collect();

        Ok(Self {
            version_major,
            version_minor,
            server_version,
            cluster_enabled,
            loaded_modules,
        })
    }
}

impl CapabilityChecker for RedisCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            s if s.starts_with("redis.version.") => s
                .strip_prefix("redis.version.")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|required| self.version_major >= required),
            "redis.cluster" => self.cluster_enabled,
            s if s.starts_with("redis.module.") => {
                if let Some(name) = s.strip_prefix("redis.module.") {
                    self.loaded_modules.iter().any(|m| m == name)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

/// Parse `"7.2.4"` or `"7.2.0-rc1"` into `(major, minor)`.
fn parse_redis_version(version: &str) -> (u32, u32) {
    let parts: Vec<&str> = version.split('.').collect();
    let major = parts.first().and_then(|p| p.split('-').next()).and_then(|p| p.parse().ok()).unwrap_or(0);
    let minor = parts.get(1).and_then(|p| p.split('-').next()).and_then(|p| p.parse().ok()).unwrap_or(0);
    (major, minor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version() {
        assert_eq!(parse_redis_version("7.2.4"), (7, 2));
        assert_eq!(parse_redis_version("6.2.14"), (6, 2));
        assert_eq!(parse_redis_version("7.2.0-rc1"), (7, 2));
        assert_eq!(parse_redis_version("7.0"), (7, 0));
        assert_eq!(parse_redis_version(""), (0, 0));
    }

    #[test]
    fn capability_checks() {
        let caps = RedisCapabilities {
            version_major: 7,
            version_minor: 2,
            server_version: Some("7.2.4".to_string()),
            cluster_enabled: true,
            loaded_modules: vec!["search".to_string(), "rejson".to_string()],
        };

        assert!(caps.has(&REDIS_VERSION_6));
        assert!(caps.has(&REDIS_VERSION_7));
        assert!(!caps.has(&CapabilityId("redis.version.8")));
        assert!(caps.has(&REDIS_CLUSTER));
        assert!(caps.has(&CapabilityId("redis.module.search")));
        assert!(caps.has(&CapabilityId("redis.module.rejson")));
        assert!(!caps.has(&CapabilityId("redis.module.timeseries")));
        assert!(!caps.has(&CapabilityId("unknown.capability")));
    }
}
