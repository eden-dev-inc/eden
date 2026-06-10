use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::{EpError, ResultEP};
use mongo_core::MongoAsync;
use mongodb::bson::doc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub const MONGO_VERSION_4: CapabilityId = CapabilityId("mongo.version.4");
pub const MONGO_VERSION_5: CapabilityId = CapabilityId("mongo.version.5");
pub const MONGO_VERSION_6: CapabilityId = CapabilityId("mongo.version.6");
pub const MONGO_VERSION_7: CapabilityId = CapabilityId("mongo.version.7");

pub const MONGO_REPLICA_SET: CapabilityId = CapabilityId("mongo.topology.replica_set");
pub const MONGO_SHARDED: CapabilityId = CapabilityId("mongo.topology.sharded");
pub const MONGO_SHARDED_OR_MONGOS: CapabilityId = CapabilityId("mongo.topology.sharded_or_mongos");
pub const MONGO_STANDALONE: CapabilityId = CapabilityId("mongo.topology.standalone");
#[derive(Debug, Clone)]
pub struct MongoCapabilities {
    pub is_sharded: bool,
    pub is_replica_set: bool,
    pub is_mongos: bool,
    pub version_major: u32,
    pub version_minor: u32,
    pub version_full: String,
}

impl MongoCapabilities {
    pub async fn discover(context: MongoAsync) -> ResultEP<Self> {
        let mongo_client = context.get().await.map_err(EpError::connect)?;
        let admin_db = mongo_client.database("admin");

        let hello = timeout(DISCOVERY_TIMEOUT, admin_db.run_command(doc! { "hello": 1 }, None))
            .await
            .map_err(|_| EpError::metadata("timeout discovering mongo capabilities"))?
            .map_err(EpError::database)?;

        let is_mongos = hello.get_str("msg").map(|m| m == "isdbgrid").unwrap_or(false);
        let is_replica_set = hello.get_str("setName").is_ok();

        let is_sharded = is_mongos
            || timeout(DISCOVERY_TIMEOUT, admin_db.run_command(doc! { "listShards": 1 }, None))
                .await
                .ok()
                .and_then(|r| r.ok())
                .map(|doc| doc.get_array("shards").map(|shards| !shards.is_empty()).unwrap_or(false))
                .unwrap_or(false);

        let (version_major, version_minor, version_full) =
            match timeout(DISCOVERY_TIMEOUT, admin_db.run_command(doc! { "buildInfo": 1 }, None)).await {
                Ok(Ok(build_info)) => {
                    let version_str = build_info.get_str("version").unwrap_or("");
                    let (major, minor, _patch) = parse_mongo_version(version_str);
                    (major, minor, version_str.to_string())
                }
                Ok(Err(e)) => {
                    warn!("capabilities: buildInfo command failed, version unknown: {e}");
                    (0, 0, String::new())
                }
                Err(_) => {
                    warn!("capabilities: buildInfo command timed out, version unknown");
                    (0, 0, String::new())
                }
            };

        Ok(Self {
            is_sharded,
            is_replica_set,
            is_mongos,
            version_major,
            version_minor,
            version_full,
        })
    }
}

impl CapabilityChecker for MongoCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            s if s.starts_with("mongo.version.") => s
                .strip_prefix("mongo.version.")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|required| self.version_major >= required),
            "mongo.topology.replica_set" => self.is_replica_set,
            "mongo.topology.sharded" => self.is_sharded,
            "mongo.topology.sharded_or_mongos" => self.is_sharded || self.is_mongos,
            "mongo.topology.standalone" => !self.is_replica_set && !self.is_sharded && !self.is_mongos,
            _ => false,
        }
    }
}

/// Parse `"7.0.4"` or `"6.0.12-rc0"` into `(major, minor, patch)`.
fn parse_mongo_version(version: &str) -> (u32, u32, u32) {
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
    fn parse_version() {
        assert_eq!(parse_mongo_version("7.0.4"), (7, 0, 4));
        assert_eq!(parse_mongo_version("6.0.12"), (6, 0, 12));
        assert_eq!(parse_mongo_version("7.0.4-rc0"), (7, 0, 4));
        assert_eq!(parse_mongo_version("7"), (7, 0, 0));
        assert_eq!(parse_mongo_version(""), (0, 0, 0));
    }

    fn make_caps(is_replica_set: bool, is_sharded: bool, is_mongos: bool, version_major: u32) -> MongoCapabilities {
        MongoCapabilities {
            is_sharded,
            is_replica_set,
            is_mongos,
            version_major,
            version_minor: 0,
            version_full: format!("{version_major}.0.0"),
        }
    }

    #[test]
    fn topology_checks() {
        let rs = make_caps(true, false, false, 7);
        assert!(rs.has(&MONGO_REPLICA_SET));
        assert!(!rs.has(&MONGO_SHARDED));
        assert!(!rs.has(&MONGO_STANDALONE));

        let sharded = make_caps(false, true, false, 7);
        assert!(sharded.has(&MONGO_SHARDED));
        assert!(sharded.has(&MONGO_SHARDED_OR_MONGOS));
        assert!(!sharded.has(&MONGO_STANDALONE));

        let mongos = make_caps(false, false, true, 7);
        assert!(mongos.has(&MONGO_SHARDED_OR_MONGOS));
        assert!(!mongos.has(&MONGO_STANDALONE));

        let standalone = make_caps(false, false, false, 7);
        assert!(standalone.has(&MONGO_STANDALONE));
        assert!(!standalone.has(&MONGO_REPLICA_SET));
    }

    #[test]
    fn version_checks() {
        let v7 = make_caps(false, false, false, 7);
        assert!(v7.has(&MONGO_VERSION_4));
        assert!(v7.has(&MONGO_VERSION_7));

        let v4 = make_caps(false, false, false, 4);
        assert!(v4.has(&MONGO_VERSION_4));
        assert!(!v4.has(&MONGO_VERSION_5));

        assert!(!make_caps(false, false, false, 0).has(&MONGO_VERSION_4));
        assert!(!v7.has(&CapabilityId("mongo.nonexistent")));
    }
}
