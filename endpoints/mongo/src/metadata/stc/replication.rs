use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{DocAccessor, execute_admin_command_as_profiled};
use crate::metadata::capabilities::MONGO_REPLICA_SET;

/// MongoDB replication statistics and performance metrics
///
/// Simplified struct containing essential metrics about replica set
/// health, replication lag, and member status. Focuses on core replication indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoReplicationInfo {
    /// Total number of replica set members
    pub total_members: u32,
    /// Number of healthy replica set members
    pub healthy_members: u32,
    /// Number of members currently unreachable
    pub unreachable_members: u32,
    /// Primary server information
    pub primary_info: Option<ReplicaMemberInfo>,
    /// Secondary servers information
    pub secondary_info: Vec<ReplicaMemberInfo>,
    /// Maximum replication lag across all secondaries (milliseconds)
    pub max_replication_lag_ms: f64,
    /// Average replication lag across all secondaries (milliseconds)
    pub avg_replication_lag_ms: f64,
    /// Total number of oplog entries processed
    pub total_oplog_entries: u64,
    /// Oplog size in bytes
    pub oplog_size_bytes: u64,
    /// Oplog utilization percentage
    pub oplog_utilization_percentage: f64,
    /// Number of replication failures in the last period
    pub replication_failures: u64,
    /// Election count in the last period
    pub elections_count: u64,
    /// Time since last election (milliseconds)
    pub time_since_last_election_ms: f64,
    /// Network round-trip time to secondaries (milliseconds)
    pub avg_network_rtt_ms: f64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoReplicationDetailedMetrics>,
}

/// Information about a replica set member
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ReplicaMemberInfo {
    /// Member ID
    pub member_id: u32,
    /// Member name/host
    pub name: String,
    /// Member state (PRIMARY, SECONDARY, ARBITER, etc.)
    pub state: String,
    /// Member health (0 = down, 1 = up)
    pub health: u32,
    /// Replication lag in milliseconds
    pub replication_lag_ms: f64,
    /// Last heartbeat time
    pub last_heartbeat: Option<DateTimeWrapper>,
    /// Priority for elections
    pub priority: f64,
    /// Whether member is hidden
    pub hidden: bool,
    /// Whether member can vote in elections
    pub votes: u32,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoReplicationDetailedMetrics {
    /// Slow replication operations (only collected when lag > threshold)
    pub slow_replication_ops: Vec<MongoSlowReplicationOp>,
    /// Failed replication operations (only collected when failures > 0)
    pub failed_replication_ops: Vec<MongoFailedReplicationOp>,
    /// Replication breakdown by member (collected when issues detected)
    pub replication_by_member: Option<Vec<MongoReplicationByMember>>,
    /// Oplog analysis (only collected when oplog utilization is high)
    pub oplog_analysis: Option<MongoOplogAnalysis>,
    /// Election history (only collected when elections > threshold)
    pub election_history: Vec<MongoElectionEvent>,
}

/// Information about slow replication operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSlowReplicationOp {
    /// Operation ID
    pub operation_id: String,
    /// Source member
    pub source_member: String,
    /// Target member
    pub target_member: String,
    /// Operation type (insert, update, delete, etc.)
    pub operation_type: String,
    /// Replication time in milliseconds
    pub replication_time_ms: f64,
    /// Timestamp when the operation started
    pub timestamp: DateTimeWrapper,
    /// Database and collection
    pub namespace: String,
    /// Oplog timestamp
    pub oplog_ts: Option<DateTimeWrapper>,
}

/// Information about failed replication operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFailedReplicationOp {
    /// Operation ID
    pub operation_id: String,
    /// Source member
    pub source_member: String,
    /// Target member
    pub target_member: String,
    /// Error message
    pub error_message: String,
    /// Error code
    pub error_code: Option<i32>,
    /// Timestamp when the failure occurred
    pub timestamp: DateTimeWrapper,
    /// Number of retry attempts
    pub retry_attempts: u32,
    /// Database and collection
    pub namespace: String,
}

/// Replication statistics grouped by member
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoReplicationByMember {
    /// Member name
    pub member_name: String,
    /// Member ID
    pub member_id: u32,
    /// Total operations replicated
    pub total_operations: u64,
    /// Average replication time
    pub avg_replication_time_ms: f64,
    /// Failed operations
    pub failed_operations: u64,
    /// Current lag in milliseconds
    pub current_lag_ms: f64,
    /// Network latency to this member
    pub network_latency_ms: f64,
}

/// Oplog analysis information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOplogAnalysis {
    /// Oplog size in MB
    pub oplog_size_mb: f64,
    /// Oplog utilization percentage
    pub utilization_percentage: f64,
    /// Estimated time until oplog full (hours)
    pub estimated_hours_until_full: f64,
    /// Growth rate per hour (MB/hour)
    pub growth_rate_mb_per_hour: f64,
    /// Oldest operation timestamp
    pub oldest_operation: Option<DateTimeWrapper>,
    /// Newest operation timestamp
    pub newest_operation: Option<DateTimeWrapper>,
    /// Average operation size in bytes
    pub avg_operation_size_bytes: f64,
}

/// Election event information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoElectionEvent {
    /// Election timestamp
    pub timestamp: DateTimeWrapper,
    /// Previous primary
    pub previous_primary: Option<String>,
    /// New primary
    pub new_primary: String,
    /// Election cause
    pub cause: String,
    /// Election duration in milliseconds
    pub duration_ms: f64,
    /// Number of voting members
    pub voting_members: u32,
    /// Number of votes received by winner
    pub votes_received: u32,
}

impl MetadataCollection for MongoReplicationInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "replica_set_status".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.replSetGetStatus": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(10)),
                ),
            ),
            (
                "oplog_entries".to_string(),
                FindInput::new(
                    "local".to_string(),
                    "oplog.rs".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "replication_lag".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.replSetGetStatus": { "$exists": true } },
                            { "command.serverStatus": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(20)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential replication metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "replication"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

#[allow(dead_code)]
impl MongoReplicationInfo {
    const HIGH_REPLICATION_LAG_THRESHOLD_MS: f64 = 10000.0; // 10 seconds
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 50;
    const HIGH_OPLOG_UTILIZATION_THRESHOLD: f64 = 80.0; // 80%
    const ELECTION_THRESHOLD: u64 = 1; // Any elections trigger detailed collection

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        if !capabilities.has(&MONGO_REPLICA_SET) {
            return Ok(MongoReplicationInfo::default());
        }

        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut replication_info = MongoReplicationInfo::default();

        // Execute replSetGetStatus directly - contains replication data
        match execute_admin_command_as_profiled(doc! { "replSetGetStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "replSetGetStatus")
            .await
        {
            Ok(replica_status_docs) => {
                Self::parse_replica_set_status(&mut replication_info, &replica_status_docs)?;
            }
            Err(e) => {
                // MongoDB not running as replica set - return defaults
                // This is normal for standalone instances
                let err_str = e.to_string();
                if err_str.contains("NoReplicationEnabled") || err_str.contains("not running with --replSet") {
                    // Return default values for standalone MongoDB - this is OK
                } else {
                    // Other errors should propagate
                    return Err(e);
                }
            }
        }

        // Detailed metrics temporarily disabled during refactor
        replication_info.detailed_metrics = None;

        Ok(replication_info)
    }

    fn parse_replica_set_status(info: &mut MongoReplicationInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(members) = result.array("members") {
                    info.total_members = members.len() as u32;
                    let mut healthy_count = 0u32;
                    let mut unreachable_count = 0u32;
                    let mut secondary_info = Vec::new();
                    let mut replication_lags = Vec::new();

                    for member_acc in members {
                        let name = member_acc.opt_string("name").unwrap_or_else(|| "unknown".into());
                        let state_str = member_acc.opt_string("stateStr").unwrap_or_else(|| "UNKNOWN".into());
                        let member_id: u32 = member_acc.opt_u64("_id").unwrap_or(0) as u32;
                        let health: u32 = member_acc.opt_u64("health").unwrap_or(0) as u32;
                        let priority: f64 = member_acc.opt_f64("priority").unwrap_or(0.0);
                        let hidden = member_acc.opt_bool("hidden").unwrap_or(false);
                        let votes: u32 = member_acc.opt_u64("votes").unwrap_or(1) as u32;

                        if health == 1 {
                            healthy_count += 1;
                        } else {
                            unreachable_count += 1;
                        }

                        let replication_lag = Self::calculate_replication_lag(member_acc.raw())?;
                        if replication_lag > 0.0 {
                            replication_lags.push(replication_lag);
                        }

                        let last_heartbeat = member_acc.opt_datetime("lastHeartbeat");

                        let member_info = ReplicaMemberInfo {
                            member_id,
                            name: name.clone(),
                            state: state_str.clone(),
                            health,
                            replication_lag_ms: replication_lag,
                            last_heartbeat,
                            priority,
                            hidden,
                            votes,
                        };

                        if state_str == "PRIMARY" {
                            info.primary_info = Some(member_info);
                        } else if state_str == "SECONDARY" {
                            secondary_info.push(member_info);
                        }
                    }

                    info.healthy_members = healthy_count;
                    info.unreachable_members = unreachable_count;
                    info.secondary_info = secondary_info;

                    if !replication_lags.is_empty() {
                        info.max_replication_lag_ms = replication_lags.iter().fold(0.0f64, |a, &b| a.max(b));
                        info.avg_replication_lag_ms = replication_lags.iter().sum::<f64>() / replication_lags.len() as f64;
                    }
                }

                if result.child("electionId").is_some() {
                    info.elections_count = 1;
                }
            }
        }

        Ok(())
    }

    fn calculate_replication_lag(member: &Document) -> ResultEP<f64> {
        // Try to get optime difference
        if let (Ok(optime), Ok(primary_optime)) = (member.get_document("optime"), member.get_document("primaryOpTime"))
            && let (Ok(optime_ts), Ok(primary_ts)) = (optime.get_datetime("ts"), primary_optime.get_datetime("ts"))
        {
            let lag_duration = DateTime::<Utc>::from(*primary_ts).signed_duration_since(DateTime::<Utc>::from(*optime_ts));
            return Ok(lag_duration.num_milliseconds() as f64);
        }

        // Fallback to lastHeartbeat difference
        if let Ok(last_heartbeat) = member.get_datetime("lastHeartbeat") {
            let now = Utc::now();
            let lag_duration = now.signed_duration_since(DateTime::<Utc>::from(*last_heartbeat));
            return Ok(lag_duration.num_milliseconds() as f64);
        }

        Ok(0.0)
    }
}

impl MongoReplicationInfo {
    /// Returns the percentage of healthy members
    pub fn health_percentage(&self) -> f64 {
        if self.total_members == 0 {
            0.0
        } else {
            (self.healthy_members as f64 / self.total_members as f64) * 100.0
        }
    }

    /// Checks if replication lag is concerning
    pub fn has_high_replication_lag(&self, threshold_ms: f64) -> bool {
        self.max_replication_lag_ms > threshold_ms
    }

    /// Checks if the replica set has connectivity issues
    pub fn has_connectivity_issues(&self) -> bool {
        self.unreachable_members > 0 || self.replication_failures > 0
    }

    /// Checks if oplog utilization is high
    pub fn has_high_oplog_utilization(&self, threshold_percentage: f64) -> bool {
        self.oplog_utilization_percentage > threshold_percentage
    }

    /// Returns the number of voting members
    pub fn voting_members_count(&self) -> u32 {
        let mut count = 0;
        if let Some(primary) = &self.primary_info {
            count += primary.votes;
        }
        for secondary in &self.secondary_info {
            count += secondary.votes;
        }
        count
    }

    /// Checks if the replica set can achieve majority
    pub fn can_achieve_majority(&self) -> bool {
        let voting_members = self.voting_members_count();
        // Fall back to total_members when voting member details aren't populated
        let effective_total = if voting_members > 0 { voting_members } else { self.total_members };
        if effective_total == 0 {
            return false;
        }
        let quorum = effective_total / 2 + 1;
        self.healthy_members >= quorum
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns the average network performance score (0.0 to 1.0)
    pub fn network_performance_score(&self) -> f64 {
        // Lower RTT and lower lag = better score
        let rtt_score = if self.avg_network_rtt_ms < 10.0 {
            1.0
        } else if self.avg_network_rtt_ms < 50.0 {
            0.8
        } else if self.avg_network_rtt_ms < 100.0 {
            0.6
        } else {
            0.3
        };

        let lag_score = if self.avg_replication_lag_ms < 1000.0 {
            1.0
        } else if self.avg_replication_lag_ms < 5000.0 {
            0.7
        } else if self.avg_replication_lag_ms < 10000.0 {
            0.4
        } else {
            0.1
        };

        (rtt_score + lag_score) / 2.0
    }

    /// Returns the overall replica set health score (0.0 to 1.0)
    pub fn overall_health_score(&self) -> f64 {
        let health_score = self.health_percentage() / 100.0;
        let network_score = self.network_performance_score();
        let oplog_score = if self.oplog_utilization_percentage < 70.0 {
            1.0
        } else if self.oplog_utilization_percentage < 85.0 {
            0.7
        } else if self.oplog_utilization_percentage < 95.0 {
            0.4
        } else {
            0.1
        };

        (health_score + network_score + oplog_score) / 3.0
    }

    /// Returns election frequency score (lower is better)
    pub fn election_stability_score(&self) -> f64 {
        if self.elections_count == 0 {
            1.0
        } else if self.elections_count == 1 {
            0.8
        } else if self.elections_count <= 3 {
            0.5
        } else {
            0.2
        }
    }

    /// Checks if the replica set is experiencing instability
    pub fn is_unstable(&self) -> bool {
        self.elections_count > 1 || self.replication_failures > 5 || self.unreachable_members > 0 || self.overall_health_score() < 0.7
    }
}

#[cfg(all(test, external_db))]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_replication_info() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let replication_info = MongoReplicationInfo::default();

        let result = replication_info
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.health_percentage() >= 0.0);
        assert!(info.health_percentage() <= 100.0);
    }

    #[test]
    fn test_health_percentage() {
        let info = MongoReplicationInfo {
            total_members: 3,
            healthy_members: 2,
            ..MongoReplicationInfo::default()
        };

        assert!((info.health_percentage() - 66.667).abs() < 0.01);
    }

    #[test]
    fn test_can_achieve_majority() {
        let mut info = MongoReplicationInfo {
            total_members: 3,
            healthy_members: 2,
            ..MongoReplicationInfo::default()
        };

        // Simplified test - in reality this would check voting members
        assert!(info.can_achieve_majority());

        info.healthy_members = 1;
        assert!(!info.can_achieve_majority());
    }

    #[test]
    fn test_network_performance_score() {
        let info = MongoReplicationInfo {
            avg_network_rtt_ms: 5.0,
            avg_replication_lag_ms: 500.0,
            ..MongoReplicationInfo::default()
        };

        let score = info.network_performance_score();
        assert!(score > 0.8);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_overall_health_score() {
        let info = MongoReplicationInfo {
            total_members: 3,
            healthy_members: 3,
            avg_network_rtt_ms: 5.0,
            avg_replication_lag_ms: 500.0,
            oplog_utilization_percentage: 50.0,
            ..MongoReplicationInfo::default()
        };

        let score = info.overall_health_score();
        assert!(score > 0.8);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_is_unstable() {
        let mut info = MongoReplicationInfo {
            total_members: 3,
            healthy_members: 3,
            elections_count: 0,
            replication_failures: 0,
            unreachable_members: 0,
            avg_network_rtt_ms: 5.0,
            avg_replication_lag_ms: 500.0,
            oplog_utilization_percentage: 50.0,
            ..MongoReplicationInfo::default()
        };

        assert!(!info.is_unstable());

        // Add instability
        info.elections_count = 3;
        assert!(info.is_unstable());
    }
}
