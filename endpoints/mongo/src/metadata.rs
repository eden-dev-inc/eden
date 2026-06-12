pub mod capabilities;
pub mod profile;
pub mod stc;
pub mod sync;

use crate::metadata::profile::{MongoProfilingLevel, ensure_profiling_level, fetch_profiling_level};
use crate::metadata::{
    capabilities::{MONGO_REPLICA_SET, MONGO_SHARDED, MONGO_SHARDED_OR_MONGOS},
    stc::{
        aggregation::MongoAggregationStats, balancer::MongoBalancerInfo, collections::MongoCollectionInfo,
        connections::MongoConnectionInfo, database::MongoDatabaseInfo, gridfs::MongoGridFSInfo, indexes::MongoIndexInfo,
        locks::MongoLockInfo, memory::MongoMemoryInfo, network::MongoNetworkInfo, oplog::MongoOplogInfo,
        performance::MongoPerformanceStats, profiler::MongoProfilerInfo, replication::MongoReplicationInfo, security::MongoSecurityInfo,
        server::MongoServerInfo, sharding::MongoShardingInfo, transactions::MongoTransactionInfo, users::MongoUserInfo,
        wiredtiger::MongoWiredTigerInfo,
    },
    sync::MongoLastSyncTimestamps,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use eden_config::MongoProfilingMode;
use endpoint_types::metadata::{
    CapabilityChecker, CapabilityId, EpMetadata, MetadataCollection, MetadataJob, ProfilingRequirement, SyncCollector, SyncFrequency,
    SyncMetadata, UnknownCapabilities,
};
use ep_core::define_metadata_serializer_stuff;
use error::ResultEP;
use format::endpoint::EpKind;
use mongo_core::MongoAsync;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::future::Future;
use telemetry::TelemetryWrapper;
use tracing::warn;

/// Enhanced MongoMetadata with sync interval support
///
/// Mirrors the rich collectors defined in `stc/` while integrating with the
/// job-based metadata scheduler.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoMetadata {
    // high priority - critical performance and connection metrics
    pub connection_info: Option<MongoConnectionInfo>,     // connection pool status
    pub lock_info: Option<MongoLockInfo>,                 // blocking locks and contention
    pub network_info: Option<MongoNetworkInfo>,           // network traffic and connections
    pub performance_stats: Option<MongoPerformanceStats>, // query performance, cache hits
    pub replication_info: Option<MongoReplicationInfo>,   // replica set status
    pub server_info: Option<MongoServerInfo>,             // server status and health
    pub transaction_info: Option<MongoTransactionInfo>,   // transaction stats, deadlocks
    pub wiredtiger_info: Option<MongoWiredTigerInfo>,     // storage engine stats

    // medium priority - operational metrics
    pub aggregation_stats: Option<MongoAggregationStats>, // aggregation pipeline performance
    pub collection_info: Option<Vec<MongoCollectionInfo>>, // collection statistics
    pub database_stats: Option<Vec<MongoDatabaseInfo>>,   // per-database statistics
    pub gridfs_info: Option<MongoGridFSInfo>,             // GridFS file storage stats
    pub index_info: Option<Vec<MongoIndexInfo>>,          // index usage and performance
    pub oplog_info: Option<MongoOplogInfo>,               // oplog status and size
    pub profiler_info: Option<MongoProfilerInfo>,         // query profiling and slow ops
    pub sharding_info: Option<MongoShardingInfo>,         // sharding status and cluster info

    // low priority - configuration and static info
    pub balancer_info: Option<MongoBalancerInfo>, // detailed balancer statistics
    pub memory_info: Option<MongoMemoryInfo>,     // memory allocation and usage
    pub security_info: Option<MongoSecurityInfo>, // authentication and authorization stats
    pub user_info: Option<Vec<MongoUserInfo>>,    // user and role information

    // collection metadata
    pub collection_timestamp: Option<u64>,
    pub last_sync_timestamps: Option<MongoLastSyncTimestamps>,
}

impl Default for MongoMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl MongoMetadata {
    /// Creates a new instance with default sync configuration.
    pub fn new() -> Self {
        Self {
            aggregation_stats: Some(MongoAggregationStats::default()),
            balancer_info: Some(MongoBalancerInfo::default()),
            collection_info: Some(vec![MongoCollectionInfo::default()]),
            collection_timestamp: Some(0),
            connection_info: Some(MongoConnectionInfo::default()),
            database_stats: Some(vec![MongoDatabaseInfo::default()]),
            gridfs_info: Some(MongoGridFSInfo::default()),
            index_info: Some(vec![MongoIndexInfo::default()]),
            last_sync_timestamps: Some(MongoLastSyncTimestamps::default()),
            lock_info: Some(MongoLockInfo::default()),
            memory_info: Some(MongoMemoryInfo::default()),
            network_info: Some(MongoNetworkInfo::default()),
            oplog_info: Some(MongoOplogInfo::default()),
            performance_stats: Some(MongoPerformanceStats::default()),
            profiler_info: Some(MongoProfilerInfo::default()),
            replication_info: Some(MongoReplicationInfo::default()),
            security_info: Some(MongoSecurityInfo::default()),
            server_info: Some(MongoServerInfo::default()),
            sharding_info: Some(MongoShardingInfo::default()),
            transaction_info: Some(MongoTransactionInfo::default()),
            user_info: Some(vec![MongoUserInfo::default()]),
            wiredtiger_info: Some(MongoWiredTigerInfo::default()),
        }
    }

    fn update_timestamp<F>(&mut self, updater: F)
    where
        F: FnOnce(&mut MongoLastSyncTimestamps),
    {
        if let Some(ref mut timestamps) = self.last_sync_timestamps {
            updater(timestamps);
        }
    }
}

// ---------------------------------------------------------------------------
// SyncCollector<MongoAsync> implementations
// ---------------------------------------------------------------------------
// Delegates to each type's inherent `sync_metadata` method.  Inherent methods
// take priority over trait methods in Rust method resolution, so
// `self.sync_metadata(ctx, tel)` inside the impl body calls the inherent
// async fn, not the trait fn (no recursion).
macro_rules! impl_sync_collector_mongo {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<MongoAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: MongoAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures_util::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_mongo!(
    MongoAggregationStats,
    MongoBalancerInfo,
    MongoCollectionInfo,
    MongoConnectionInfo,
    MongoDatabaseInfo,
    MongoGridFSInfo,
    MongoIndexInfo,
    MongoLockInfo,
    MongoMemoryInfo,
    MongoNetworkInfo,
    MongoOplogInfo,
    MongoPerformanceStats,
    MongoProfilerInfo,
    MongoReplicationInfo,
    MongoSecurityInfo,
    MongoServerInfo,
    MongoShardingInfo,
    MongoTransactionInfo,
    MongoUserInfo,
    MongoWiredTigerInfo,
);

// ---------------------------------------------------------------------------
// Generic builder functions
// ---------------------------------------------------------------------------
// These contain ALL business logic (profiling checks, sync, update).
// The thin macros in jobs() only do field wiring.

/// Build a job for a single-value `Option<T>` field.
fn build_mongo_single_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    profiling_req: ProfilingRequirement,
    max_profiling: u8,
    get: fn(&MongoMetadata) -> &Option<T>,
    set: fn(&mut MongoMetadata, Option<T>),
    touch_ts: fn(&mut MongoMetadata),
) -> Option<MetadataJob<MongoAsync, MongoMetadata>>
where
    T: SyncCollector<MongoAsync> + 'static,
{
    if profiling_req.minimum_level() > max_profiling {
        return None;
    }

    Some(make_job(
        name,
        frequency,
        move |metadata: &mut MongoMetadata, ctx: MongoAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                if let Some(template) = get(metadata).clone() {
                    let requirement = template.profiling_requirement();
                    if !profiling_allows_execution(requirement, ctx.clone(), name).await? {
                        return Ok(());
                    }

                    let value = template.sync_metadata(ctx, telemetry, capabilities).await?;
                    set(metadata, Some(value));
                    touch_ts(metadata);
                }
                Ok(())
            })
        },
    ))
}

/// Build a job for an `Option<Vec<T>>` field.
fn build_mongo_vec_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    profiling_req: ProfilingRequirement,
    max_profiling: u8,
    get: fn(&MongoMetadata) -> &Option<Vec<T>>,
    set: fn(&mut MongoMetadata, Option<Vec<T>>),
    touch_ts: fn(&mut MongoMetadata),
) -> Option<MetadataJob<MongoAsync, MongoMetadata>>
where
    T: SyncCollector<MongoAsync> + 'static,
{
    if profiling_req.minimum_level() > max_profiling {
        return None;
    }

    Some(make_job(
        name,
        frequency,
        move |metadata: &mut MongoMetadata, ctx: MongoAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                if let Some(templates) = get(metadata).clone() {
                    let requirement = templates.iter().fold(ProfilingRequirement::Off, |current, t| {
                        let candidate = t.profiling_requirement();
                        if candidate.minimum_level() > current.minimum_level() {
                            candidate
                        } else {
                            current
                        }
                    });

                    if !profiling_allows_execution(requirement, ctx.clone(), name).await? {
                        return Ok(());
                    }

                    let mut results = Vec::with_capacity(templates.len());
                    for template in templates {
                        results.push(template.sync_metadata(ctx.clone(), telemetry, capabilities).await?);
                    }
                    set(metadata, Some(results));
                    touch_ts(metadata);
                }
                Ok(())
            })
        },
    ))
}

impl SyncMetadata<MongoAsync> for MongoMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<MongoAsync, Self>> {
        let mongo_config = eden_config::analytics().mongo.clone();
        let max_profiling: u8 = match mongo_config.profiling {
            MongoProfilingMode::Disabled => 0,
            MongoProfilingMode::Level1 | MongoProfilingMode::Dynamic => 1,
            MongoProfilingMode::Level2 => 2,
        };

        // Thin field-wiring macros — only extract field references and profiling
        // requirements.  All business logic lives in build_mongo_*_job().
        macro_rules! mongo_single {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_mongo_single_job(
                    $name,
                    $freq,
                    self.$field.as_ref().map(|t| t.profiling_requirement()).unwrap_or(ProfilingRequirement::Off),
                    max_profiling,
                    |m| &m.$field,
                    |m, v| m.$field = v,
                    |m| m.update_timestamp(|ts| ts.$ts_field = Utc::now().timestamp() as u64),
                )
            };
        }

        macro_rules! mongo_vec {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_mongo_vec_job(
                    $name,
                    $freq,
                    self.$field
                        .as_ref()
                        .map(|templates| {
                            templates.iter().fold(ProfilingRequirement::Off, |current, t| {
                                let c = t.profiling_requirement();
                                if c.minimum_level() > current.minimum_level() {
                                    c
                                } else {
                                    current
                                }
                            })
                        })
                        .unwrap_or(ProfilingRequirement::Off),
                    max_profiling,
                    |m| &m.$field,
                    |m, v| m.$field = v,
                    |m| m.update_timestamp(|ts| ts.$ts_field = Utc::now().timestamp() as u64),
                )
            };
        }

        self.collection_timestamp = Some(Utc::now().timestamp() as u64);
        if self.last_sync_timestamps.is_none() {
            self.last_sync_timestamps = Some(MongoLastSyncTimestamps::default());
        }

        let mut jobs = Vec::new();
        let replica_set_requirement: CapabilityId = MONGO_REPLICA_SET.clone();
        let sharded_requirement: CapabilityId = MONGO_SHARDED.clone();
        let sharded_or_mongos_requirement: CapabilityId = MONGO_SHARDED_OR_MONGOS.clone();

        if matches!(frequency, SyncFrequency::High) {
            jobs.extend(
                [
                    mongo_single!("mongo.connection_info", SyncFrequency::High, connection_info, connection_info_last_sync),
                    mongo_single!("mongo.lock_info", SyncFrequency::High, lock_info, lock_info_last_sync),
                    mongo_single!("mongo.network_info", SyncFrequency::High, network_info, network_info_last_sync),
                    mongo_single!("mongo.performance_stats", SyncFrequency::High, performance_stats, performance_stats_last_sync),
                    mongo_single!("mongo.replication_info", SyncFrequency::High, replication_info, replication_info_last_sync)
                        .map(|j| j.with_requirement(replica_set_requirement.clone())),
                    mongo_single!("mongo.server_info", SyncFrequency::High, server_info, server_info_last_sync),
                    mongo_single!("mongo.transaction_info", SyncFrequency::High, transaction_info, transaction_info_last_sync),
                    mongo_single!("mongo.wiredtiger_info", SyncFrequency::High, wiredtiger_info, wiredtiger_info_last_sync),
                ]
                .into_iter()
                .flatten(),
            );
        }

        if matches!(frequency, SyncFrequency::Medium) {
            // For level1/level2 static modes, inject a profiling setup job at the
            // start of the Medium tier so profiling is active before dependent
            // collectors run. This is a non-fatal safety net — if it fails, the
            // runtime `profiling_allows_execution` check will skip collectors.
            if matches!(mongo_config.profiling, MongoProfilingMode::Level1 | MongoProfilingMode::Level2) {
                let target_level = match mongo_config.profiling {
                    MongoProfilingMode::Level1 => MongoProfilingLevel::Level1,
                    MongoProfilingMode::Level2 => MongoProfilingLevel::Level2,
                    _ => unreachable!(),
                };
                let slow_ms = mongo_config.profiling_slow_ms;
                jobs.push(make_job(
                    "mongo.ensure_profiling",
                    SyncFrequency::Medium,
                    move |_metadata: &mut MongoMetadata, ctx, _telemetry, _capabilities| {
                        Box::pin(async move {
                            if let Err(err) = ensure_profiling_level(ctx, target_level, slow_ms).await {
                                warn!(
                                    ?target_level,
                                    slow_ms,
                                    error = ?err,
                                    "failed to set MongoDB profiling level; profiling-dependent collectors may be skipped"
                                );
                            }
                            Ok(())
                        })
                    },
                ));
            }

            jobs.extend(
                [
                    mongo_single!("mongo.aggregation_stats", SyncFrequency::Medium, aggregation_stats, aggregation_stats_last_sync),
                    mongo_vec!("mongo.collection_info", SyncFrequency::Medium, collection_info, collection_info_last_sync),
                    mongo_vec!("mongo.database_stats", SyncFrequency::Medium, database_stats, database_stats_last_sync),
                    mongo_single!("mongo.gridfs_info", SyncFrequency::Medium, gridfs_info, gridfs_info_last_sync),
                    mongo_vec!("mongo.index_info", SyncFrequency::Medium, index_info, index_info_last_sync),
                    mongo_single!("mongo.oplog_info", SyncFrequency::Medium, oplog_info, oplog_info_last_sync)
                        .map(|j| j.with_requirement(replica_set_requirement.clone())),
                    mongo_single!("mongo.profiler_info", SyncFrequency::Medium, profiler_info, profiler_info_last_sync),
                    mongo_single!("mongo.sharding_info", SyncFrequency::Medium, sharding_info, sharding_info_last_sync)
                        .map(|j| j.with_requirement(sharded_requirement.clone())),
                ]
                .into_iter()
                .flatten(),
            );
        }

        if matches!(frequency, SyncFrequency::Low) {
            // Inject profiling setup job in Low tier as well for level1/level2.
            if matches!(mongo_config.profiling, MongoProfilingMode::Level1 | MongoProfilingMode::Level2) {
                let target_level = match mongo_config.profiling {
                    MongoProfilingMode::Level1 => MongoProfilingLevel::Level1,
                    MongoProfilingMode::Level2 => MongoProfilingLevel::Level2,
                    _ => unreachable!(),
                };
                let slow_ms = mongo_config.profiling_slow_ms;
                jobs.push(make_job(
                    "mongo.ensure_profiling",
                    SyncFrequency::Low,
                    move |_metadata: &mut MongoMetadata, ctx, _telemetry, _capabilities| {
                        Box::pin(async move {
                            if let Err(err) = ensure_profiling_level(ctx, target_level, slow_ms).await {
                                warn!(
                                    ?target_level,
                                    slow_ms,
                                    error = ?err,
                                    "failed to set MongoDB profiling level; profiling-dependent collectors may be skipped"
                                );
                            }
                            Ok(())
                        })
                    },
                ));
            }

            jobs.extend(
                [
                    mongo_single!("mongo.balancer_info", SyncFrequency::Low, balancer_info, balancer_info_last_sync)
                        .map(|j| j.with_requirement(sharded_or_mongos_requirement.clone())),
                    mongo_single!("mongo.memory_info", SyncFrequency::Low, memory_info, memory_info_last_sync),
                    mongo_single!("mongo.security_info", SyncFrequency::Low, security_info, security_info_last_sync),
                    mongo_vec!("mongo.user_info", SyncFrequency::Low, user_info, user_info_last_sync),
                ]
                .into_iter()
                .flatten(),
            );
        }

        jobs
    }

    fn discover_capabilities<'a>(
        connection: MongoAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures_util::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match capabilities::MongoCapabilities::discover(connection).await {
                Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                Err(e) => {
                    warn!("failed to discover MongoDB capabilities: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities)
                }
            }
        })
    }
}

impl EpMetadata for MongoMetadata {
    fn as_metadata(self: Box<Self>) -> Box<dyn EpMetadata> {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn kind(&self) -> EpKind {
        EpKind::Mongo
    }

    fn clone_box(&self) -> Box<dyn EpMetadata> {
        Box::new(self.clone())
    }

    fn to_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        borsh::to_writer(writer, self)
    }
}

define_metadata_serializer_stuff!(EpKind::Mongo => MongoMetadata);

fn make_job<F>(name: &'static str, frequency: SyncFrequency, func: F) -> MetadataJob<MongoAsync, MongoMetadata>
where
    F: for<'a> Fn(
            &'a mut MongoMetadata,
            MongoAsync,
            &'a mut TelemetryWrapper,
            &'a dyn CapabilityChecker,
        ) -> std::pin::Pin<Box<dyn Future<Output = ResultEP<()>> + Send + 'a>>
        + Send
        + Sync
        + 'static,
{
    MetadataJob::new(name.to_string(), frequency, func)
}

async fn profiling_allows_execution(requirement: ProfilingRequirement, context: MongoAsync, job_name: &str) -> ResultEP<bool> {
    if !requirement.requires_profiling() {
        return Ok(true);
    }

    match fetch_profiling_level(context).await {
        Ok(level) => {
            if level.satisfies(requirement) {
                Ok(true)
            } else {
                warn!(
                    job = job_name,
                    ?level,
                    ?requirement,
                    "skipping MongoDB metadata collector because profiling level is below requirement"
                );
                Ok(false)
            }
        }
        Err(err) => {
            warn!(
                job = job_name,
                error = ?err,
                ?requirement,
                "skipping MongoDB metadata collector because profiling level could not be determined"
            );
            Ok(false)
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::collections::HashSet;

    fn job_names(jobs: &[MetadataJob<MongoAsync, MongoMetadata>]) -> HashSet<&str> {
        jobs.iter().map(|job| job.name()).collect()
    }

    /// Install config with the given profiling mode for testing.
    fn install_profiling_mode(mode: MongoProfilingMode) {
        eden_config::update_config(|c| {
            c.analytics.mongo.profiling = mode;
        })
        .expect("config update failed");
    }

    // Default mode (disabled): profiling-dependent collectors are filtered out.

    #[test]
    #[serial]
    fn high_frequency_jobs_disabled_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Disabled);
        let mut metadata = MongoMetadata { collection_timestamp: Some(0), ..MongoMetadata::default() };

        let jobs = metadata.jobs(SyncFrequency::High);

        // All 8 high-frequency collectors have ProfilingRequirement::Off, so all are present.
        assert_eq!(jobs.len(), 8, "expected eight high-frequency collectors in disabled mode");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::High),
            "high-frequency job list contains mismatched frequencies"
        );
        assert!(
            matches!(metadata.collection_timestamp, Some(ts) if ts > 0),
            "collection timestamp should be refreshed"
        );
        assert!(
            metadata.last_sync_timestamps.is_some(),
            "high-frequency jobs should ensure sync timestamps are present"
        );

        let names = job_names(&jobs);
        for expected in [
            "mongo.connection_info",
            "mongo.lock_info",
            "mongo.network_info",
            "mongo.performance_stats",
            "mongo.replication_info",
            "mongo.server_info",
            "mongo.transaction_info",
            "mongo.wiredtiger_info",
        ] {
            assert!(names.contains(expected), "expected job '{}' missing from high-frequency schedule", expected);
        }
    }

    #[test]
    #[serial]
    fn medium_frequency_jobs_disabled_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Disabled);
        let mut metadata = MongoMetadata { collection_timestamp: Some(0), ..MongoMetadata::default() };

        let jobs = metadata.jobs(SyncFrequency::Medium);

        // With profiling disabled, only sharding_info (Off requirement) survives.
        // aggregation_stats, collection_info, database_stats, gridfs_info,
        // index_info, oplog_info, profiler_info all require Level1.
        let names = job_names(&jobs);
        assert!(names.contains("mongo.sharding_info"), "sharding_info should be present in disabled mode");
        assert!(!names.contains("mongo.profiler_info"), "profiler_info should be filtered in disabled mode");
        assert!(!names.contains("mongo.aggregation_stats"), "aggregation_stats should be filtered in disabled mode");
    }

    #[test]
    #[serial]
    fn low_frequency_jobs_disabled_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Disabled);
        let mut metadata = MongoMetadata {
            collection_timestamp: Some(0),
            last_sync_timestamps: None,
            ..MongoMetadata::default()
        };

        let jobs = metadata.jobs(SyncFrequency::Low);

        // balancer_info (Off) survives; memory_info, security_info, user_info require Level1.
        let names = job_names(&jobs);
        assert!(names.contains("mongo.balancer_info"), "balancer_info should be present in disabled mode");
        assert!(!names.contains("mongo.memory_info"), "memory_info should be filtered in disabled mode");
        assert!(!names.contains("mongo.security_info"), "security_info should be filtered in disabled mode");
        assert!(!names.contains("mongo.user_info"), "user_info should be filtered in disabled mode");
        assert!(
            metadata.last_sync_timestamps.is_some(),
            "low-frequency run should initialize sync timestamps when missing"
        );
    }

    // Level1 mode: all collectors registered, plus profiling setup jobs.

    #[test]
    #[serial]
    fn high_frequency_jobs_level1_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Level1);
        let mut metadata = MongoMetadata::default();

        let jobs = metadata.jobs(SyncFrequency::High);
        // No profiling setup job in High tier; all 8 collectors present.
        assert_eq!(jobs.len(), 8, "expected eight high-frequency collectors in level1 mode");
    }

    #[test]
    #[serial]
    fn medium_frequency_jobs_level1_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Level1);
        let mut metadata = MongoMetadata::default();

        let jobs = metadata.jobs(SyncFrequency::Medium);
        let names = job_names(&jobs);

        // 1 profiling setup + 8 collectors = 9
        assert_eq!(jobs.len(), 9, "expected 9 medium-frequency jobs in level1 mode (1 setup + 8 collectors)");
        assert!(names.contains("mongo.ensure_profiling"), "ensure_profiling job should be present");
        assert!(names.contains("mongo.aggregation_stats"), "aggregation_stats should be present in level1 mode");
        assert!(names.contains("mongo.profiler_info"), "profiler_info should be present in level1 mode");
        assert!(names.contains("mongo.sharding_info"), "sharding_info should be present in level1 mode");
    }

    #[test]
    #[serial]
    fn low_frequency_jobs_level1_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Level1);
        let mut metadata = MongoMetadata::default();

        let jobs = metadata.jobs(SyncFrequency::Low);
        let names = job_names(&jobs);

        // 1 profiling setup + 4 collectors = 5
        assert_eq!(jobs.len(), 5, "expected 5 low-frequency jobs in level1 mode (1 setup + 4 collectors)");
        assert!(names.contains("mongo.ensure_profiling"), "ensure_profiling job should be present");
        assert!(names.contains("mongo.balancer_info"), "balancer_info should be present in level1 mode");
        assert!(names.contains("mongo.memory_info"), "memory_info should be present in level1 mode");
        assert!(names.contains("mongo.security_info"), "security_info should be present in level1 mode");
        assert!(names.contains("mongo.user_info"), "user_info should be present in level1 mode");
    }

    // Dynamic mode: same registration as level1 (collectors registered), no setup jobs.

    #[test]
    #[serial]
    fn medium_frequency_jobs_dynamic_mode() {
        eden_config::install_default_config();
        install_profiling_mode(MongoProfilingMode::Dynamic);
        let mut metadata = MongoMetadata::default();

        let jobs = metadata.jobs(SyncFrequency::Medium);
        let names = job_names(&jobs);

        // Dynamic mode registers collectors (same as level1) but does NOT inject
        // ensure_profiling setup jobs — profiling is toggled by the escalation bridge.
        assert_eq!(jobs.len(), 8, "expected 8 medium-frequency collectors in dynamic mode (no setup job)");
        assert!(!names.contains("mongo.ensure_profiling"), "dynamic mode should not have ensure_profiling job");
        assert!(names.contains("mongo.aggregation_stats"), "aggregation_stats should be present in dynamic mode");
    }
}
