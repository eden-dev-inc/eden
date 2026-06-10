// TODO: revisit once run_transaction_generic stubs are implemented with telemetry.
// #[named] is applied for future function_name!() use in telemetry spans.
#[allow(unused_macros)]
pub mod capabilities;
pub mod parser;
pub mod stats;
pub mod stc;
use crate::metadata::capabilities::REDIS_CLUSTER;
use crate::metadata::parser::{
    ParsingErrors, load_client_info, load_cluster_info, load_configuration_info, load_cpu_info, load_database_stats, load_memory_info,
    load_modules_info, load_persistence_info, load_replication_info, load_security_info, load_server_info, load_structure_samples,
};
use crate::metadata::stc::{
    client::RedisClientInfo, cluster::RedisClusterInfo, config::RedisConfigInfo, cpu::RedisCpuInfo, database::RedisDatabaseStats,
    memory::RedisMemoryInfo, module::RedisModulesInfo, persistence::RedisPersistenceInfo, replication::RedisReplicationInfo,
    security::RedisSecurityInfo, server::RedisServerInfo, structure_sampling::RedisStructureSamples,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{
    CapabilityChecker, CapabilityId, EpMetadata, MetadataJob, SyncCollector, SyncFrequency, SyncMetadata, UnknownCapabilities,
};
use ep_core::define_metadata_serializer_stuff;
use error::ResultEP;
use format::endpoint::EpKind;
use redis_core::RedisAsync;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use telemetry::{MetricEvent, TelemetryWrapper};
use tracing::warn;

/// Enhanced RedisMetadata with sync interval support
///
/// This replaces the simple sync_interval field with comprehensive
/// per-category sync tracking and configuration.
///
/// Note: Slowlog and keyspace sampling have been removed as they are now
/// captured via wire protocol analytics, which provides
/// more accurate real-time data with tenant/service attribution.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct RedisMetadata {
    // high
    pub client_info: RedisClientInfo,
    pub cpu_info: RedisCpuInfo,
    pub memory_info: RedisMemoryInfo,
    pub replication_info: RedisReplicationInfo,
    // medium
    pub cluster_info: Option<RedisClusterInfo>,  // if using clustering
    pub database_stats: Vec<RedisDatabaseStats>, // keyspace info
    pub persistence_info: RedisPersistenceInfo,  // backup status
    pub modules_info: RedisModulesInfo,          // module information
    // low
    pub configuration: RedisConfigInfo,                   // config changes
    pub server_info: RedisServerInfo,                     // version, uptime
    pub security_info: RedisSecurityInfo,                 // auth settings
    pub structure_samples: Option<RedisStructureSamples>, // sampled key structure data
    // collection data
    pub collection_timestamp: u64,
    // parsing errors and warnings
    pub parsing_errors: ParsingErrors,
}

// ---------------------------------------------------------------------------
// SyncCollector<RedisAsync> implementations
// ---------------------------------------------------------------------------
// Redis collectors delegate to standalone load_* functions rather than
// inherent sync_metadata methods.
macro_rules! impl_sync_collector_redis {
    ($type:ty, $loader:path) => {
        impl SyncCollector<RedisAsync> for $type {
            fn sync_metadata<'a>(
                &'a self,
                context: RedisAsync,
                telemetry: &'a mut TelemetryWrapper,
                capabilities: &'a dyn CapabilityChecker,
            ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                Box::pin($loader(context, telemetry, capabilities))
            }
        }
    };
}

impl_sync_collector_redis!(RedisClientInfo, load_client_info);
impl_sync_collector_redis!(RedisCpuInfo, load_cpu_info);
impl_sync_collector_redis!(RedisMemoryInfo, load_memory_info);
impl_sync_collector_redis!(RedisReplicationInfo, load_replication_info);
impl_sync_collector_redis!(RedisPersistenceInfo, load_persistence_info);
impl_sync_collector_redis!(RedisModulesInfo, load_modules_info);
impl_sync_collector_redis!(RedisConfigInfo, load_configuration_info);
impl_sync_collector_redis!(RedisServerInfo, load_server_info);
impl_sync_collector_redis!(RedisSecurityInfo, load_security_info);
impl_sync_collector_redis!(RedisStructureSamples, load_structure_samples);

// ---------------------------------------------------------------------------
// Generic builder functions
// ---------------------------------------------------------------------------

/// Build a job for a plain `T` field (sync via SyncCollector, set field).
fn build_redis_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut RedisMetadata, T),
) -> MetadataJob<RedisAsync, RedisMetadata>
where
    T: SyncCollector<RedisAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut RedisMetadata, ctx: RedisAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, value);
                Ok(())
            })
        },
    )
}

/// Build a job that records success/failure to `parsing_errors` (RecordAndContinue pattern).
/// On error the error is still propagated (job fails), but a critical message is also recorded.
fn build_redis_job_with_errors<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut RedisMetadata, T),
    success_msg: &'static str,
    error_msg_prefix: &'static str,
) -> MetadataJob<RedisAsync, RedisMetadata>
where
    T: SyncCollector<RedisAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut RedisMetadata, ctx: RedisAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                match T::default().sync_metadata(ctx, telemetry, capabilities).await {
                    Ok(value) => {
                        set(metadata, value);
                        metadata.parsing_errors.add_info(success_msg.to_string());
                    }
                    Err(e) => {
                        metadata.parsing_errors.add_critical(format!("{error_msg_prefix}{e}"));
                        return Err(e);
                    }
                }
                Ok(())
            })
        },
    )
}

impl SyncMetadata<RedisAsync> for RedisMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<RedisAsync, Self>> {
        if matches!(frequency, SyncFrequency::High) {
            self.parsing_errors = ParsingErrors::default();
        }
        self.collection_timestamp = Utc::now().timestamp().try_into().unwrap_or_default();

        let mut jobs: Vec<MetadataJob<RedisAsync, Self>> = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.push(build_redis_job_with_errors::<RedisClientInfo>(
                "redis.client_info",
                SyncFrequency::High,
                |m, v| m.client_info = v,
                "Client info loaded successfully",
                "Failed to load client info: ",
            ));

            jobs.push(build_redis_job::<RedisCpuInfo>("redis.cpu_info", SyncFrequency::High, |m, v| m.cpu_info = v));

            jobs.push(build_redis_job_with_errors::<RedisMemoryInfo>(
                "redis.memory_info",
                SyncFrequency::High,
                |m, v| m.memory_info = v,
                "Memory info loaded successfully",
                "Failed to load memory info: ",
            ));

            jobs.push(build_redis_job::<RedisReplicationInfo>("redis.replication_info", SyncFrequency::High, |m, v| {
                m.replication_info = v
            }));

            // Custom closure: loader returns Vec<T>, not a single T
            jobs.push(MetadataJob::new(
                "redis.database_stats".to_string(),
                SyncFrequency::High,
                move |metadata: &mut Self, ctx: RedisAsync, telemetry, capabilities| {
                    Box::pin(async move {
                        metadata.database_stats = load_database_stats(ctx, telemetry, capabilities).await?;
                        Ok(())
                    })
                },
            ));

            // Custom closure: aggregates data from other fields, no loader
            jobs.push(MetadataJob::new(
                "redis.workload_metrics".to_string(),
                SyncFrequency::High,
                move |metadata: &mut Self, _ctx: RedisAsync, telemetry, _capabilities| {
                    Box::pin(async move {
                        let endpoint_id = telemetry.labels().endpoint_uuid().unwrap_or("unknown").to_string();
                        let org_uuid = telemetry.labels().org_uuid().unwrap_or("unknown").to_string();

                        let used_memory = metadata.memory_info.used_memory;
                        let avg_ops_per_sec = 0.0;
                        let instantaneous_ops_per_sec = 0;
                        let total_commands_processed = 0;

                        let (total_keys, keys_with_ttl) =
                            metadata.database_stats.iter().fold((0u64, 0u64), |(keys, ttl), db| (keys + db.keys, ttl + db.expires));

                        let used_cpu_user: f64 = metadata.cpu_info.used_cpu_user;
                        let connected_clients = metadata.client_info.connected_clients as u64;

                        telemetry.record_event(MetricEvent::WorkloadSnapshot {
                            org_uuid: &org_uuid,
                            endpoint_id: &endpoint_id,
                            avg_ops_per_sec,
                            used_memory_bytes: used_memory,
                            total_keys,
                            keys_with_ttl,
                            instantaneous_ops_per_sec,
                            total_commands_processed,
                            used_cpu_user,
                            connected_clients,
                        });

                        Ok(())
                    })
                },
            ));
        }

        if matches!(frequency, SyncFrequency::Medium) {
            let cluster_requirement: CapabilityId = REDIS_CLUSTER.clone();
            // Custom closure: loader returns Option<T> + section error recording
            jobs.push(
                MetadataJob::new(
                    "redis.cluster_info".to_string(),
                    SyncFrequency::Medium,
                    move |metadata: &mut Self, ctx: RedisAsync, telemetry, capabilities| {
                        Box::pin(async move {
                            match load_cluster_info(ctx, telemetry, capabilities).await {
                                Ok(cluster_info) => {
                                    metadata.cluster_info = cluster_info;
                                    metadata.parsing_errors.add_info("Cluster info loaded successfully".to_string());
                                }
                                Err(e) => {
                                    metadata
                                        .parsing_errors
                                        .add_section_error("cluster".to_string(), format!("Failed to load cluster info: {}", e));
                                }
                            }
                            Ok(())
                        })
                    },
                )
                .with_requirement(cluster_requirement),
            );

            jobs.push(build_redis_job::<RedisPersistenceInfo>("redis.persistence_info", SyncFrequency::Medium, |m, v| {
                m.persistence_info = v
            }));

            jobs.push(build_redis_job::<RedisModulesInfo>("redis.modules_info", SyncFrequency::Medium, |m, v| {
                m.modules_info = v
            }));
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.push(build_redis_job::<RedisConfigInfo>("redis.configuration", SyncFrequency::Low, |m, v| {
                m.configuration = v
            }));

            jobs.push(build_redis_job::<RedisServerInfo>("redis.server_info", SyncFrequency::Low, |m, v| {
                m.server_info = v
            }));

            jobs.push(build_redis_job::<RedisSecurityInfo>("redis.security_info", SyncFrequency::Low, |m, v| {
                m.security_info = v
            }));

            jobs.push(MetadataJob::new(
                "redis.structure_sampling".to_string(),
                SyncFrequency::Low,
                move |metadata: &mut Self, ctx: RedisAsync, telemetry, capabilities| {
                    Box::pin(async move {
                        match RedisStructureSamples::default().sync_metadata(ctx, telemetry, capabilities).await {
                            Ok(value) => {
                                metadata.structure_samples = Some(value);
                                metadata.parsing_errors.add_info("Structure sampling loaded successfully".to_string());
                            }
                            Err(e) => {
                                metadata.parsing_errors.add_section_error(
                                    "structure_sampling".to_string(),
                                    format!("Failed to load structure samples: {}", e),
                                );
                            }
                        }
                        Ok(())
                    })
                },
            ));
        }

        jobs
    }

    fn discover_capabilities<'a>(
        connection: RedisAsync,
        telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match capabilities::RedisCapabilities::discover(connection, telemetry).await {
                Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                Err(e) => {
                    warn!("failed to discover Redis capabilities: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities)
                }
            }
        })
    }
}

impl EpMetadata for RedisMetadata {
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
        EpKind::Redis
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

define_metadata_serializer_stuff!(EpKind::Redis => RedisMetadata);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn to_name_set(jobs: &[MetadataJob<RedisAsync, RedisMetadata>]) -> HashSet<&str> {
        jobs.iter().map(|job| job.name()).collect()
    }

    #[test]
    fn high_frequency_jobs_reset_errors_and_cover_expected_collectors() {
        let mut metadata = RedisMetadata::default();
        metadata.parsing_errors.add_info("stale state should reset".to_string());
        metadata.collection_timestamp = 0;

        let jobs = metadata.jobs(SyncFrequency::High);

        assert_eq!(jobs.len(), 6, "unexpected number of high-frequency jobs");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::High),
            "high-frequency job list contains mismatched frequencies",
        );
        assert!(metadata.collection_timestamp > 0, "collection timestamp should be refreshed");
        assert!(
            metadata.parsing_errors.info_messages.is_empty(),
            "parsing errors should be cleared on high-frequency runs"
        );

        let names = to_name_set(&jobs);
        for expected in [
            "redis.client_info",
            "redis.cpu_info",
            "redis.memory_info",
            "redis.replication_info",
            "redis.database_stats",
            "redis.workload_metrics",
        ] {
            assert!(names.contains(expected), "expected job '{}' missing from high-frequency schedule", expected);
        }
    }

    #[test]
    fn medium_frequency_jobs_preserve_errors_and_schedule_expected_collectors() {
        let mut metadata = RedisMetadata::default();
        metadata.parsing_errors.add_warning("should persist".to_string());
        metadata.collection_timestamp = 0;

        let jobs = metadata.jobs(SyncFrequency::Medium);

        assert_eq!(jobs.len(), 3, "unexpected number of medium-frequency jobs");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::Medium),
            "medium-frequency job list contains mismatched frequencies",
        );
        assert!(metadata.collection_timestamp > 0, "collection timestamp should update on medium runs");
        assert!(!metadata.parsing_errors.warning_errors.is_empty(), "medium runs should not reset parsing errors");

        let names = to_name_set(&jobs);
        for expected in ["redis.cluster_info", "redis.persistence_info", "redis.modules_info"] {
            assert!(names.contains(expected), "expected job '{}' missing from medium-frequency schedule", expected);
        }
    }

    #[test]
    fn low_frequency_jobs_schedule_configuration_collectors() {
        let mut metadata = RedisMetadata { collection_timestamp: 0, ..Default::default() };

        let jobs = metadata.jobs(SyncFrequency::Low);

        assert_eq!(jobs.len(), 4, "unexpected number of low-frequency jobs");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::Low),
            "low-frequency job list contains mismatched frequencies",
        );
        assert!(metadata.collection_timestamp > 0, "collection timestamp should update on low runs");

        let names = to_name_set(&jobs);
        for expected in [
            "redis.configuration",
            "redis.server_info",
            "redis.security_info",
            "redis.structure_sampling",
        ] {
            assert!(names.contains(expected), "expected job '{}' missing from low-frequency schedule", expected);
        }
    }

    #[test]
    fn package_lookup_yields_expected_job() {
        let mut metadata = RedisMetadata::default();

        let job = metadata.package("redis.client_info").expect("redis.client_info package should be defined");

        assert_eq!(job.name(), "redis.client_info");
        assert_eq!(job.frequency(), SyncFrequency::High);
        assert!(metadata.package("redis.unknown").is_none(), "unknown package lookup should return None");
    }
}
