pub mod capabilities;
pub mod stc;
pub mod sync;

use crate::metadata::{
    capabilities::{PG_ROLE_PRIMARY, PG_VERSION_14},
    stc::{
        activity::PostgresActivityInfo, bgwriter::PostgresBgWriterInfo, connections::PostgresConnectionInfo,
        database::PostgresDatabaseStats, extensions::PostgresExtensionInfo, indexes::PostgresIndexInfo, locks::PostgresLockInfo,
        replication::PostgresReplicationInfo, schema_graph::PostgresSchemaGraph, settings::PostgresSettingsInfo,
        stats::PostgresPerformanceStats, tables::PostgresTableInfo, transactions::PostgresTransactionInfo, vacuum::PostgresVacuumInfo,
        wal::PostgresWalInfo,
    },
    sync::PostgresLastSyncTimestamps,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{
    CapabilityChecker, CapabilityId, EpMetadata, MetadataJob, SyncCollector, SyncFrequency, SyncMetadata, UnknownCapabilities,
};
use error::ResultEP;
use format::endpoint::EpKind;
use log::warn;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use telemetry::TelemetryWrapper;
use {ep_core::define_metadata_serializer_stuff, postgres_core::PostgresAsync};

/// Enhanced PostgresMetadata with sync interval support
///
/// This replaces the simple sync_interval field with comprehensive
/// per-category sync tracking and configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresMetadata {
    // high priority - critical performance and connection metrics
    pub activity_info: PostgresActivityInfo,         // current queries, connections
    pub connection_info: PostgresConnectionInfo,     // connection pool status
    pub lock_info: PostgresLockInfo,                 // blocking locks
    pub performance_stats: PostgresPerformanceStats, // query performance, cache hits
    pub replication_info: PostgresReplicationInfo,   // master/replica status
    pub transaction_info: PostgresTransactionInfo,   // transaction stats, deadlocks
    pub wal_info: PostgresWalInfo,                   // write-ahead log status

    // medium priority - operational metrics
    pub bgwriter_info: PostgresBgWriterInfo,        // background writer stats
    pub database_stats: Vec<PostgresDatabaseStats>, // per-database statistics
    pub index_info: Vec<PostgresIndexInfo>,         // index usage and bloat
    pub table_info: Vec<PostgresTableInfo>,         // table statistics
    pub vacuum_info: PostgresVacuumInfo,            // vacuum and analyze stats

    // low priority - configuration and static info
    pub extension_info: Vec<PostgresExtensionInfo>, // installed extensions
    pub settings_info: PostgresSettingsInfo,        // configuration parameters
    pub schema_graph: Option<PostgresSchemaGraph>,  // relational schema graph for curation

    // collection metadata
    pub collection_timestamp: u64,
    pub last_sync_timestamps: PostgresLastSyncTimestamps,
}

// ---------------------------------------------------------------------------
// SyncCollector<PostgresAsync> implementations
// ---------------------------------------------------------------------------
macro_rules! impl_sync_collector_pg {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<PostgresAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: PostgresAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_pg!(
    PostgresActivityInfo,
    PostgresBgWriterInfo,
    PostgresConnectionInfo,
    PostgresDatabaseStats,
    PostgresExtensionInfo,
    PostgresIndexInfo,
    PostgresLockInfo,
    PostgresPerformanceStats,
    PostgresReplicationInfo,
    PostgresSchemaGraph,
    PostgresSettingsInfo,
    PostgresTableInfo,
    PostgresTransactionInfo,
    PostgresVacuumInfo,
    PostgresWalInfo,
);

// ---------------------------------------------------------------------------
// Generic builder functions
// ---------------------------------------------------------------------------

/// Build a job for a plain `T` field (creates a default, syncs, sets the field).
fn build_pg_single_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut PostgresMetadata, T),
    touch_ts: fn(&mut PostgresMetadata),
) -> MetadataJob<PostgresAsync, PostgresMetadata>
where
    T: SyncCollector<PostgresAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut PostgresMetadata,
              ctx: PostgresAsync,
              telemetry: &mut TelemetryWrapper,
              capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, value);
                touch_ts(metadata);
                Ok(())
            })
        },
    )
}

/// Build a job for a `Vec<T>` field (creates a default, syncs, wraps in a one-element vec).
fn build_pg_vec_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut PostgresMetadata, Vec<T>),
    touch_ts: fn(&mut PostgresMetadata),
) -> MetadataJob<PostgresAsync, PostgresMetadata>
where
    T: SyncCollector<PostgresAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut PostgresMetadata,
              ctx: PostgresAsync,
              telemetry: &mut TelemetryWrapper,
              capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, vec![value]);
                touch_ts(metadata);
                Ok(())
            })
        },
    )
}

impl SyncMetadata<PostgresAsync> for PostgresMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<PostgresAsync, Self>> {
        // Thin field-wiring macros — only extract field references.
        // All business logic lives in build_pg_*_job().
        macro_rules! pg_single {
            ($name:expr, $field:ident, $type:ty, $freq:expr) => {
                paste::paste! {
                    build_pg_single_job::<$type>(
                        $name,
                        $freq,
                        |m, v| { m.$field = v },
                        |m| { m.last_sync_timestamps.[<$field _last_sync>] = PostgresMetadata::current_timestamp() },
                    )
                }
            };
        }

        macro_rules! pg_vec {
            ($name:expr, $field:ident, $type:ty, $freq:expr) => {
                paste::paste! {
                    build_pg_vec_job::<$type>(
                        $name,
                        $freq,
                        |m, v| { m.$field = v },
                        |m| { m.last_sync_timestamps.[<$field _last_sync>] = PostgresMetadata::current_timestamp() },
                    )
                }
            };
        }

        self.collection_timestamp = PostgresMetadata::current_timestamp();

        let mut jobs: Vec<MetadataJob<PostgresAsync, Self>> = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            let wal_requirements: Vec<CapabilityId> = vec![PG_VERSION_14.clone(), PG_ROLE_PRIMARY.clone()];
            jobs.extend([
                pg_single!("postgres.activity_info", activity_info, PostgresActivityInfo, SyncFrequency::High),
                pg_single!("postgres.connection_info", connection_info, PostgresConnectionInfo, SyncFrequency::High),
                pg_single!("postgres.lock_info", lock_info, PostgresLockInfo, SyncFrequency::High),
                pg_single!("postgres.performance_stats", performance_stats, PostgresPerformanceStats, SyncFrequency::High),
                pg_single!("postgres.replication_info", replication_info, PostgresReplicationInfo, SyncFrequency::High)
                    .with_requirement(PG_ROLE_PRIMARY.clone()),
                pg_single!("postgres.transaction_info", transaction_info, PostgresTransactionInfo, SyncFrequency::High),
                pg_single!("postgres.wal_info", wal_info, PostgresWalInfo, SyncFrequency::High).with_requirements(wal_requirements),
            ]);
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.extend([
                pg_single!("postgres.bgwriter_info", bgwriter_info, PostgresBgWriterInfo, SyncFrequency::Medium),
                pg_vec!("postgres.database_stats", database_stats, PostgresDatabaseStats, SyncFrequency::Medium),
                pg_vec!("postgres.index_info", index_info, PostgresIndexInfo, SyncFrequency::Medium),
                pg_vec!("postgres.table_info", table_info, PostgresTableInfo, SyncFrequency::Medium),
                pg_single!("postgres.vacuum_info", vacuum_info, PostgresVacuumInfo, SyncFrequency::Medium),
            ]);
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.extend([
                pg_vec!("postgres.extension_info", extension_info, PostgresExtensionInfo, SyncFrequency::Low),
                pg_single!("postgres.settings_info", settings_info, PostgresSettingsInfo, SyncFrequency::Low),
            ]);
            jobs.push(
                MetadataJob::new(
                    "postgres.schema_graph".to_string(),
                    SyncFrequency::Low,
                    move |metadata: &mut Self, ctx: PostgresAsync, telemetry, capabilities| {
                        Box::pin(async move {
                            let value = PostgresSchemaGraph::default().sync_metadata(ctx, telemetry, capabilities).await?;
                            metadata.schema_graph = Some(value);
                            Ok(())
                        })
                    },
                )
                .with_requirement(PG_VERSION_14.clone()),
            );
        }

        jobs
    }

    fn discover_capabilities<'a>(
        connection: PostgresAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match capabilities::PostgresCapabilities::discover(connection).await {
                Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                Err(e) => {
                    warn!("failed to discover PostgreSQL capabilities: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities)
                }
            }
        })
    }
}

impl PostgresMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }

    /// Interface to expose nested sync timestamps to other modules
    pub fn last_sync(&self) -> &PostgresLastSyncTimestamps {
        &self.last_sync_timestamps
    }

    pub fn new() -> Self {
        Self {
            activity_info: PostgresActivityInfo::default(),
            connection_info: PostgresConnectionInfo::default(),
            lock_info: PostgresLockInfo::default(),
            performance_stats: PostgresPerformanceStats::default(),
            replication_info: PostgresReplicationInfo::default(),
            transaction_info: PostgresTransactionInfo::default(),
            wal_info: PostgresWalInfo::default(),
            bgwriter_info: PostgresBgWriterInfo::default(),
            database_stats: Vec::new(),
            index_info: Vec::new(),
            table_info: Vec::new(),
            vacuum_info: PostgresVacuumInfo::default(),
            extension_info: Vec::new(),
            settings_info: PostgresSettingsInfo::default(),
            schema_graph: None,
            collection_timestamp: PostgresMetadata::current_timestamp(),
            last_sync_timestamps: PostgresLastSyncTimestamps::default(),
        }
    }
}
impl EpMetadata for PostgresMetadata {
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
        EpKind::Postgres
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

define_metadata_serializer_stuff!(EpKind::Postgres => PostgresMetadata);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn job_names(jobs: &[MetadataJob<PostgresAsync, PostgresMetadata>]) -> HashSet<&str> {
        jobs.iter().map(|job| job.name()).collect()
    }

    #[test]
    fn high_frequency_jobs_include_expected_collectors() {
        let mut metadata = PostgresMetadata { collection_timestamp: 0, ..Default::default() };

        let jobs = metadata.jobs(SyncFrequency::High);

        assert_eq!(jobs.len(), 7, "unexpected number of high-frequency jobs");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::High),
            "high-frequency job list contains mismatched frequencies",
        );
        assert!(metadata.collection_timestamp > 0, "collection timestamp should be refreshed");

        let names = job_names(&jobs);
        for expected in [
            "postgres.activity_info",
            "postgres.connection_info",
            "postgres.lock_info",
            "postgres.performance_stats",
            "postgres.replication_info",
            "postgres.transaction_info",
            "postgres.wal_info",
        ] {
            assert!(names.contains(expected), "expected job '{}' missing from high-frequency schedule", expected);
        }
    }

    #[test]
    fn medium_frequency_jobs_cover_operational_metrics() {
        let mut metadata = PostgresMetadata { collection_timestamp: 0, ..Default::default() };

        let jobs = metadata.jobs(SyncFrequency::Medium);

        assert_eq!(jobs.len(), 5, "unexpected number of medium-frequency jobs");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::Medium),
            "medium-frequency job list contains mismatched frequencies",
        );
        assert!(metadata.collection_timestamp > 0, "collection timestamp should be refreshed");

        let names = job_names(&jobs);
        for expected in [
            "postgres.bgwriter_info",
            "postgres.database_stats",
            "postgres.index_info",
            "postgres.table_info",
            "postgres.vacuum_info",
        ] {
            assert!(names.contains(expected), "expected job '{}' missing from medium-frequency schedule", expected);
        }
    }

    #[test]
    fn low_frequency_jobs_cover_configuration_collectors() {
        let mut metadata = PostgresMetadata { collection_timestamp: 0, ..Default::default() };

        let jobs = metadata.jobs(SyncFrequency::Low);

        assert_eq!(jobs.len(), 3, "unexpected number of low-frequency jobs");
        assert!(
            jobs.iter().all(|job| job.frequency() == SyncFrequency::Low),
            "low-frequency job list contains mismatched frequencies",
        );
        assert!(metadata.collection_timestamp > 0, "collection timestamp should be refreshed");

        let names = job_names(&jobs);
        for expected in ["postgres.extension_info", "postgres.settings_info", "postgres.schema_graph"] {
            assert!(names.contains(expected), "expected job '{}' missing from low-frequency schedule", expected);
        }
    }
}
