pub mod capabilities;
pub mod stc;
pub mod sync;

use crate::metadata::stc::parameters::OracleParametersCollection;
use crate::metadata::{
    stc::{
        activity::OracleActivityInfo, connections::OracleConnectionInfo, database::OracleDatabaseStats, indexes::OracleIndexInfo,
        locks::OracleLockInfo, performance::OraclePerformanceStatsCollection, redolog::OracleRedoLogInfo, segments::OracleSegmentInfo,
        sessions::OracleSessionInfo, storage::OracleStorageInfo, tables::OracleTableInfo, tablespaces::OracleTablespaceInfo,
        transactions::OracleTransactionInfo, wait_events::OracleWaitEventInfo,
    },
    sync::OracleLastSyncTimestamps,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{
    CapabilityChecker, EpMetadata, MetadataJob, SyncCollector, SyncFrequency, SyncMetadata, UnknownCapabilities,
};
use ep_core::define_metadata_serializer_stuff;
use error::ResultEP;
use format::endpoint::EpKind;
use log::warn;
use oracle_core::OracleAsync;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use telemetry::TelemetryWrapper;

macro_rules! impl_sync_collector_oracle {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<OracleAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: OracleAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_oracle!(
    OracleActivityInfo,
    OracleConnectionInfo,
    OracleDatabaseStats,
    OracleIndexInfo,
    OracleLockInfo,
    OracleParametersCollection,
    OraclePerformanceStatsCollection,
    OracleRedoLogInfo,
    OracleSegmentInfo,
    OracleSessionInfo,
    OracleStorageInfo,
    OracleTableInfo,
    OracleTablespaceInfo,
    OracleTransactionInfo,
    OracleWaitEventInfo,
);

/// Enhanced OracleMetadata with sync interval support
///
/// This provides comprehensive Oracle database monitoring with
/// per-category sync tracking and configuration organized by priority.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleMetadata {
    // high priority - critical performance and connection metrics
    pub activity_info: OracleActivityInfo,                   // current SQL execution, blocking sessions
    pub connection_info: OracleConnectionInfo,               // connection pool status, session limits
    pub lock_info: OracleLockInfo,                           // blocking locks, deadlocks
    pub performance_stats: OraclePerformanceStatsCollection, // AWR stats, buffer cache hits, SQL performance
    pub session_info: OracleSessionInfo,                     // active sessions, resource usage
    pub transaction_info: OracleTransactionInfo,             // transaction stats, undo usage
    pub wait_events: OracleWaitEventInfo,                    // wait events, bottlenecks

    // medium priority - operational metrics
    pub database_stats: Vec<OracleDatabaseStats>,   // per-database statistics
    pub index_info: Vec<OracleIndexInfo>,           // index usage, rebuilds needed
    pub redolog_info: OracleRedoLogInfo,            // redo log switches, archiving
    pub segment_info: Vec<OracleSegmentInfo>,       // segment growth, space usage
    pub storage_info: OracleStorageInfo,            // datafile usage, temp space
    pub table_info: Vec<OracleTableInfo>,           // table statistics, growth
    pub tablespace_info: Vec<OracleTablespaceInfo>, // tablespace usage, autoextend

    // low priority - configuration and static info
    pub parameter_info: OracleParametersCollection, // database parameters, configuration

    // collection metadata
    pub collection_timestamp: u64,
    pub last_sync_timestamps: OracleLastSyncTimestamps,
}

fn build_oracle_single_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut OracleMetadata, T),
    touch_ts: fn(&mut OracleMetadata),
) -> MetadataJob<OracleAsync, OracleMetadata>
where
    T: SyncCollector<OracleAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut OracleMetadata, ctx: OracleAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, value);
                touch_ts(metadata);
                Ok(())
            })
        },
    )
}

fn build_oracle_vec_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut OracleMetadata, Vec<T>),
    touch_ts: fn(&mut OracleMetadata),
) -> MetadataJob<OracleAsync, OracleMetadata>
where
    T: SyncCollector<OracleAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut OracleMetadata, ctx: OracleAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, vec![value]);
                touch_ts(metadata);
                Ok(())
            })
        },
    )
}

impl SyncMetadata<OracleAsync> for OracleMetadata {
    fn discover_capabilities<'a>(
        connection: OracleAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match capabilities::OracleCapabilities::discover(connection).await {
                Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                Err(e) => {
                    warn!("failed to discover Oracle capabilities: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities) as Box<dyn CapabilityChecker>
                }
            }
        })
    }

    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<OracleAsync, Self>> {
        self.collection_timestamp = Self::current_timestamp();

        macro_rules! oracle_single {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_oracle_single_job($name, $freq, |m, v| m.$field = v, |m| m.last_sync_timestamps.$ts_field = Self::current_timestamp())
            };
        }
        macro_rules! oracle_vec {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_oracle_vec_job($name, $freq, |m, v| m.$field = v, |m| m.last_sync_timestamps.$ts_field = Self::current_timestamp())
            };
        }

        let mut jobs = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.extend([
                oracle_single!("oracle.activity_info", SyncFrequency::High, activity_info, activity_info_last_sync),
                oracle_single!("oracle.connection_info", SyncFrequency::High, connection_info, connection_info_last_sync),
                oracle_single!("oracle.lock_info", SyncFrequency::High, lock_info, lock_info_last_sync),
                oracle_single!("oracle.performance_stats", SyncFrequency::High, performance_stats, performance_stats_last_sync),
                oracle_single!("oracle.session_info", SyncFrequency::High, session_info, session_info_last_sync),
                oracle_single!("oracle.transaction_info", SyncFrequency::High, transaction_info, transaction_info_last_sync),
                oracle_single!("oracle.wait_events", SyncFrequency::High, wait_events, wait_events_last_sync),
            ]);
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.extend([
                oracle_vec!("oracle.database_stats", SyncFrequency::Medium, database_stats, database_stats_last_sync),
                oracle_vec!("oracle.index_info", SyncFrequency::Medium, index_info, index_info_last_sync)
                    .with_requirement(capabilities::ORACLE_HAS_DBA_VIEWS),
                oracle_single!("oracle.redolog_info", SyncFrequency::Medium, redolog_info, redolog_info_last_sync),
                oracle_vec!("oracle.segment_info", SyncFrequency::Medium, segment_info, segment_info_last_sync)
                    .with_requirement(capabilities::ORACLE_HAS_DBA_VIEWS),
                oracle_single!("oracle.storage_info", SyncFrequency::Medium, storage_info, storage_info_last_sync)
                    .with_requirement(capabilities::ORACLE_HAS_DBA_VIEWS),
                oracle_vec!("oracle.table_info", SyncFrequency::Medium, table_info, table_info_last_sync)
                    .with_requirement(capabilities::ORACLE_HAS_DBA_VIEWS),
                oracle_vec!("oracle.tablespace_info", SyncFrequency::Medium, tablespace_info, tablespace_info_last_sync)
                    .with_requirement(capabilities::ORACLE_HAS_DBA_VIEWS),
            ]);
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.push(oracle_single!(
                "oracle.parameter_info",
                SyncFrequency::Low,
                parameter_info,
                parameter_info_last_sync
            ));
        }

        jobs
    }
}

impl OracleMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }
}

impl EpMetadata for OracleMetadata {
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
        EpKind::Oracle
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

define_metadata_serializer_stuff!(EpKind::Oracle => OracleMetadata);

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_metadata_creation() {
        let metadata = OracleMetadata {
            collection_timestamp: OracleMetadata::current_timestamp(),
            ..OracleMetadata::default()
        };
        assert_eq!(metadata.kind(), EpKind::Oracle);
        assert!(metadata.collection_timestamp > 0);
    }

    #[test]
    fn test_oracle_metadata_default() {
        let metadata = OracleMetadata::default();
        assert_eq!(metadata.kind(), EpKind::Oracle);
        assert_eq!(metadata.collection_timestamp, 0);
    }

    #[test]
    fn test_oracle_metadata_clone() {
        let metadata = OracleMetadata {
            collection_timestamp: OracleMetadata::current_timestamp(),
            ..OracleMetadata::default()
        };
        let cloned = metadata.clone();
        assert_eq!(metadata.kind(), cloned.kind());
        assert_eq!(metadata.collection_timestamp, cloned.collection_timestamp);
    }
}
