pub mod capabilities;
pub mod stc;
pub mod sync;

use crate::metadata::{
    stc::{
        activity::ClickhouseActivityInfo, cluster::ClickhouseClusterInfo, connections::ClickhouseConnectionInfo,
        database::ClickhouseDatabaseStats, dictionaries::ClickhouseDictionaryInfo, merges::ClickhouseMergeInfo,
        mutations::ClickhouseMutationInfo, parts::ClickhousePartInfo, queries::ClickhouseQueryInfo, replication::ClickhouseReplicationInfo,
        settings::ClickhouseSettingsInfo, storage::ClickhouseStorageInfo, tables::ClickhouseTableInfo, zookeeper::ClickhouseZooKeeperInfo,
    },
    sync::ClickhouseLastSyncTimestamps,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{
    CapabilityChecker, EpMetadata, MetadataJob, SyncCollector, SyncFrequency, SyncMetadata, UnknownCapabilities,
};
use error::ResultEP;
use format::endpoint::EpKind;
use log::warn;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use telemetry::TelemetryWrapper;
use {clickhouse_core::ClickhouseAsync, ep_core::define_metadata_serializer_stuff};

macro_rules! impl_sync_collector_ch {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<ClickhouseAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: ClickhouseAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_ch!(
    ClickhouseActivityInfo,
    ClickhouseClusterInfo,
    ClickhouseConnectionInfo,
    ClickhouseDatabaseStats,
    ClickhouseDictionaryInfo,
    ClickhouseMergeInfo,
    ClickhouseMutationInfo,
    ClickhousePartInfo,
    ClickhouseQueryInfo,
    ClickhouseReplicationInfo,
    ClickhouseSettingsInfo,
    ClickhouseStorageInfo,
    ClickhouseTableInfo,
    ClickhouseZooKeeperInfo,
);

/// Build a job for a plain `T` field: create a default, sync and set the field.
fn build_ch_single_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut ClickhouseMetadata, T),
    touch_ts: fn(&mut ClickhouseMetadata),
) -> MetadataJob<ClickhouseAsync, ClickhouseMetadata>
where
    T: SyncCollector<ClickhouseAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut ClickhouseMetadata,
              ctx: ClickhouseAsync,
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

/// Build a job for a `Vec<T>` field: create a default, sync and wrap in a one-element vec.
fn build_ch_vec_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut ClickhouseMetadata, Vec<T>),
    touch_ts: fn(&mut ClickhouseMetadata),
) -> MetadataJob<ClickhouseAsync, ClickhouseMetadata>
where
    T: SyncCollector<ClickhouseAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut ClickhouseMetadata,
              ctx: ClickhouseAsync,
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

/// Enhanced ClickhouseMetadata with sync interval support
///
/// This organizes Clickhouse-specific metrics by priority:
/// - High: Critical performance, query execution, and cluster health
/// - Medium: Operational metrics for maintenance and optimization
/// - Low: Configuration and static information
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMetadata {
    // High priority - critical performance and cluster health
    pub activity_info: ClickhouseActivityInfo,       // current queries, processes
    pub connection_info: ClickhouseConnectionInfo,   // connection pool status
    pub query_info: ClickhouseQueryInfo,             // query performance, slow queries
    pub cluster_info: ClickhouseClusterInfo,         // cluster topology, shard health
    pub replication_info: ClickhouseReplicationInfo, // replica status, replication lag
    pub storage_info: ClickhouseStorageInfo,         // disk usage, storage metrics
    pub zookeeper_info: ClickhouseZooKeeperInfo,     // ZooKeeper connectivity, coordination

    // Medium priority - operational metrics for maintenance
    pub merge_info: ClickhouseMergeInfo,              // merge tree operations
    pub mutation_info: ClickhouseMutationInfo,        // ALTER table mutations
    pub part_info: Vec<ClickhousePartInfo>,           // table parts, merges needed
    pub database_stats: Vec<ClickhouseDatabaseStats>, // per-database statistics
    pub table_info: Vec<ClickhouseTableInfo>,         // table statistics, compression

    // Low priority - configuration and static info
    pub dictionary_info: Vec<ClickhouseDictionaryInfo>, // external dictionaries
    pub settings_info: ClickhouseSettingsInfo,          // configuration parameters

    // Collection metadata
    pub collection_timestamp: u64,
    pub last_sync_timestamps: ClickhouseLastSyncTimestamps,
}

impl SyncMetadata<ClickhouseAsync> for ClickhouseMetadata {
    fn discover_capabilities<'a>(
        connection: ClickhouseAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match capabilities::ClickhouseCapabilities::discover(connection).await {
                Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                Err(e) => {
                    warn!("failed to discover ClickHouse capabilities: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities) as Box<dyn CapabilityChecker>
                }
            }
        })
    }

    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<ClickhouseAsync, Self>> {
        self.collection_timestamp = ClickhouseMetadata::current_timestamp();

        macro_rules! ch_single {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_ch_single_job($name, $freq, |m, v| m.$field = v, |m| m.last_sync_timestamps.$ts_field = Self::current_timestamp())
            };
        }
        macro_rules! ch_vec {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_ch_vec_job($name, $freq, |m, v| m.$field = v, |m| m.last_sync_timestamps.$ts_field = Self::current_timestamp())
            };
        }

        let mut jobs = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.extend([
                ch_single!("clickhouse.activity_info", SyncFrequency::High, activity_info, activity_info_last_sync),
                ch_single!("clickhouse.connection_info", SyncFrequency::High, connection_info, connection_info_last_sync),
                ch_single!("clickhouse.query_info", SyncFrequency::High, query_info, query_info_last_sync),
                ch_single!("clickhouse.cluster_info", SyncFrequency::High, cluster_info, cluster_info_last_sync)
                    .with_requirement(capabilities::CLICKHOUSE_HAS_CLUSTERS),
                ch_single!("clickhouse.replication_info", SyncFrequency::High, replication_info, replication_info_last_sync)
                    .with_requirement(capabilities::CLICKHOUSE_HAS_REPLICATION),
                ch_single!("clickhouse.storage_info", SyncFrequency::High, storage_info, storage_info_last_sync),
                ch_single!("clickhouse.zookeeper_info", SyncFrequency::High, zookeeper_info, zookeeper_info_last_sync)
                    .with_requirement(capabilities::CLICKHOUSE_HAS_ZOOKEEPER),
            ]);
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.extend([
                ch_single!("clickhouse.merge_info", SyncFrequency::Medium, merge_info, merge_info_last_sync),
                ch_single!("clickhouse.mutation_info", SyncFrequency::Medium, mutation_info, mutation_info_last_sync),
                ch_vec!("clickhouse.part_info", SyncFrequency::Medium, part_info, part_info_last_sync),
                ch_vec!("clickhouse.database_stats", SyncFrequency::Medium, database_stats, database_stats_last_sync),
                ch_vec!("clickhouse.table_info", SyncFrequency::Medium, table_info, table_info_last_sync),
            ]);
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.extend([
                ch_vec!("clickhouse.dictionary_info", SyncFrequency::Low, dictionary_info, dictionary_info_last_sync)
                    .with_requirement(capabilities::CLICKHOUSE_HAS_DICTIONARIES),
                ch_single!("clickhouse.settings_info", SyncFrequency::Low, settings_info, settings_info_last_sync),
            ]);
        }

        jobs
    }
}

impl ClickhouseMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }
}

impl EpMetadata for ClickhouseMetadata {
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
        EpKind::Clickhouse
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

define_metadata_serializer_stuff!(EpKind::Clickhouse => ClickhouseMetadata);
