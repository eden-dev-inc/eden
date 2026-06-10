pub mod capabilities;
pub mod stc;
pub mod sync;

use crate::metadata::{
    stc::{
        cluster::CassandraClusterInfo, compaction::CassandraCompactionInfo, keyspaces::CassandraKeyspaceInfo, nodes::CassandraNodeInfo,
        repair::CassandraRepairInfo, schema::CassandraSchemaInfo, snapshots::CassandraSnapshotInfo, tables::CassandraTableInfo,
        threadpools::CassandraThreadPoolInfo, tombstones::CassandraTombstoneInfo,
    },
    sync::CassandraLastSyncTimestamps,
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
use serde_json::Value;
use std::convert::TryInto;
use std::{any::Any, io};
use telemetry::TelemetryWrapper;
use {cassandra_core::CassandraAsync, ep_core::define_metadata_serializer_stuff};

macro_rules! impl_sync_collector_cass {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<CassandraAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: CassandraAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_cass!(
    CassandraClusterInfo,
    CassandraCompactionInfo,
    CassandraKeyspaceInfo,
    CassandraNodeInfo,
    CassandraRepairInfo,
    CassandraSchemaInfo,
    CassandraSnapshotInfo,
    CassandraTableInfo,
    CassandraThreadPoolInfo,
    CassandraTombstoneInfo,
);

/// Enhanced CassandraMetadata with sync interval support
///
/// This organizes Cassandra metrics by priority levels:
/// - High priority: Critical cluster health and performance metrics
/// - Medium priority: Operational metrics and table statistics
/// - Low priority: Configuration and static information
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraMetadata {
    // high priority
    pub cluster_info: CassandraClusterInfo,       // cluster status, node availability
    pub node_info: Vec<CassandraNodeInfo>,        // per-node health and performance
    pub threadpool_info: CassandraThreadPoolInfo, // thread pool statistics

    // medium priority
    pub compaction_info: CassandraCompactionInfo,  // compaction status and metrics
    pub repair_info: CassandraRepairInfo,          // repair status and history
    pub tombstone_info: CassandraTombstoneInfo,    // tombstone warnings and counts
    pub keyspace_info: Vec<CassandraKeyspaceInfo>, // per-keyspace statistics
    pub table_info: Vec<CassandraTableInfo>,       // table statistics and metrics
    pub snapshot_info: CassandraSnapshotInfo,      // snapshot status and management

    // low priority - configuration and static info
    pub schema_info: CassandraSchemaInfo, // schema definitions and versions

    // collection metadata
    pub collection_timestamp: u64,
    pub last_sync_timestamps: CassandraLastSyncTimestamps,
}

/// Build a job for a plain `T` field: create a default, sync and set the field.
fn build_cass_single_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut CassandraMetadata, T),
    touch_ts: fn(&mut CassandraMetadata),
) -> MetadataJob<CassandraAsync, CassandraMetadata>
where
    T: SyncCollector<CassandraAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut CassandraMetadata,
              ctx: CassandraAsync,
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
fn build_cass_vec_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut CassandraMetadata, Vec<T>),
    touch_ts: fn(&mut CassandraMetadata),
) -> MetadataJob<CassandraAsync, CassandraMetadata>
where
    T: SyncCollector<CassandraAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut CassandraMetadata,
              ctx: CassandraAsync,
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

impl SyncMetadata<CassandraAsync> for CassandraMetadata {
    fn discover_capabilities<'a>(
        connection: CassandraAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match capabilities::CassandraCapabilities::discover(connection).await {
                Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                Err(e) => {
                    warn!("failed to discover Cassandra capabilities: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities) as Box<dyn CapabilityChecker>
                }
            }
        })
    }

    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<CassandraAsync, Self>> {
        self.collection_timestamp = Self::current_timestamp();

        macro_rules! cass_single {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_cass_single_job($name, $freq, |m, v| m.$field = v, |m| m.last_sync_timestamps.$ts_field = Self::current_timestamp())
            };
        }
        macro_rules! cass_vec {
            ($name:expr, $freq:expr, $field:ident, $ts_field:ident) => {
                build_cass_vec_job($name, $freq, |m, v| m.$field = v, |m| m.last_sync_timestamps.$ts_field = Self::current_timestamp())
            };
        }

        let mut jobs = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.extend([
                cass_single!("cassandra.cluster_info", SyncFrequency::High, cluster_info, cluster_info_last_sync),
                cass_vec!("cassandra.node_info", SyncFrequency::High, node_info, node_info_last_sync),
                cass_single!("cassandra.threadpool_info", SyncFrequency::High, threadpool_info, threadpool_info_last_sync),
            ]);
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.extend([
                cass_single!("cassandra.compaction_info", SyncFrequency::Medium, compaction_info, compaction_info_last_sync)
                    .with_requirement(capabilities::CASSANDRA_HAS_COMPACTION_HISTORY),
                cass_single!("cassandra.repair_info", SyncFrequency::Medium, repair_info, repair_info_last_sync)
                    .with_requirement(capabilities::CASSANDRA_HAS_COMPACTION_HISTORY),
                cass_single!("cassandra.tombstone_info", SyncFrequency::Medium, tombstone_info, tombstone_info_last_sync),
                cass_vec!("cassandra.keyspace_info", SyncFrequency::Medium, keyspace_info, keyspace_info_last_sync),
                cass_vec!("cassandra.table_info", SyncFrequency::Medium, table_info, table_info_last_sync)
                    .with_requirement(capabilities::CASSANDRA_HAS_SIZE_ESTIMATES),
                cass_single!("cassandra.snapshot_info", SyncFrequency::Medium, snapshot_info, snapshot_info_last_sync),
            ]);
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.push(cass_single!("cassandra.schema_info", SyncFrequency::Low, schema_info, schema_info_last_sync));
        }

        jobs
    }
}

impl CassandraMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }
}

impl EpMetadata for CassandraMetadata {
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
        EpKind::Cassandra
    }
    fn clone_box(&self) -> Box<dyn EpMetadata> {
        Box::new(self.clone())
    }

    fn to_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        borsh::to_writer(writer, self)
    }
}

define_metadata_serializer_stuff!(EpKind::Cassandra => CassandraMetadata);
