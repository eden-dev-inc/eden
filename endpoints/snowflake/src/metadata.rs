use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, EpMetadata, MetadataJob, SyncFrequency, SyncMetadata};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use {ep_core::define_metadata_serializer_stuff, snowflake_core::SnowflakeAsync};

/// Snowflake metadata with basic information.
///
/// Snowflake-specific metrics include:
/// - High: Warehouse status, query performance
/// - Medium: Database and schema information
/// - Low: Configuration settings
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SnowflakeMetadata {
    /// Warehouse information
    pub warehouse_info: SnowflakeWarehouseInfo,

    /// Database information
    pub databases: Vec<SnowflakeDatabaseInfo>,

    /// Query history summary
    pub query_info: SnowflakeQueryInfo,

    /// Collection metadata
    pub collection_timestamp: u64,
    pub last_sync_timestamps: SnowflakeLastSyncTimestamps,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SnowflakeWarehouseInfo {
    pub name: String,
    pub state: String,
    pub size: String,
    pub running_queries: u64,
    pub queued_queries: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SnowflakeDatabaseInfo {
    pub name: String,
    pub schema_count: u64,
    pub table_count: u64,
    pub created_on: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SnowflakeQueryInfo {
    pub total_queries: u64,
    pub running_queries: u64,
    pub avg_execution_time_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SnowflakeLastSyncTimestamps {
    pub warehouse_info_last_sync: u64,
    pub databases_last_sync: u64,
    pub query_info_last_sync: u64,
}

impl SyncMetadata<SnowflakeAsync> for SnowflakeMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<SnowflakeAsync, Self>> {
        self.collection_timestamp = SnowflakeMetadata::current_timestamp();

        let mut jobs: Vec<MetadataJob<SnowflakeAsync, Self>> = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.extend([MetadataJob::new(
                "snowflake.warehouse_info".to_string(),
                SyncFrequency::High,
                |metadata: &mut Self, _ctx: SnowflakeAsync, _telemetry, _capabilities: &dyn CapabilityChecker| {
                    Box::pin(async move {
                        // TODO: Implement warehouse info sync
                        metadata.last_sync_timestamps.warehouse_info_last_sync = SnowflakeMetadata::current_timestamp();
                        Ok(())
                    })
                },
            )]);
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.extend([MetadataJob::new(
                "snowflake.databases".to_string(),
                SyncFrequency::Medium,
                |metadata: &mut Self, _ctx: SnowflakeAsync, _telemetry, _capabilities: &dyn CapabilityChecker| {
                    Box::pin(async move {
                        // TODO: Implement database info sync
                        metadata.last_sync_timestamps.databases_last_sync = SnowflakeMetadata::current_timestamp();
                        Ok(())
                    })
                },
            )]);
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.extend([MetadataJob::new(
                "snowflake.query_info".to_string(),
                SyncFrequency::Low,
                |metadata: &mut Self, _ctx: SnowflakeAsync, _telemetry, _capabilities: &dyn CapabilityChecker| {
                    Box::pin(async move {
                        // TODO: Implement query info sync
                        metadata.last_sync_timestamps.query_info_last_sync = SnowflakeMetadata::current_timestamp();
                        Ok(())
                    })
                },
            )]);
        }

        jobs
    }
}

impl SnowflakeMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }

    /// Creates a new instance with default values
    pub fn new() -> Self {
        Self {
            warehouse_info: SnowflakeWarehouseInfo::default(),
            databases: Vec::new(),
            query_info: SnowflakeQueryInfo::default(),
            collection_timestamp: SnowflakeMetadata::current_timestamp(),
            last_sync_timestamps: SnowflakeLastSyncTimestamps::default(),
        }
    }
}

impl EpMetadata for SnowflakeMetadata {
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
        EpKind::Snowflake
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

define_metadata_serializer_stuff!(EpKind::Snowflake => SnowflakeMetadata);
