use crate::ep::DatabricksAsync;
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, EpMetadata, MetadataJob, SyncFrequency, SyncMetadata};
use ep_core::define_metadata_serializer_stuff;
use error::EpError;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DatabricksMetadata {
    pub collection_timestamp: u64,
    pub last_sync_timestamps: DatabricksLastSyncTimestamps,
    pub warehouse: DatabricksWarehouseMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DatabricksLastSyncTimestamps {
    pub warehouse_info_last_sync: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DatabricksWarehouseMetadata {
    pub id: String,
    pub name: String,
    pub state: String,
    pub cluster_size: Option<String>,
    pub min_num_clusters: Option<u32>,
    pub max_num_clusters: Option<u32>,
    pub num_clusters: Option<u32>,
    pub num_active_sessions: Option<u64>,
    pub auto_stop_mins: Option<u32>,
    pub warehouse_type: Option<String>,
    pub enable_serverless_compute: Option<bool>,
}

impl SyncMetadata<DatabricksAsync> for DatabricksMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<DatabricksAsync, Self>> {
        self.collection_timestamp = DatabricksMetadata::current_timestamp();

        let mut jobs: Vec<MetadataJob<DatabricksAsync, Self>> = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.extend([MetadataJob::new(
                "databricks.warehouse_info".to_string(),
                SyncFrequency::High,
                |metadata: &mut Self, ctx: DatabricksAsync, _telemetry, _capabilities: &dyn CapabilityChecker| {
                    Box::pin(async move {
                        let client = ctx.get().await.map_err(EpError::connect)?;
                        let info = client.get_warehouse_info().await?;

                        metadata.warehouse = DatabricksWarehouseMetadata {
                            id: info.id,
                            name: info.name,
                            state: info.state,
                            cluster_size: info.cluster_size,
                            min_num_clusters: info.min_num_clusters,
                            max_num_clusters: info.max_num_clusters,
                            num_clusters: info.num_clusters,
                            num_active_sessions: info.num_active_sessions,
                            auto_stop_mins: info.auto_stop_mins,
                            warehouse_type: info.warehouse_type,
                            enable_serverless_compute: info.enable_serverless_compute,
                        };
                        metadata.last_sync_timestamps.warehouse_info_last_sync = DatabricksMetadata::current_timestamp();
                        Ok(())
                    })
                },
            )]);
        }

        jobs
    }
}

impl DatabricksMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }

    pub fn new() -> Self {
        Self {
            collection_timestamp: DatabricksMetadata::current_timestamp(),
            ..Default::default()
        }
    }
}

impl EpMetadata for DatabricksMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Databricks
    }
    fn as_metadata(self: Box<Self>) -> Box<dyn EpMetadata> {
        self
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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

define_metadata_serializer_stuff!(EpKind::Databricks => DatabricksMetadata);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_frequency_jobs_include_warehouse_info() {
        let mut metadata = DatabricksMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::High);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name(), "databricks.warehouse_info");
    }

    #[test]
    fn medium_frequency_returns_no_jobs() {
        let mut metadata = DatabricksMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::Medium);
        assert!(jobs.is_empty());
    }

    #[test]
    fn low_frequency_returns_no_jobs() {
        let mut metadata = DatabricksMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::Low);
        assert!(jobs.is_empty());
    }

    #[test]
    fn metadata_kind_is_databricks() {
        let metadata = DatabricksMetadata::new();
        assert_eq!(metadata.kind(), EpKind::Databricks);
    }

    #[test]
    fn metadata_new_sets_timestamp() {
        let metadata = DatabricksMetadata::new();
        assert!(metadata.collection_timestamp > 0);
    }

    #[test]
    fn metadata_serde_roundtrip() {
        let metadata = DatabricksMetadata::new();
        let json = serde_json::to_value(&metadata).expect("Failed to serialize");
        let deserialized: DatabricksMetadata = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(deserialized.collection_timestamp, metadata.collection_timestamp);
    }

    #[test]
    fn metadata_borsh_roundtrip() {
        let metadata = DatabricksMetadata::new();
        let bytes = borsh::to_vec(&metadata).expect("Failed to borsh serialize");
        let deserialized: DatabricksMetadata = borsh::from_slice(&bytes).expect("Failed to borsh deserialize");
        assert_eq!(deserialized.collection_timestamp, metadata.collection_timestamp);
    }

    #[test]
    fn warehouse_metadata_defaults() {
        let wh = DatabricksWarehouseMetadata::default();
        assert!(wh.id.is_empty());
        assert!(wh.name.is_empty());
        assert!(wh.state.is_empty());
        assert!(wh.cluster_size.is_none());
        assert!(wh.num_clusters.is_none());
    }

    #[test]
    fn metadata_with_warehouse_serde_roundtrip() {
        let mut metadata = DatabricksMetadata::new();
        metadata.warehouse = DatabricksWarehouseMetadata {
            id: "wh-abc".to_string(),
            name: "Production".to_string(),
            state: "RUNNING".to_string(),
            cluster_size: Some("Large".to_string()),
            min_num_clusters: Some(1),
            max_num_clusters: Some(4),
            num_clusters: Some(2),
            num_active_sessions: Some(10),
            auto_stop_mins: Some(30),
            warehouse_type: Some("PRO".to_string()),
            enable_serverless_compute: Some(true),
        };

        let json = serde_json::to_value(&metadata).expect("Failed to serialize");
        let deserialized: DatabricksMetadata = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(deserialized.warehouse.id, "wh-abc");
        assert_eq!(deserialized.warehouse.name, "Production");
        assert_eq!(deserialized.warehouse.state, "RUNNING");
        assert_eq!(deserialized.warehouse.num_clusters, Some(2));
        assert_eq!(deserialized.warehouse.warehouse_type, Some("PRO".to_string()));
    }

    #[test]
    fn metadata_with_warehouse_borsh_roundtrip() {
        let mut metadata = DatabricksMetadata::new();
        metadata.warehouse = DatabricksWarehouseMetadata {
            id: "wh-123".to_string(),
            name: "Test".to_string(),
            state: "STOPPED".to_string(),
            cluster_size: None,
            min_num_clusters: None,
            max_num_clusters: None,
            num_clusters: None,
            num_active_sessions: None,
            auto_stop_mins: Some(15),
            warehouse_type: Some("CLASSIC".to_string()),
            enable_serverless_compute: Some(false),
        };

        let bytes = borsh::to_vec(&metadata).expect("Failed to borsh serialize");
        let deserialized: DatabricksMetadata = borsh::from_slice(&bytes).expect("Failed to borsh deserialize");
        assert_eq!(deserialized.warehouse.id, "wh-123");
        assert_eq!(deserialized.warehouse.state, "STOPPED");
        assert_eq!(deserialized.warehouse.auto_stop_mins, Some(15));
    }
}
