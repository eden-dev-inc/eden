use crate::ep::AzureAsync;
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, EpMetadata, MetadataJob, SyncFrequency, SyncMetadata, UnknownCapabilities};
use ep_core::define_metadata_serializer_stuff;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use telemetry::TelemetryWrapper;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct AzureMetadata {
    pub collection_timestamp: u64,
}

impl EpMetadata for AzureMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Azure
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

impl SyncMetadata<AzureAsync> for AzureMetadata {
    fn jobs(&mut self, _frequency: SyncFrequency) -> Vec<MetadataJob<AzureAsync, Self>> {
        vec![]
    }

    fn discover_capabilities<'a>(
        _connection: AzureAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move { Box::new(UnknownCapabilities) as Box<dyn CapabilityChecker> })
    }
}

define_metadata_serializer_stuff!(EpKind::Azure => AzureMetadata);
