mod sync;

use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{EpMetadata, MetadataJob, SyncFrequency, SyncMetadata};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use {ep_core::define_metadata_serializer_stuff, weaviate_core::WeaviateAsync};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct WeaviateMetadata {}

impl WeaviateMetadata {
    pub fn new() -> Self {
        Self {}
    }
}

impl SyncMetadata<WeaviateAsync> for WeaviateMetadata {
    fn jobs(&mut self, _: SyncFrequency) -> Vec<MetadataJob<WeaviateAsync, Self>> {
        Vec::new()
    }
}

impl EpMetadata for WeaviateMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Weaviate
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

define_metadata_serializer_stuff!(EpKind::Weaviate => WeaviateMetadata);
