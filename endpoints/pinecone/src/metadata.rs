mod sync;

use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{EpMetadata, MetadataJob, SyncFrequency, SyncMetadata};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use {ep_core::define_metadata_serializer_stuff, pinecone_core::PineconeAsync};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct PineconeMetadata {}

impl PineconeMetadata {
    pub fn new() -> Self {
        Self {}
    }
}

impl SyncMetadata<PineconeAsync> for PineconeMetadata {
    fn jobs(&mut self, _: SyncFrequency) -> Vec<MetadataJob<PineconeAsync, Self>> {
        Vec::new()
    }
}

impl EpMetadata for PineconeMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Pinecone
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

define_metadata_serializer_stuff!(EpKind::Pinecone => PineconeMetadata);
