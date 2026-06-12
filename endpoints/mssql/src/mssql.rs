use crate::ep::EpMetadata;
use borsh::{BorshDeserialize, BorshSerialize};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug)]
pub struct MssqlMetadata {}

impl EpMetadata for MssqlMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Mssql
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
        self.clone_box()
    }

    fn to_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        borsh::to_writer(writer, self)
    }
}
