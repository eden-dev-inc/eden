#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Communication Types
//!
//! Node communication and coordination data structures.
//!
//! ## Overview
//!
//! Defines types for inter-node communication:
//! - Node identity and metadata
//! - Disconnect operations
//! - gRPC communication helpers

use format::{EdenNodeId, EdenNodeUuid};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// pub mod connect;
pub mod disconnect;
mod grpc;
// pub mod request;
// pub mod transaction;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NodeData {
    id: EdenNodeId,
    uuid: EdenNodeUuid,
}

impl NodeData {
    pub fn new(eden_node_id: EdenNodeId, eden_node_uuid: EdenNodeUuid) -> Self {
        NodeData { id: eden_node_id, uuid: eden_node_uuid }
    }
    pub fn id(&self) -> &EdenNodeId {
        &self.id
    }
    pub fn uuid(&self) -> &EdenNodeUuid {
        &self.uuid
    }
}
