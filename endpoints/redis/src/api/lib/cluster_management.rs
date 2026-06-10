use crate::api::value::RedisJsonValue;
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod asking;
mod cluster_addslots;
mod cluster_addslotsrange;
mod cluster_bumpepoch;
mod cluster_count_failure_reports;
mod cluster_countkeysinslot;
mod cluster_delslots;
mod cluster_delslotsrange;
mod cluster_failover;
mod cluster_flushslots;
mod cluster_forget;
mod cluster_getkeysinslot;
mod cluster_info;
mod cluster_keyslot;
mod cluster_links;
mod cluster_meet;
mod cluster_myid;
mod cluster_myshardid;
mod cluster_nodes;
mod cluster_replicas;
mod cluster_replicate;
mod cluster_reset;
mod cluster_saveconfig;
mod cluster_set_config_epoch;
mod cluster_setslot;
mod cluster_shards;
mod cluster_slaves;
mod cluster_slots;
mod readonly;
mod readwrite;

pub use asking::*;
pub use cluster_addslots::*;
pub use cluster_addslotsrange::*;
pub use cluster_bumpepoch::*;
pub use cluster_count_failure_reports::*;
pub use cluster_countkeysinslot::*;
pub use cluster_delslots::*;
pub use cluster_delslotsrange::*;
pub use cluster_failover::*;
pub use cluster_flushslots::*;
pub use cluster_forget::*;
pub use cluster_getkeysinslot::*;
pub use cluster_info::*;
pub use cluster_keyslot::*;
pub use cluster_links::*;
pub use cluster_meet::*;
pub use cluster_myid::*;
pub use cluster_myshardid::*;
pub use cluster_nodes::*;
pub use cluster_replicas::*;
pub use cluster_replicate::*;
pub use cluster_reset::*;
pub use cluster_saveconfig::*;
pub use cluster_set_config_epoch::*;
pub use cluster_setslot::*;
pub use cluster_shards::*;
pub use cluster_slaves::*;
pub use cluster_slots::*;
pub use readonly::*;
pub use readwrite::*;

/// A slot range with start and end values (inclusive)
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, Builder, ToSchema, JsonSchema)]
pub struct Slot {
    /// Start of slot range (0-16383)
    start: RedisJsonValue,
    /// End of slot range (inclusive, 0-16383)
    end: RedisJsonValue,
}

impl Slot {
    pub fn new(start: RedisJsonValue, end: RedisJsonValue) -> Self {
        Self { start, end }
    }

    pub fn start(&self) -> &RedisJsonValue {
        &self.start
    }

    pub fn end(&self) -> &RedisJsonValue {
        &self.end
    }
}

/// Result of CLUSTER BUMPEPOCH command
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum BumpepochResult {
    /// Epoch was incremented ("BUMPED")
    Bumped,
    /// Epoch was not changed ("STILL")
    Still,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum Failover {
    FORCE,
    TAKEOVER,
}

/// Represents a single cluster link
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterLink {
    /// Direction of the link ("to" or "from")
    pub direction: String,
    /// Node ID of the peer
    pub node: String,
    /// Creation time of the link (Unix timestamp in milliseconds)
    pub create_time: i64,
    /// Events being monitored (e.g., "rw")
    pub events: String,
    /// Bytes sent on this link
    pub send_buffer_allocated: i64,
    /// Bytes in send buffer
    pub send_buffer_used: i64,
}

/// Represents a single node in the cluster
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterNode {
    /// The node ID (40-character hex string)
    pub node_id: String,
    /// The node's address (ip:port@cport)
    pub address: String,
    /// Node flags (e.g., "master", "slave", "myself")
    pub flags: Vec<String>,
    /// Master node ID if this is a replica, "-" otherwise
    pub master_id: Option<String>,
    /// Ping sent timestamp
    pub ping_sent: i64,
    /// Pong received timestamp
    pub pong_recv: i64,
    /// Config epoch
    pub config_epoch: i64,
    /// Link state ("connected" or "disconnected")
    pub link_state: String,
    /// Slot ranges served by this node (for masters)
    pub slots: Vec<String>,
}

/// Represents a replica node info line
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ReplicaInfo {
    /// The node ID (40-character hex string)
    pub node_id: String,
    /// The node's address (ip:port@cport)
    pub address: String,
    /// Node flags
    pub flags: Vec<String>,
    /// Master node ID
    pub master_id: String,
    /// Ping sent timestamp
    pub ping_sent: i64,
    /// Pong received timestamp
    pub pong_recv: i64,
    /// Config epoch
    pub config_epoch: i64,
    /// Link state
    pub link_state: String,
}

impl Serialize for ReplicaInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ReplicaInfo", 8)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.serialize_field("address", &self.address)?;
        state.serialize_field("flags", &self.flags)?;
        state.serialize_field("master_id", &self.master_id)?;
        state.serialize_field("ping_sent", &self.ping_sent)?;
        state.serialize_field("pong_recv", &self.pong_recv)?;
        state.serialize_field("config_epoch", &self.config_epoch)?;
        state.serialize_field("link_state", &self.link_state)?;
        state.end()
    }
}

/// Reset mode for CLUSTER RESET command
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, ToSchema, JsonSchema,
)]
pub enum Reset {
    /// Hard reset: flushes data and resets cluster state completely
    HARD,
    /// Soft reset: only resets cluster state, preserves data
    #[default]
    SOFT,
}

/// Subcommand for CLUSTER SETSLOT
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum SetslotSubcommand {
    /// Set slot to importing state from specified node
    Importing(RedisJsonValue),
    /// Set slot to migrating state to specified node
    Migrating(RedisJsonValue),
    /// Bind slot to specified node
    Node(RedisJsonValue),
    /// Clear importing/migrating state
    Stable,
}
