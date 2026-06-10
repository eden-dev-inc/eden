use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod latency_doctor;
mod latency_graph;
mod latency_histogram;
mod latency_history;
mod latency_latest;
mod latency_reset;

pub use latency_doctor::*;
pub use latency_graph::*;
pub use latency_histogram::*;
pub use latency_history::*;
pub use latency_latest::*;
pub use latency_reset::*;

/// A single latency event sample
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, PartialEq)]
pub struct LatencyEvent {
    /// The event name (e.g., "command", "fork", "expire-cycle")
    pub name: String,
    /// Unix timestamp of the latest latency spike
    pub timestamp: i64,
    /// Latest latency in milliseconds
    pub latest_latency_ms: i64,
    /// Maximum latency ever recorded for this event in milliseconds
    pub max_latency_ms: i64,
}

impl Serialize for LatencyEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyEvent", 4)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("latest_latency_ms", &self.latest_latency_ms)?;
        state.serialize_field("max_latency_ms", &self.max_latency_ms)?;
        state.end()
    }
}
