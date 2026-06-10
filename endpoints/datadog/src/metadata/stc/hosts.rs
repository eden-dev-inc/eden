use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use telemetry::TelemetryWrapper;

use crate::ep::DatadogAsync;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DatadogHostInfo {
    pub total_hosts: u64,
    pub up_hosts: u64,
    pub host_names: Vec<String>,
}

impl MetadataCollection for DatadogHostInfo {
    type Request = ();

    fn request(&self) -> Self::Request {}

    fn description(&self) -> &'static str {
        "Collect Datadog host infrastructure information"
    }

    fn category(&self) -> &'static str {
        "hosts"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl DatadogHostInfo {
    pub(crate) async fn sync_metadata(
        &self,
        context: DatadogAsync,
        _telemetry: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let client = context.get().await.map_err(error::EpError::request)?;
        let result = client.get("/api/v1/hosts").await?;

        let mut info = DatadogHostInfo::default();

        if let Some(total) = result.get("total_returned").and_then(Value::as_u64) {
            info.total_hosts = total;
        }

        if let Some(hosts) = result.get("host_list").and_then(Value::as_array) {
            for host in hosts {
                let is_up = host.get("up").and_then(Value::as_bool).unwrap_or(false);
                if is_up {
                    info.up_hosts += 1;
                }
                if let Some(name) = host.get("name").and_then(Value::as_str) {
                    info.host_names.push(name.to_string());
                }
            }
        }

        Ok(info)
    }
}
