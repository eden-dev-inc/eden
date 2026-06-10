use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use telemetry::TelemetryWrapper;

use crate::ep::DatadogAsync;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DatadogMonitorSummary {
    pub total_monitors: u64,
    pub ok_count: u64,
    pub alert_count: u64,
    pub warn_count: u64,
    pub no_data_count: u64,
}

impl MetadataCollection for DatadogMonitorSummary {
    type Request = ();

    fn request(&self) -> Self::Request {}

    fn description(&self) -> &'static str {
        "Collect summary of Datadog monitor statuses"
    }

    fn category(&self) -> &'static str {
        "monitors"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl DatadogMonitorSummary {
    pub(crate) async fn sync_metadata(
        &self,
        context: DatadogAsync,
        _telemetry: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let client = context.get().await.map_err(error::EpError::request)?;
        let result = client.get("/api/v1/monitor").await?;

        let mut summary = DatadogMonitorSummary::default();

        if let Some(monitors) = result.as_array() {
            summary.total_monitors = monitors.len() as u64;

            for monitor in monitors {
                match monitor.get("overall_state").and_then(Value::as_str) {
                    Some("OK") => summary.ok_count += 1,
                    Some("Alert") => summary.alert_count += 1,
                    Some("Warn") => summary.warn_count += 1,
                    Some("No Data") => summary.no_data_count += 1,
                    _ => {}
                }
            }
        }

        Ok(summary)
    }
}
