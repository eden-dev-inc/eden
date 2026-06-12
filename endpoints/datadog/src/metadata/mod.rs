pub mod capabilities;
pub mod stc;
mod sync;

use crate::ep::DatadogAsync;
use crate::metadata::{
    stc::{hosts::DatadogHostInfo, monitors::DatadogMonitorSummary},
    sync::DatadogLastSyncTimestamps,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{
    CapabilityChecker, EpMetadata, MetadataJob, SyncCollector, SyncFrequency, SyncMetadata, UnknownCapabilities,
};
use ep_core::define_metadata_serializer_stuff;
use error::ResultEP;
use format::endpoint::EpKind;
use log::warn;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use telemetry::TelemetryWrapper;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct DatadogMetadata {
    pub monitor_summary: DatadogMonitorSummary,
    pub host_info: DatadogHostInfo,

    pub collection_timestamp: u64,
    pub last_sync_timestamps: DatadogLastSyncTimestamps,
}

// SyncCollector impls
macro_rules! impl_sync_collector_dd {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<DatadogAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: DatadogAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_dd!(DatadogMonitorSummary, DatadogHostInfo);

fn build_dd_single_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut DatadogMetadata, T),
    touch_ts: fn(&mut DatadogMetadata),
) -> MetadataJob<DatadogAsync, DatadogMetadata>
where
    T: SyncCollector<DatadogAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut DatadogMetadata, ctx: DatadogAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, value);
                touch_ts(metadata);
                Ok(())
            })
        },
    )
}

impl SyncMetadata<DatadogAsync> for DatadogMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<DatadogAsync, Self>> {
        self.collection_timestamp = DatadogMetadata::current_timestamp();

        let mut jobs: Vec<MetadataJob<DatadogAsync, Self>> = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.push(build_dd_single_job::<DatadogMonitorSummary>(
                "datadog.monitor_summary",
                SyncFrequency::High,
                |m, v| m.monitor_summary = v,
                |m| m.last_sync_timestamps.monitor_summary_last_sync = DatadogMetadata::current_timestamp(),
            ));
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.push(build_dd_single_job::<DatadogHostInfo>(
                "datadog.host_info",
                SyncFrequency::Medium,
                |m, v| m.host_info = v,
                |m| m.last_sync_timestamps.host_info_last_sync = DatadogMetadata::current_timestamp(),
            ));
        }

        jobs
    }

    fn discover_capabilities<'a>(
        connection: DatadogAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move {
            match connection.get().await {
                Ok(client) => match capabilities::DatadogCapabilities::discover(&client).await {
                    Ok(caps) => Box::new(caps) as Box<dyn CapabilityChecker>,
                    Err(e) => {
                        warn!("failed to discover Datadog capabilities: {e}; using unknown defaults");
                        Box::new(UnknownCapabilities)
                    }
                },
                Err(e) => {
                    warn!("failed to get Datadog client for capability discovery: {e}; using unknown defaults");
                    Box::new(UnknownCapabilities)
                }
            }
        })
    }
}

impl DatadogMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }

    pub fn last_sync(&self) -> &DatadogLastSyncTimestamps {
        &self.last_sync_timestamps
    }

    pub fn new() -> Self {
        Self {
            collection_timestamp: DatadogMetadata::current_timestamp(),
            ..Default::default()
        }
    }
}

impl EpMetadata for DatadogMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Datadog
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

define_metadata_serializer_stuff!(EpKind::Datadog => DatadogMetadata);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn job_names(jobs: &[MetadataJob<DatadogAsync, DatadogMetadata>]) -> HashSet<&str> {
        jobs.iter().map(|job| job.name()).collect()
    }

    #[test]
    fn high_frequency_jobs_include_monitors() {
        let mut metadata = DatadogMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::High);
        assert_eq!(jobs.len(), 1);
        let names = job_names(&jobs);
        assert!(names.contains("datadog.monitor_summary"));
    }

    #[test]
    fn medium_frequency_jobs_include_hosts() {
        let mut metadata = DatadogMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::Medium);
        assert_eq!(jobs.len(), 1);
        let names = job_names(&jobs);
        assert!(names.contains("datadog.host_info"));
    }

    #[test]
    fn low_frequency_returns_no_jobs() {
        let mut metadata = DatadogMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::Low);
        assert!(jobs.is_empty());
    }
}
