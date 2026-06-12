pub mod stc;
mod sync;

use crate::ep::AwsAsync;
use crate::metadata::{
    stc::{account_aliases::AwsAccountAliases, iam_summary::AwsIamSummary, identity::AwsAccountIdentity},
    sync::AwsLastSyncTimestamps,
};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{
    CapabilityChecker, EpMetadata, MetadataJob, SyncCollector, SyncFrequency, SyncMetadata, UnknownCapabilities,
};
use ep_core::define_metadata_serializer_stuff;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryInto;
use telemetry::TelemetryWrapper;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct AwsMetadata {
    pub identity: AwsAccountIdentity,
    pub iam_summary: AwsIamSummary,
    pub account_aliases: AwsAccountAliases,

    pub collection_timestamp: u64,
    pub last_sync_timestamps: AwsLastSyncTimestamps,
}

// SyncCollector impls
macro_rules! impl_sync_collector_aws {
    ($($type:ty),* $(,)?) => {
        $(
            impl SyncCollector<AwsAsync> for $type {
                fn sync_metadata<'a>(
                    &'a self,
                    context: AwsAsync,
                    telemetry: &'a mut TelemetryWrapper,
                    capabilities: &'a dyn CapabilityChecker,
                ) -> futures::future::BoxFuture<'a, ResultEP<Self>> {
                    Box::pin(self.sync_metadata(context, telemetry, capabilities))
                }
            }
        )*
    };
}

impl_sync_collector_aws!(AwsAccountIdentity, AwsIamSummary, AwsAccountAliases);

fn build_aws_job<T>(
    name: &'static str,
    frequency: SyncFrequency,
    set: fn(&mut AwsMetadata, T),
    touch_ts: fn(&mut AwsMetadata),
) -> MetadataJob<AwsAsync, AwsMetadata>
where
    T: SyncCollector<AwsAsync> + Default + 'static,
{
    MetadataJob::new(
        name.to_string(),
        frequency,
        move |metadata: &mut AwsMetadata, ctx: AwsAsync, telemetry: &mut TelemetryWrapper, capabilities: &dyn CapabilityChecker| {
            Box::pin(async move {
                let value = T::default().sync_metadata(ctx, telemetry, capabilities).await?;
                set(metadata, value);
                touch_ts(metadata);
                Ok(())
            })
        },
    )
}

impl SyncMetadata<AwsAsync> for AwsMetadata {
    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<AwsAsync, Self>> {
        self.collection_timestamp = AwsMetadata::current_timestamp();

        let mut jobs: Vec<MetadataJob<AwsAsync, Self>> = Vec::new();

        if matches!(frequency, SyncFrequency::High) {
            jobs.push(build_aws_job::<AwsAccountIdentity>(
                "aws.identity",
                SyncFrequency::High,
                |m, v| m.identity = v,
                |m| m.last_sync_timestamps.identity_last_sync = AwsMetadata::current_timestamp(),
            ));
        }

        if matches!(frequency, SyncFrequency::Medium) {
            jobs.push(build_aws_job::<AwsIamSummary>(
                "aws.iam_summary",
                SyncFrequency::Medium,
                |m, v| m.iam_summary = v,
                |m| m.last_sync_timestamps.iam_summary_last_sync = AwsMetadata::current_timestamp(),
            ));
        }

        if matches!(frequency, SyncFrequency::Low) {
            jobs.push(build_aws_job::<AwsAccountAliases>(
                "aws.account_aliases",
                SyncFrequency::Low,
                |m, v| m.account_aliases = v,
                |m| m.last_sync_timestamps.account_aliases_last_sync = AwsMetadata::current_timestamp(),
            ));
        }

        jobs
    }

    fn discover_capabilities<'a>(
        _connection: AwsAsync,
        _telemetry: &'a mut TelemetryWrapper,
    ) -> futures::future::BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async move { Box::new(UnknownCapabilities) as Box<dyn CapabilityChecker> })
    }
}

impl AwsMetadata {
    fn current_timestamp() -> u64 {
        Utc::now().timestamp().try_into().unwrap_or_default()
    }

    pub fn last_sync(&self) -> &AwsLastSyncTimestamps {
        &self.last_sync_timestamps
    }

    pub fn new() -> Self {
        Self {
            collection_timestamp: AwsMetadata::current_timestamp(),
            ..Default::default()
        }
    }
}

impl EpMetadata for AwsMetadata {
    fn kind(&self) -> EpKind {
        EpKind::Aws
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

define_metadata_serializer_stuff!(EpKind::Aws => AwsMetadata);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn job_names(jobs: &[MetadataJob<AwsAsync, AwsMetadata>]) -> HashSet<&str> {
        jobs.iter().map(|job| job.name()).collect()
    }

    #[test]
    fn high_frequency_jobs_include_identity() {
        let mut metadata = AwsMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::High);
        assert_eq!(jobs.len(), 1);
        let names = job_names(&jobs);
        assert!(names.contains("aws.identity"));
    }

    #[test]
    fn medium_frequency_jobs_include_iam_summary() {
        let mut metadata = AwsMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::Medium);
        assert_eq!(jobs.len(), 1);
        let names = job_names(&jobs);
        assert!(names.contains("aws.iam_summary"));
    }

    #[test]
    fn low_frequency_jobs_include_account_aliases() {
        let mut metadata = AwsMetadata::default();
        let jobs = metadata.jobs(SyncFrequency::Low);
        assert_eq!(jobs.len(), 1);
        let names = job_names(&jobs);
        assert!(names.contains("aws.account_aliases"));
    }
}
