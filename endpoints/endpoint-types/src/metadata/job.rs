use std::time::Instant;

use chrono::{DateTime, Utc};
use eden_logger_internal::{LogAudience, log_debug, log_error, trace_context};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use telemetry::TelemetryWrapper;
use tokio::time::{Duration, timeout};

use super::{MetadataConfig, SyncFrequency};
use error::{EpError, MetadataError, ResultEP};

pub fn job_timeout_duration() -> Duration {
    MetadataConfig::default().job_timeout.max(Duration::from_millis(1))
}

type MetadataJobExec<A, M> =
    Box<dyn for<'a> Fn(&'a mut M, A, &'a mut TelemetryWrapper, &'a dyn CapabilityChecker) -> BoxFuture<'a, ResultEP<()>> + Send + Sync>;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobErrorMode {
    Recoverable,
    Fatal,
}

/// Controls how collector errors are handled by builder functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorErrorPolicy {
    /// Propagate the error, failing the job (default for Mongo/PG).
    Propagate,
    /// Record the error but mark the job as successful (for Redis `parsing_errors` pattern).
    RecordAndContinue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapabilityId(pub &'static str);

impl std::fmt::Display for CapabilityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

pub trait CapabilityChecker: Send + Sync {
    fn has(&self, id: &CapabilityId) -> bool;
}

/// Checker that treats all capabilities as present.
/// Used by compatibility paths that bypass capability checks.
pub struct PermissiveCapabilities;

impl CapabilityChecker for PermissiveCapabilities {
    fn has(&self, _id: &CapabilityId) -> bool {
        true
    }
}

/// Checker that treats all capabilities as absent.
/// Used when capability discovery fails — jobs with requirements are skipped.
pub struct UnknownCapabilities;

impl CapabilityChecker for UnknownCapabilities {
    fn has(&self, _id: &CapabilityId) -> bool {
        false
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum SkipReason {
    CapabilityMissing(String),
}

#[derive(Debug, Clone, Serialize)]
pub enum JobStatus {
    Success,
    Failure { severity: JobErrorMode },
    Skipped { reason: SkipReason },
}

#[derive(Debug, Clone, Serialize)]
pub struct JobReport {
    pub name: String,
    pub frequency: SyncFrequency,
    pub duration_ms: u128,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<EpError>,
}

impl JobReport {
    pub fn success(name: impl Into<String>, frequency: SyncFrequency, duration_ms: u128) -> Self {
        Self {
            name: name.into(),
            frequency,
            duration_ms,
            status: JobStatus::Success,
            error: None,
        }
    }

    pub fn failure(name: impl Into<String>, frequency: SyncFrequency, duration_ms: u128, severity: JobErrorMode, error: EpError) -> Self {
        Self {
            name: name.into(),
            frequency,
            duration_ms,
            status: JobStatus::Failure { severity },
            error: Some(error),
        }
    }

    pub fn skipped(name: impl Into<String>, frequency: SyncFrequency, reason: SkipReason) -> Self {
        Self {
            name: name.into(),
            frequency,
            duration_ms: 0,
            status: JobStatus::Skipped { reason },
            error: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MetadataBatch<M>
where
    M: Serialize,
{
    pub frequency: SyncFrequency,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub reports: Vec<JobReport>,
    pub had_fatal: bool,
    pub data: M,
}

impl<M> MetadataBatch<M>
where
    M: Serialize,
{
    pub fn new(frequency: SyncFrequency, started_at: DateTime<Utc>, finished_at: DateTime<Utc>, data: M, reports: Vec<JobReport>) -> Self {
        let had_fatal = reports.iter().any(|report| matches!(report.status, JobStatus::Failure { severity: JobErrorMode::Fatal }));

        Self { frequency, started_at, finished_at, reports, had_fatal, data }
    }

    pub fn failure(frequency: SyncFrequency, data: M, name: impl Into<String>, severity: JobErrorMode, error: EpError) -> Self {
        let now = Utc::now();
        let report = JobReport::failure(name, frequency, 0, severity, error);
        Self {
            frequency,
            started_at: now,
            finished_at: now,
            reports: vec![report],
            had_fatal: severity == JobErrorMode::Fatal,
            data,
        }
    }
}

pub struct MetadataJob<A, M> {
    name: String,
    frequency: SyncFrequency,
    executor: MetadataJobExec<A, M>,
    error_mode: JobErrorMode,
    timeout: Option<Duration>,
    requirements: Vec<CapabilityId>,
}

impl<A, M> MetadataJob<A, M> {
    pub fn new<F>(name: String, frequency: SyncFrequency, func: F) -> Self
    where
        F: for<'a> Fn(&'a mut M, A, &'a mut TelemetryWrapper, &'a dyn CapabilityChecker) -> BoxFuture<'a, ResultEP<()>>
            + Send
            + Sync
            + 'static,
    {
        let executor: MetadataJobExec<A, M> = Box::new(func);

        Self {
            name,
            frequency,
            executor,
            error_mode: JobErrorMode::Recoverable,
            timeout: None,
            requirements: Vec::new(),
        }
    }

    pub fn with_error_mode(mut self, mode: JobErrorMode) -> Self {
        self.error_mode = mode;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_requirement(mut self, req: CapabilityId) -> Self {
        self.requirements.push(req);
        self
    }

    pub fn with_requirements(mut self, reqs: Vec<CapabilityId>) -> Self {
        self.requirements.extend(reqs);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn frequency(&self) -> SyncFrequency {
        self.frequency
    }

    pub fn error_mode(&self) -> JobErrorMode {
        self.error_mode
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn requirements(&self) -> &[CapabilityId] {
        &self.requirements
    }

    pub fn execute<'a>(
        &'a self,
        metadata: &'a mut M,
        context: A,
        telemetry: &'a mut TelemetryWrapper,
        capabilities: &'a dyn CapabilityChecker,
    ) -> BoxFuture<'a, ResultEP<()>> {
        (self.executor)(metadata, context, telemetry, capabilities)
    }

    fn priority(&self) -> u8 {
        match self.frequency {
            SyncFrequency::High => 0,
            SyncFrequency::Medium => 1,
            SyncFrequency::Low => 2,
        }
    }
}

pub async fn run_metadata_jobs<A, M>(
    metadata: M,
    context: A,
    jobs: Vec<MetadataJob<A, M>>,
    telemetry: &mut TelemetryWrapper,
    frequency: SyncFrequency,
    timeout_duration: Duration,
) -> MetadataBatch<M>
where
    A: Clone + Send + Sync + 'static,
    M: Serialize,
{
    run_metadata_jobs_with_capabilities(metadata, context, jobs, telemetry, frequency, timeout_duration, &PermissiveCapabilities).await
}

pub async fn run_metadata_jobs_with_capabilities<A, M>(
    metadata: M,
    context: A,
    mut jobs: Vec<MetadataJob<A, M>>,
    telemetry: &mut TelemetryWrapper,
    frequency: SyncFrequency,
    timeout_duration: Duration,
    capabilities: &dyn CapabilityChecker,
) -> MetadataBatch<M>
where
    A: Clone + Send + Sync + 'static,
    M: Serialize,
{
    let ctx = trace_context().with_function("run_metadata_jobs");
    jobs.sort_by_key(|left| left.priority());

    let mut metadata = metadata;
    let mut reports = Vec::with_capacity(jobs.len());
    let started_at = Utc::now();

    for job in jobs {
        if let Some(unmet) = job.requirements().iter().find(|req| !capabilities.has(req)) {
            let reason = SkipReason::CapabilityMissing(unmet.to_string());
            log_debug!(
                ctx.clone(),
                format!("skipping job '{}': requirement not met: {unmet}", job.name()),
                audience = LogAudience::Internal,
                frequency = job.frequency().as_str()
            );
            reports.push(JobReport::skipped(job.name(), job.frequency(), reason));
            continue;
        }

        let start = Instant::now();
        let job_name = job.name().to_string();
        let job_timeout = job.timeout().unwrap_or(timeout_duration);
        let result = match timeout(job_timeout, job.execute(&mut metadata, context.clone(), telemetry, capabilities)).await {
            Ok(inner) => inner,
            Err(_) => Err(EpError::Metadata(MetadataError::QueryTimeout(job_name))),
        };

        match result {
            Ok(()) => {
                reports.push(JobReport::success(job.name(), job.frequency(), start.elapsed().as_millis()));
            }
            Err(err) => {
                log_error!(
                    ctx.clone(),
                    format!("metadata job '{}' failed: {err}", job.name()),
                    audience = LogAudience::Internal,
                    frequency = job.frequency().as_str()
                );
                reports.push(JobReport::failure(job.name(), job.frequency(), start.elapsed().as_millis(), job.error_mode(), err));
                if matches!(job.error_mode(), JobErrorMode::Fatal) {
                    break;
                }
            }
        }
    }

    let finished_at = Utc::now();
    MetadataBatch::new(frequency, started_at, finished_at, metadata, reports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use format::EdenNodeUuid;
    use serde::Serialize;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use telemetry::{TelemetryDurations, labels::TelemetryLabels, setup_metrics};
    use tokio::sync::Mutex;

    #[derive(Debug, Default, Serialize, Clone)]
    struct DummyMetadata {
        pub note: &'static str,
    }

    fn telemetry() -> TelemetryWrapper {
        TelemetryWrapper::new(
            Arc::new(setup_metrics("http://localhost:4317", "").expect("setup metrics")),
            TelemetryLabels::new(&EdenNodeUuid::new_uuid()),
            TelemetryDurations::default(),
        )
    }

    struct DenyAllCapabilities;
    impl CapabilityChecker for DenyAllCapabilities {
        fn has(&self, _id: &CapabilityId) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn stops_after_fatal_job() {
        let mut telemetry = telemetry();
        let order = Arc::new(Mutex::new(Vec::new()));
        let metadata = DummyMetadata::default();
        let ctx = ();

        let jobs = vec![
            MetadataJob::new("ok".to_string(), SyncFrequency::High, {
                let order = order.clone();
                move |_metadata: &mut DummyMetadata, _ctx: (), _telemetry: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    let order = order.clone();
                    Box::pin(async move {
                        order.lock().await.push("ok");
                        Ok(())
                    })
                }
            }),
            MetadataJob::new("fatal".to_string(), SyncFrequency::High, {
                let order = order.clone();
                move |_metadata: &mut DummyMetadata, _ctx: (), _telemetry: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    let order = order.clone();
                    Box::pin(async move {
                        order.lock().await.push("fatal");
                        Err(EpError::Metadata(MetadataError::Custom("boom".to_string())))
                    })
                }
            })
            .with_error_mode(JobErrorMode::Fatal),
            MetadataJob::new("skipped".to_string(), SyncFrequency::High, {
                let order = order.clone();
                move |_metadata: &mut DummyMetadata, _ctx: (), _telemetry: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    let order = order.clone();
                    Box::pin(async move {
                        order.lock().await.push("skipped");
                        Ok(())
                    })
                }
            }),
        ];

        let batch = run_metadata_jobs(metadata, ctx, jobs, &mut telemetry, SyncFrequency::High, job_timeout_duration()).await;

        let order = order.lock().await;
        assert_eq!(order.as_slice(), &["ok", "fatal"]);
        assert_eq!(batch.reports.len(), 2);
        assert!(batch.had_fatal);
    }

    #[tokio::test]
    async fn times_out_long_running_jobs() {
        let mut telemetry = telemetry();
        let metadata = DummyMetadata::default();
        let ctx = ();
        let timeout = Duration::from_secs(1);

        let jobs = vec![
            MetadataJob::new("slow".to_string(), SyncFrequency::High, {
                move |_metadata: &mut DummyMetadata, _ctx: (), _telemetry: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        Ok(())
                    })
                }
            })
            .with_error_mode(JobErrorMode::Fatal),
        ];

        let batch = run_metadata_jobs(metadata, ctx, jobs, &mut telemetry, SyncFrequency::High, timeout).await;

        assert_eq!(batch.reports.len(), 1);
        assert!(batch.had_fatal);
        assert!(matches!(batch.reports[0].status, JobStatus::Failure { severity: JobErrorMode::Fatal }));
        assert!(matches!(batch.reports[0].error, Some(EpError::Metadata(MetadataError::QueryTimeout(_)))));
    }

    #[tokio::test]
    async fn per_job_timeout_override_is_used() {
        let mut telemetry = telemetry();
        let metadata = DummyMetadata::default();
        let ctx = ();

        let jobs = vec![
            MetadataJob::new("slow_override".to_string(), SyncFrequency::High, {
                move |_metadata: &mut DummyMetadata, _ctx: (), _telemetry: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        Ok(())
                    })
                }
            })
            .with_error_mode(JobErrorMode::Fatal)
            .with_timeout(Duration::from_millis(50)),
        ];

        let batch = run_metadata_jobs(metadata, ctx, jobs, &mut telemetry, SyncFrequency::High, Duration::from_secs(5)).await;

        assert_eq!(batch.reports.len(), 1);
        assert!(batch.had_fatal);
        assert!(matches!(batch.reports[0].error, Some(EpError::Metadata(MetadataError::QueryTimeout(_)))));
    }

    #[tokio::test]
    async fn skips_jobs_with_unmet_requirements() {
        let mut telemetry = telemetry();
        let executed = Arc::new(AtomicUsize::new(0));
        let metadata = DummyMetadata::default();

        let jobs = vec![
            MetadataJob::new("needs_cluster".to_string(), SyncFrequency::High, {
                let executed = executed.clone();
                move |_m: &mut DummyMetadata, _c: (), _t: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    let executed = executed.clone();
                    Box::pin(async move {
                        executed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }
            })
            .with_requirement(CapabilityId("redis.cluster")),
            MetadataJob::new("no_requirements".to_string(), SyncFrequency::High, {
                let executed = executed.clone();
                move |_m: &mut DummyMetadata, _c: (), _t: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    let executed = executed.clone();
                    Box::pin(async move {
                        executed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }
            }),
        ];

        let batch = run_metadata_jobs_with_capabilities(
            metadata,
            (),
            jobs,
            &mut telemetry,
            SyncFrequency::High,
            job_timeout_duration(),
            &DenyAllCapabilities,
        )
        .await;

        assert_eq!(executed.load(Ordering::SeqCst), 1);
        assert_eq!(batch.reports.len(), 2);
        assert!(matches!(batch.reports[0].status, JobStatus::Skipped { .. }));
        assert!(matches!(batch.reports[1].status, JobStatus::Success));
    }

    #[tokio::test]
    async fn permissive_capabilities_runs_all_jobs() {
        let mut telemetry = telemetry();
        let executed = Arc::new(AtomicUsize::new(0));
        let metadata = DummyMetadata::default();

        let jobs = vec![
            MetadataJob::new("with_req".to_string(), SyncFrequency::High, {
                let executed = executed.clone();
                move |_m: &mut DummyMetadata, _c: (), _t: &mut TelemetryWrapper, _capabilities: &dyn CapabilityChecker| {
                    let executed = executed.clone();
                    Box::pin(async move {
                        executed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }
            })
            .with_requirement(CapabilityId("redis.cluster")),
        ];

        let batch = run_metadata_jobs(metadata, (), jobs, &mut telemetry, SyncFrequency::High, job_timeout_duration()).await;

        assert_eq!(executed.load(Ordering::SeqCst), 1);
        assert!(matches!(batch.reports[0].status, JobStatus::Success));
    }
}
