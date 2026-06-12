// Suppress async_fn_in_trait warning because we don't need to specify auto trait bounds for these traits.
#![allow(async_fn_in_trait)]

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::{Operation, RunRequest, downcast_config, downcast_request};
pub use crate::{request::EpRequest, transaction::EpTransaction};

use crate::metadata::{
    EpMetadata, JobReport, MetadataBatch, MetadataCollectorInfo, SyncFrequency, SyncMetadata, job_timeout_duration, run_metadata_jobs,
};
use crate::request::EpWireRequest;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_logger_internal::{ctx_with_trace, log_trace};
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpConfig, EpConnection, EpRouter, PoolType, RWPool};
use ep_core::settings::EdenSettings;
pub use ep_core::tls::TlsData;
use ep_core::{EndpointType, GetPool, ReqType};
use error::{EpError, MetadataError, ResultEP};
use format::CacheUuid;
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use format::{EndpointUuid, OrganizationUuid};
use function_name::named;
use telemetry::{FastSpanAttribute, FastSpanStatus};

use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use telemetry::TelemetryWrapper;
use telemetry::metric_event::{MetricEvent, RecordMetric};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Receiver;
use utoipa::ToSchema;

fn unknown_packages<'a>(requested: &'a std::collections::HashSet<&str>, collectors: &[MetadataCollectorInfo]) -> Vec<&'a str> {
    let available: std::collections::HashSet<&str> = collectors.iter().map(|info| info.package()).collect();
    requested.iter().cloned().filter(|name| !available.contains(name)).collect()
}

pub type RouterFuture<'a, T> = Pin<Box<dyn Future<Output = ResultEP<T>> + Send + 'a>>;

pub trait EpLifecycleSpec {
    type Async: Clone + Send + Sync + 'static;
    type Config: EpConfig + RWPool<Self::Async> + Clone + ToSchema + 'static;
    type Request: EpRequest + EndpointType + RunRequest<Self::Async, Self::Api, Self::Tx> + 'static;
    type Metadata: EpMetadata + SyncMetadata<Self::Async> + Clone + Serialize + Default + 'static;
    type Api: 'static;
    type Tx: 'static;
}

pub trait EpLifecycleRouter: EpRouter {
    fn connect_boxed<'a>(
        &'a mut self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()>;

    fn disconnect_boxed<'a>(
        &'a mut self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()>;

    fn reconnect_boxed<'a>(
        &'a mut self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()>;

    fn health_check_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()>;

    fn test_write_conn_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()>;

    fn read_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        read: &'a mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value>;

    fn read_with_conn_boxed<'a>(
        &'a self,
        els_conn: Box<dyn EpConnection>,
        config: Box<dyn EpConfig>,
        read: &'a mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value>;

    fn read_bytes_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        read: &'a mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Bytes>;

    fn write_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        write: &'a dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value>;

    fn write_with_conn_boxed<'a>(
        &'a self,
        els_conn: Box<dyn EpConnection>,
        config: Box<dyn EpConfig>,
        write: &'a dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value>;

    fn write_bytes_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        write: &'a dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Bytes>;

    fn metadata_boxed<'a>(
        &'a self,
        db_manager: &'a DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        settings: EdenSettings,
        packages: Option<&'a [String]>,
        frequency: Option<SyncFrequency>,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value>;

    fn collector_info_boxed(&self) -> Vec<MetadataCollectorInfo>;

    fn transaction_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &'a dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value>;
}

pub trait EP<
    A: Clone + Send + Sync + 'static, // Async Database Context
    C: EpConfig + RWPool<A> + Clone + ToSchema + 'static,
    Req: EpRequest + EndpointType + RunRequest<A, K, X> + 'static,
    M: EpMetadata + SyncMetadata<A> + Clone + Serialize + 'static,
    K: 'static,
    X: 'static,
>: Send + Sync + GetPool<A>
{
    //* SYNC FUNCTIONS *//
    fn new() -> Self
    where
        Self: Sized;
    fn transaction(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>, // send initial prepare response (Ok or Err)
        inbound: Receiver<bool>, // receive response (true = commit, false = rollback)
        transaction: &dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<Value, EpError>> + Send;

    fn health_check(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<()>> + Send;


    fn kind() -> EpKind;
    fn validate_operation(&self, _op: &dyn Operation<A, K, X>) -> ResultEP<()> {
        Ok(())
    }
    #[named]
    fn connect_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Option<PoolType<A>>>> + Send {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            let config = downcast_config::<A, C>(config, &mut span).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) })
            })?;

            // Pin the endpoint UUID onto the telemetry labels so downstream pool
            // constructors (e.g. `RedisConnectionManager`) can label their
            // per-connection counters by endpoint.
            let endpoint_uuid: EndpointUuid = endpoint_cache_uuid.eden_uuid();
            telemetry_wrapper.mut_labels(|labels| labels.set_endpoint_uuid(endpoint_uuid.clone()));

            let conn_set = config.init_conn_async(telemetry_wrapper).await?;
            Ok(self.mut_pool().connect_async(endpoint_cache_uuid, conn_set).await)
        }
    }
    #[named]
    fn disconnect_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Option<PoolType<A>>>> + Send {
        async move {
            let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            Ok(self.mut_pool().disconnect_async(endpoint_cache_uuid).await)
        }
    }
    #[named]
    fn reconnect_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<()>> + Send
    where
        Self: Sized,
    {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            span.add_event(format!("attempting to reconnect async connection to {}", Self::kind()), vec![]);

            let config = downcast_config::<A, C>(config, &mut span)?;

            span.add_event(format!("downcast connection to {}-config", Self::kind()), vec![]);

            let mut candidate = Self::new();
            candidate.connect_async(endpoint_cache_uuid, config.as_config(), telemetry_wrapper).await.inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            })?;

            let health_result = candidate.health_check(endpoint_cache_uuid, telemetry_wrapper).await;
            let disconnect_result = candidate.disconnect_async(endpoint_cache_uuid, telemetry_wrapper).await;

            if let Err(disconnect_error) = disconnect_result {
                span.add_event(
                    "failed to disconnect temporary reconnect validation connection",
                    vec![FastSpanAttribute::new("error", disconnect_error.to_string())],
                );

                if health_result.is_ok() {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(disconnect_error.to_string()) });
                    return Err(disconnect_error);
                }
            }

            if let Err(e) = health_result {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                return Err(e);
            }

            self.connect_async(endpoint_cache_uuid, config.as_config(), telemetry_wrapper).await.inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            })?;

            span.add_simple_event("reconnected successfully");

            Ok(())
        }
    }
    #[named]
    fn test_write_conn_async(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<()>> + Send {
        async move {
            let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            match self.pool().write_conn_async(endpoint_cache_uuid).await {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        }
    }
    #[named]
    fn read(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        read: &mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Value>> + Send {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            span.add_event(
                "running read async",
                vec![FastSpanAttribute::new("endpoint", endpoint_cache_uuid.to_string())],
            );

            let downcast_request = downcast_request::<A, K, X, Req>(&*read, &mut span)?;

            self.validate_operation(downcast_request.operation()).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) })
            })?;

            if downcast_request.operation().request_type() != ReqType::Read {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned("attempted write operation on read connection".to_string()),
                });
                return Err(EpError::request_read_only());
            }

            let client = self.pool().read_conn_async(endpoint_cache_uuid).await?;

            if settings.test() {
                Ok(json!("PASSED"))
            } else {
                downcast_request.run_request(client.clone(), settings, telemetry_wrapper).await?.try_serde_serialize()
            }
        }
    }
    /// Read using a one-off ELS connection override instead of the pool.
    ///
    /// Creates a temporary async context from `els_conn` via `config.conn_async()`,
    /// runs the request on it, then drops the context (connection closes).
    #[named]
    fn read_with_conn(
        &self,
        els_conn: Box<dyn EpConnection>,
        config: Box<dyn EpConfig>,
        read: &mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Value>> + Send {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            span.add_simple_event("running ELS read with override connection");

            let downcast_request = downcast_request::<A, K, X, Req>(&*read, &mut span)?;

            self.validate_operation(downcast_request.operation()).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) })
            })?;

            if downcast_request.operation().request_type() != ReqType::Read {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned("attempted write operation on read connection".to_string()),
                });
                return Err(EpError::request_read_only());
            }

            let concrete_config = downcast_config::<A, C>(config, &mut span)?;
            let client = concrete_config.conn_async(els_conn, telemetry_wrapper).await?;

            if settings.test() {
                Ok(json!("PASSED"))
            } else {
                downcast_request.run_request(client, settings, telemetry_wrapper).await?.try_serde_serialize()
            }
        }
    }

    #[named]
    fn read_bytes(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        read: &mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Bytes>> + Send {
        async move {
            let _t0 = Instant::now();
            let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

            span.add_event(
                "running read async",
                vec![FastSpanAttribute::new("endpoint", endpoint_cache_uuid.to_string())],
            );

            let _ctx = ctx_with_trace!().with_feature("endpoint");
            log_trace!(_ctx.clone(), "ReadBytes Core: initialized tracer, trying to get connection",
                audience = eden_logger_internal::LogAudience::Internal,
                endpoint_cache_uuid = endpoint_cache_uuid.to_string(),
                elapsed_micros = _t0.elapsed().as_micros()
            );

            let downcast_request = downcast_request::<A, K, X, Req>(&*read, &mut span)?;

            self.validate_operation(downcast_request.operation()).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) })
            })?;

            if downcast_request.operation().request_type() != ReqType::Read {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned("attempted write operation on read connection".to_string()),
                });
                return Err(EpError::request_read_only());
            }

            let client = self.pool().read_conn_async(endpoint_cache_uuid).await?;

            log_trace!(_ctx.clone(), "ReadBytes Core: got client connection",
                audience = eden_logger_internal::LogAudience::Internal,
                elapsed_micros = _t0.elapsed().as_micros()
            );

            let result = if settings.test() {
                Ok(Bytes::from_static(b"PASSED"))
            } else {
                downcast_request.run_request(client.clone(), settings, telemetry_wrapper).await?.try_to_bytes()
            };

            log_trace!(_ctx, "ReadBytes Core: run_request finished",
                audience = eden_logger_internal::LogAudience::Internal,
                elapsed_micros = _t0.elapsed().as_micros()
            );
            result
        }
    }
    fn raw_bytes<B: EpWireRequest<A>>(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: B,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<bytes::Bytes>> {
        async move {
            let req_type = bytes.request_type()?;
            self.raw_bytes_with_req_type(endpoint_cache_uuid, bytes, req_type, settings, telemetry_wrapper).await
        }
    }

    #[named]
    fn raw_bytes_with_req_type<B: EpWireRequest<A>>(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        bytes: B,
        req_type: ReqType,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<bytes::Bytes>> {
        async move {
            let _t0 = Instant::now();

            // Store UUID inline — formatted to hex only at export time (SpanValue::Uuid),
            // or into a stack buffer for metric labels.
            let endpoint_uuid = endpoint_cache_uuid.uuid();

            let mut span = telemetry_wrapper.client_tracer(Self::kind().span_raw_bytes());
            span.add_event("running tcp_read_bytes", vec![FastSpanAttribute::new("endpoint", endpoint_uuid)]);

            let _ctx = ctx_with_trace!().with_feature("endpoint");

            log_trace!(_ctx.clone(), "ReadBytes Core tcp_read_bytes: initialized tracer, trying to get connection",
                audience = eden_logger_internal::LogAudience::Internal,
                endpoint_cache_uuid = endpoint_uuid.to_string(),
                request_type = format!("{:?}", req_type),
                elapsed_micros = _t0.elapsed().as_micros()
            );

            // Use the appropriate connection based on request type
            let client = {
                let mut _pool_span = telemetry_wrapper.client_tracer(Self::kind().span_pool_acquire());

                match req_type {
                    ReqType::Read => self.pool().read_conn_async(endpoint_cache_uuid).await?,
                    ReqType::Write => self.pool().write_conn_async(endpoint_cache_uuid).await?,
                }
            };

            log_trace!(_ctx.clone(), "ReadBytes Core tcp_read_bytes: got client connection",
                audience = eden_logger_internal::LogAudience::Internal,
                connection_type = format!("{:?}", req_type),
                elapsed_micros = _t0.elapsed().as_micros()
            );

            let result = if settings.test() {
                Ok(bytes::Bytes::from_static(b"PASSED"))
            } else {
                let timeout_duration = settings.max_timeout_duration();

                let network_result = {
                    let mut network_span = telemetry_wrapper.client_tracer(Self::kind().span_send_raw_bytes());
                    network_span.add_simple_event("sending bytes to endpoint");

                    // Send bytes bounded by request timeout
                    let network_result = match tokio::time::timeout(timeout_duration, bytes.send_raw_bytes(client)).await {
                        Ok(result) => result,
                        Err(_elapsed) => {
                            let err_msg = format!("Operation timed out after {} ms", timeout_duration.as_millis());
                            network_span.add_event("timeout", vec![FastSpanAttribute::new("error", err_msg.clone())]);
                            Err(EpError::timeout(err_msg))
                        }
                    };

                    // Record result in span
                    match &network_result {
                        Ok(_) => network_span.add_simple_event("received response from endpoint"),
                        Err(e) => network_span.add_event("error from endpoint", vec![FastSpanAttribute::new("error", e.to_string())]),
                    }
                    network_result
                };

                // Record network latency metric — format UUID into stack buffer, no heap alloc
                if let Ok((_, network_latency_us)) = &network_result
                    && let Some(org_uuid) = endpoint_cache_uuid.org()
                {
                    let mut endpoint_uuid_buf = [0u8; uuid::fmt::Hyphenated::LENGTH];
                    let endpoint_id = endpoint_uuid.as_hyphenated().encode_lower(&mut endpoint_uuid_buf);
                    let org_uuid_label = org_uuid.eden_uuid::<OrganizationUuid>().to_string();
                    telemetry_wrapper.record(MetricEvent::NetworkLatency {
                        org_uuid: org_uuid_label.as_str(),
                        endpoint_uuid: endpoint_id,
                        endpoint_kind: Self::kind().as_str(),
                        duration_us: *network_latency_us,
                    });
                }

                // Extract just the bytes from the result
                network_result.map(|(bytes, _)| bytes)
            };

            log_trace!(_ctx, "ReadBytes Core: run_request finished",
                audience = eden_logger_internal::LogAudience::Internal,
                elapsed_micros = _t0.elapsed().as_micros()
            );
            result
        }
    }
    #[named]
    fn metadata(
        &self,
        _db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_uuid: &EndpointCacheUuid,
        settings: EdenSettings,
        packages: Option<&[String]>,
        frequency: Option<SyncFrequency>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Value>> + Send
    where
        M: Default,
    {
        async move {
        let mut span = telemetry_wrapper
            .client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        span.add_event(
            "running metadata",
            vec![FastSpanAttribute::new("endpoint", endpoint_cache_uuid.to_string())],
        );

        let conn = self
            .pool()
            .read_conn_async(endpoint_cache_uuid)
            .await?;

        if settings.test() {
            return Ok(json!("PASSED"));
        }

        if let Some(package_list) = packages {
            if package_list.is_empty() {
                return Err(EpError::Metadata(MetadataError::Custom(
                    "packages list must not be empty".to_string(),
                )));
            }
            let timeout = job_timeout_duration();

            if package_list.len() == 1 {
                let package_name = &package_list[0];
                let mut metadata = M::default();
                let job = metadata.package(package_name).ok_or_else(|| {
                    EpError::Metadata(MetadataError::PackageNotFound {
                        package: package_name.to_string(),
                    })
                })?;

                let frequency = job.frequency();
                let batch = run_metadata_jobs(
                    metadata,
                    conn.clone(),
                    vec![job],
                    telemetry_wrapper,
                    frequency,
                    timeout,
                )
                .await;

                let MetadataBatch {
                    frequency: _,
                    started_at,
                    finished_at,
                    reports,
                    had_fatal,
                    data,
                } = batch;

                let package_field = package_name
                    .split('.')
                    .next_back()
                    .unwrap_or(package_name)
                    .to_string();

                let metadata_value = serde_json::to_value(&data).map_err(EpError::serde)?;
                let mut package_data = metadata_value
                    .get(&package_field)
                    .cloned()
                    .unwrap_or(Value::Null);

                if let Value::Object(ref mut obj) = package_data {
                    obj.insert(
                        "collection_priority".to_string(),
                        Value::String(frequency.as_str().to_string()),
                    );
                }

                let response = json!({
                    "package": package_name,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "had_fatal": had_fatal,
                    "reports": reports,
                    "data": package_data,
                });

                return Ok(response);
            }

            let requested: std::collections::HashSet<&str> =
                package_list.iter().map(|p| p.as_str()).collect();
            let collectors = M::collector_info();
            let unknown = unknown_packages(&requested, &collectors);
            if !unknown.is_empty() {
                return Err(EpError::Metadata(MetadataError::PackageNotFound {
                    package: unknown.join(", "),
                }));
            }
            let priorities: BTreeMap<String, String> = collectors
                .into_iter()
                .filter(|info| requested.contains(info.package()))
                .map(|info| {
                    (
                        info.short_name().to_string(),
                        info.frequency().as_str().to_string(),
                    )
                })
                .collect();

            if priorities.is_empty() {
                return Err(EpError::Metadata(MetadataError::PackageNotFound {
                    package: package_list.join(", "),
                }));
            }

            let mut metadata = M::default();
            let mut reports: Vec<JobReport> = Vec::new();
            let mut had_fatal = false;
            let mut started_at: Option<DateTime<Utc>> = None;
            let mut finished_at: Option<DateTime<Utc>> = None;
            let timeout = job_timeout_duration();

            for frequency in [SyncFrequency::High, SyncFrequency::Medium, SyncFrequency::Low] {
                let jobs = metadata.jobs(frequency);
                let selected_jobs: Vec<_> = jobs
                    .into_iter()
                    .filter(|job| requested.contains(job.name()))
                    .collect();

                if selected_jobs.is_empty() {
                    continue;
                }

                let batch = run_metadata_jobs(
                    metadata,
                    conn.clone(),
                    selected_jobs,
                    telemetry_wrapper,
                    frequency,
                    timeout,
                )
                .await;

                let MetadataBatch {
                    frequency: _,
                    started_at: batch_started_at,
                    finished_at: batch_finished_at,
                    reports: batch_reports,
                    had_fatal: batch_had_fatal,
                    data: batch_data,
                } = batch;

                started_at = match started_at {
                    Some(existing) if batch_started_at < existing => Some(batch_started_at),
                    Some(existing) => Some(existing),
                    None => Some(batch_started_at),
                };

                finished_at = match finished_at {
                    Some(existing) if batch_finished_at > existing => Some(batch_finished_at),
                    Some(existing) => Some(existing),
                    None => Some(batch_finished_at),
                };

                had_fatal |= batch_had_fatal;
                reports.extend(batch_reports.into_iter());
                metadata = batch_data;
            }

            let mut metadata_value = serde_json::to_value(&metadata).map_err(EpError::serde)?;
            if let Value::Object(ref mut obj) = metadata_value {
                obj.insert(
                    "collection_priorities".to_string(),
                    serde_json::to_value(&priorities).map_err(EpError::serde)?,
                );
            }

            let response = json!({
                "metadata": metadata_value,
                "reports": reports,
                "had_fatal": had_fatal,
                "started_at": started_at,
                "finished_at": finished_at,
            });

            return Ok(response);
        }

        if let Some(frequency) = frequency {
            let collectors = M::collector_info();
            let priorities: BTreeMap<String, String> = collectors
                .into_iter()
                .filter(|info| info.frequency() == frequency)
                .map(|info| {
                    (
                        info.short_name().to_string(),
                        info.frequency().as_str().to_string(),
                    )
                })
                .collect();

            let mut metadata = M::default();
            let mut reports: Vec<JobReport> = Vec::new();
            let mut had_fatal = false;
            let mut started_at: Option<DateTime<Utc>> = None;
            let mut finished_at: Option<DateTime<Utc>> = None;
            let timeout = job_timeout_duration();

            let jobs = metadata.jobs(frequency);
            if !jobs.is_empty() {
                let batch = run_metadata_jobs(
                    metadata,
                    conn.clone(),
                    jobs,
                    telemetry_wrapper,
                    frequency,
                    timeout,
                )
                .await;

                let MetadataBatch {
                    frequency: _,
                    started_at: batch_started_at,
                    finished_at: batch_finished_at,
                    reports: batch_reports,
                    had_fatal: batch_had_fatal,
                    data: batch_data,
                } = batch;

                started_at = Some(batch_started_at);
                finished_at = Some(batch_finished_at);
                had_fatal = batch_had_fatal;
                reports = batch_reports;
                metadata = batch_data;
            }

            let mut metadata_value = serde_json::to_value(&metadata).map_err(EpError::serde)?;
            if let Value::Object(ref mut obj) = metadata_value {
                obj.insert(
                    "collection_priorities".to_string(),
                    serde_json::to_value(&priorities).map_err(EpError::serde)?,
                );
            }

            let response = json!({
                "metadata": metadata_value,
                "reports": reports,
                "had_fatal": had_fatal,
                "started_at": started_at,
                "finished_at": finished_at,
                "frequency": frequency.as_str(),
            });

            return Ok(response);
        }

        let collectors = M::collector_info();
        let priorities: BTreeMap<String, String> = collectors
            .into_iter()
            .map(|info| {
                (
                    info.short_name().to_string(),
                    info.frequency().as_str().to_string(),
                )
            })
            .collect();

        let mut metadata = M::default();
        let mut reports: Vec<JobReport> = Vec::new();
        let mut had_fatal = false;
        let mut started_at: Option<DateTime<Utc>> = None;
        let mut finished_at: Option<DateTime<Utc>> = None;
        let timeout = job_timeout_duration();

        for frequency in [SyncFrequency::High, SyncFrequency::Medium, SyncFrequency::Low] {
            let jobs = metadata.jobs(frequency);
            if jobs.is_empty() {
                continue;
            }

            let batch =
                run_metadata_jobs(metadata, conn.clone(), jobs, telemetry_wrapper, frequency, timeout)
                    .await;

            let MetadataBatch {
                frequency: _,
                started_at: batch_started_at,
                finished_at: batch_finished_at,
                reports: batch_reports,
                had_fatal: batch_had_fatal,
                data: batch_data,
            } = batch;

            started_at = match started_at {
                Some(existing) if batch_started_at < existing => Some(batch_started_at),
                Some(existing) => Some(existing),
                None => Some(batch_started_at),
            };

            finished_at = match finished_at {
                Some(existing) if batch_finished_at > existing => Some(batch_finished_at),
                Some(existing) => Some(existing),
                None => Some(batch_finished_at),
            };

            had_fatal |= batch_had_fatal;
            reports.extend(batch_reports.into_iter());
            metadata = batch_data;
        }

        let mut metadata_value = serde_json::to_value(&metadata).map_err(EpError::serde)?;
        if let Value::Object(ref mut obj) = metadata_value {
            obj.insert(
                "collection_priorities".to_string(),
                serde_json::to_value(&priorities).map_err(EpError::serde)?,
            );
        }

        let response = json!({
            "metadata": metadata_value,
            "reports": reports,
            "had_fatal": had_fatal,
            "started_at": started_at,
            "finished_at": finished_at,
        });

        Ok(response)
        }
    }
    #[named]
    fn write(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        write: &dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Value>> + Send {
        async move {
        let mut span = telemetry_wrapper
            .client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let downcast_request =
            downcast_request::<A, K, X, Req>(write, &mut span).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned(e.to_string()),
                })
            })?;

        self.validate_operation(downcast_request.operation()).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error {
                message: Cow::Owned(e.to_string()),
            })
        })?;

        // Use the appropriate connection based on request type
        let client = match downcast_request.operation().request_type() {
            ReqType::Read => {
                self.pool()
                    .read_conn_async(endpoint_cache_uuid)
                    .await
                    .inspect_err(|e| {
                        span.set_status(FastSpanStatus::Error {
                            message: Cow::Owned(e.to_string()),
                        })
                    })?
            }
            ReqType::Write => {
                self.pool()
                    .write_conn_async(endpoint_cache_uuid)
                    .await
                    .inspect_err(|e| {
                        span.set_status(FastSpanStatus::Error {
                            message: Cow::Owned(e.to_string()),
                        })
                    })?
            }
        };

        if settings.test() {
            Ok(json!("PASSED"))
        } else {
            downcast_request
                .run_request(client.clone(), settings, telemetry_wrapper)
                .await
                .inspect_err(|e| {
                    span.set_status(FastSpanStatus::Error {
                        message: Cow::Owned(e.to_string()),
                    })
                })?
                .try_serde_serialize()
        }
        }
    }

    /// Write using a one-off ELS connection override instead of the pool.
    #[named]
    fn write_with_conn(
        &self,
        els_conn: Box<dyn EpConnection>,
        config: Box<dyn EpConfig>,
        write: &dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Value>> + Send {
        async move {
        let mut span = telemetry_wrapper
            .client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        span.add_simple_event("running ELS write with override connection");

        let downcast_request = downcast_request::<A, K, X, Req>(write, &mut span).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error {
                message: Cow::Owned(e.to_string()),
            })
        })?;

        self.validate_operation(downcast_request.operation()).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error {
                message: Cow::Owned(e.to_string()),
            })
        })?;

        let concrete_config = downcast_config::<A, C>(config, &mut span)?;
        let client = concrete_config.conn_async(els_conn, telemetry_wrapper).await?;

        if settings.test() {
            Ok(json!("PASSED"))
        } else {
            downcast_request
                .run_request(client, settings, telemetry_wrapper)
                .await
                .inspect_err(|e| {
                    span.set_status(FastSpanStatus::Error {
                        message: Cow::Owned(e.to_string()),
                    })
                })?
                .try_serde_serialize()
        }
        }
    }

    #[named]
    fn write_bytes(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        write: &dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Bytes>> + Send {
        async move {
        let mut span = telemetry_wrapper
            .client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let downcast_request =
            downcast_request::<A, K, X, Req>(write, &mut span).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned(e.to_string()),
                })
            })?;

        self.validate_operation(downcast_request.operation()).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error {
                message: Cow::Owned(e.to_string()),
            })
        })?;

        // Use the appropriate connection based on request type
        let client = match downcast_request.operation().request_type() {
            ReqType::Read => {
                self.pool()
                    .read_conn_async(endpoint_cache_uuid)
                    .await
                    .inspect_err(|e| {
                        span.set_status(FastSpanStatus::Error {
                            message: Cow::Owned(e.to_string()),
                        })
                    })?
            }
            ReqType::Write => {
                self.pool()
                    .write_conn_async(endpoint_cache_uuid)
                    .await
                    .inspect_err(|e| {
                        span.set_status(FastSpanStatus::Error {
                            message: Cow::Owned(e.to_string()),
                        })
                    })?
            }
        };

        if settings.test() {
            Ok(Bytes::from_static(b"PASSED"))
        } else {
            downcast_request
                .run_request(client.clone(), settings, telemetry_wrapper)
                .await
                .inspect_err(|e| {
                    span.set_status(FastSpanStatus::Error {
                        message: Cow::Owned(e.to_string()),
                    })
                })?
                .try_to_bytes()
        }
        }
    }

    /// Process raw wire protocol bytes in a continuous loop.
    ///
    /// This method handles the buffering, parsing, and execution of wire protocol requests
    /// for database protocols that support direct TCP connections (Redis, Postgres, MySQL, etc.).
    ///
    /// # How It Works
    /// 1. Receives raw bytes from the client connection
    /// 2. Buffers bytes until a complete protocol frame is received
    /// 3. Validates and parses the protocol frame
    /// 4. Routes the request through the endpoint (applying migration logic via `tcp_read_bytes`)
    /// 5. Sends the response back to the client
    /// 6. Repeats until the connection closes
    ///
    /// # Migration Integration
    /// This method delegates to `tcp_read_bytes()` which applies `TrafficRouting` rules:
    /// - Routes reads/writes to old database, new database, or both
    /// - Handles dual-write scenarios with consistency policies
    /// - Implements fallback, ratio-based routing, etc.
    ///
    /// # Protocol-Specific Implementation
    /// Each endpoint implements this method using its specific wire protocol:
    /// - Redis: RESP protocol with `RedisProtocol::validate_buffer()` and `RedisBytes`
    /// - Postgres: Postgres protocol with `PostgresProtocol::validate_buffer()` and `PostgresBytes`
    /// - MySQL: MySQL protocol with `MysqlProtocol::validate_buffer()` and `MysqlBytes`
    ///
    /// # Error Handling
    /// Protocol errors are formatted as protocol-specific error responses and sent
    /// back to the client. The connection remains open for subsequent requests.
    ///
    /// # Parameters
    /// * `endpoint_cache_uuid` - Identifies the endpoint configuration (includes migration rules)
    /// * `receiver` - Channel receiving raw bytes from client TCP connection
    /// * `sender` - Channel for sending response bytes back to client
    /// * `settings` - Request settings that may affect routing decisions
    /// * `telemetry_wrapper` - Distributed tracing context
    /// * `ctx` - Logging context for structured logging
    ///
    /// # Returns
    /// Returns when the receiver channel closes (client disconnects)
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    async fn process_wire_protocol(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        sender: UnboundedSender<Vec<u8>>,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: &mut TelemetryWrapper,
        ctx: eden_logger_internal::LogContext,
    );
}

impl<T> EpLifecycleRouter for T
where
    T: EpLifecycleSpec
        + EP<
            <T as EpLifecycleSpec>::Async,
            <T as EpLifecycleSpec>::Config,
            <T as EpLifecycleSpec>::Request,
            <T as EpLifecycleSpec>::Metadata,
            <T as EpLifecycleSpec>::Api,
            <T as EpLifecycleSpec>::Tx,
        > + EpRouter
        + Send
        + Sync
        + 'static,
{
    fn connect_boxed<'a>(
        &'a mut self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()> {
        Box::pin(async move { self.connect_async(endpoint_cache_uuid, config, telemetry_wrapper).await.map(|_| ()) })
    }

    fn disconnect_boxed<'a>(
        &'a mut self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()> {
        Box::pin(async move { self.disconnect_async(endpoint_cache_uuid, telemetry_wrapper).await.map(|_| ()) })
    }

    fn reconnect_boxed<'a>(
        &'a mut self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()> {
        Box::pin(async move { self.reconnect_async(endpoint_cache_uuid, config, telemetry_wrapper).await })
    }

    fn health_check_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()> {
        Box::pin(async move { self.health_check(endpoint_cache_uuid, telemetry_wrapper).await })
    }

    fn test_write_conn_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, ()> {
        Box::pin(async move { self.test_write_conn_async(endpoint_cache_uuid, settings, telemetry_wrapper).await })
    }

    fn read_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        read: &'a mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value> {
        Box::pin(async move { self.read(endpoint_cache_uuid, read, settings, telemetry_wrapper).await })
    }

    fn read_with_conn_boxed<'a>(
        &'a self,
        els_conn: Box<dyn EpConnection>,
        config: Box<dyn EpConfig>,
        read: &'a mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value> {
        Box::pin(async move { self.read_with_conn(els_conn, config, read, settings, telemetry_wrapper).await })
    }

    fn read_bytes_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        read: &'a mut dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Bytes> {
        Box::pin(async move { self.read_bytes(endpoint_cache_uuid, read, settings, telemetry_wrapper).await })
    }

    fn write_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        write: &'a dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value> {
        Box::pin(async move { self.write(endpoint_cache_uuid, write, settings, telemetry_wrapper).await })
    }

    fn write_with_conn_boxed<'a>(
        &'a self,
        els_conn: Box<dyn EpConnection>,
        config: Box<dyn EpConfig>,
        write: &'a dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value> {
        Box::pin(async move { self.write_with_conn(els_conn, config, write, settings, telemetry_wrapper).await })
    }

    fn write_bytes_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        write: &'a dyn EpRequest,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Bytes> {
        Box::pin(async move { self.write_bytes(endpoint_cache_uuid, write, settings, telemetry_wrapper).await })
    }

    fn metadata_boxed<'a>(
        &'a self,
        db_manager: &'a DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        settings: EdenSettings,
        packages: Option<&'a [String]>,
        frequency: Option<SyncFrequency>,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value> {
        Box::pin(async move { self.metadata(db_manager, endpoint_cache_uuid, settings, packages, frequency, telemetry_wrapper).await })
    }

    fn collector_info_boxed(&self) -> Vec<MetadataCollectorInfo> {
        <T::Metadata as SyncMetadata<T::Async>>::collector_info()
    }

    fn transaction_boxed<'a>(
        &'a self,
        endpoint_cache_uuid: &'a EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &'a dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &'a mut TelemetryWrapper,
    ) -> RouterFuture<'a, Value> {
        Box::pin(async move { self.transaction(endpoint_cache_uuid, outbound, inbound, transaction, settings, telemetry_wrapper).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn detects_unknown_packages() {
        let collectors = vec![
            MetadataCollectorInfo::new("alpha.first", SyncFrequency::High),
            MetadataCollectorInfo::new("beta.second", SyncFrequency::Low),
        ];
        let requested: HashSet<&str> = ["alpha.first", "missing", "beta.second"].iter().copied().collect();
        let unknown = unknown_packages(&requested, &collectors);
        assert_eq!(unknown, vec!["missing"]);
    }
}
