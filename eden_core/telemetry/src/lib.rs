#![cfg_attr(test, allow(clippy::unwrap_used))]
#![deny(unused_must_use)]

//! # Telemetry
//!
//! OpenTelemetry integration for distributed tracing and metrics in Eve.
//!
//! ## Overview
//!
//! This crate provides comprehensive observability through:
//! - **Distributed Tracing** - Track requests across services
//! - **Metrics Collection** - Monitor performance and health
//! - **Shared Telemetry Primitives** - Keep metrics, labels, and trace context reusable
//!
//! ## Core Components
//!
//! ### Tracing ([`traces`])
//!
//! Distributed tracing with OpenTelemetry:
//!
//! ```ignore
//! use telemetry::{TelemetryWrapper, TraceContext, FastSpanStatus};
//!
//! // Create tracer
//! let mut wrapper = TelemetryWrapper::new(metrics, labels, durations);
//!
//! // Start server span
//! let span_context = wrapper.server_tracer("handle_request".into());
//! let span = span;
//!
//! // Add events
//! span.add_simple_event("Processing started");
//!
//! // Nested client span
//! let client_context = wrapper.client_tracer("db_query".into());
//! ```
//!
//! ### Metrics ([`metrics`])
//!
//! Performance and usage metrics:
//!
//! - **[`AllMetrics`](metrics::AllMetrics)** - Container for all metric types
//! - **[`EdenMetrics`](metrics::EdenMetrics)** - Eden service metrics (requests, latency)
//! - **[`EndpointMetrics`](metrics::EndpointMetrics)** - Per-endpoint metrics
//! - **[`IamMetrics`](metrics::IamMetrics)** - User/org metrics
//!
//! ```ignore
//! use telemetry::metrics::AllMetrics;
//!
//! let metrics = AllMetrics::new(meter_provider)?;
//!
//! // Track request
//! metrics.eden_metrics.start_request(&labels);
//! metrics.eden_metrics.end_request(&labels, duration);
//!
//! // Record endpoint operation
//! metrics.endpoint_metrics.record_operation(
//!     endpoint_id,
//!     operation_type,
//!     duration,
//! );
//! ```
//!
//! ### Labels ([`labels`])
//!
//! Structured labels for metrics and traces:
//!
//! ```ignore
//! use telemetry::TelemetryLabels;
//!
//! let labels = TelemetryLabels::new()
//!     .with_org_uuid(org_uuid)
//!     .with_user_id(user_id)
//!     .with_endpoint_type("postgres");
//! ```
//!
//! ### Middleware
//!
//! **[`MetricsMiddleware`]** - Actix-web middleware for automatic HTTP instrumentation:
//!
//! ```ignore
//! use telemetry::MetricsMiddleware;
//! use actix_web::App;
//!
//! let metrics_middleware = MetricsMiddleware::new(metrics.clone());
//!
//! App::new()
//!     .wrap(metrics_middleware)
//!     .service(endpoints);
//! ```
//!
//! Automatically tracks:
//! - Request count per endpoint
//! - Response latency (p50, p95, p99)
//! - Error rates by status code
//! - Active request count
//!
//! ### Function Instrumentation with `#[with_telemetry]`
//!
//! Use the `#[with_telemetry]` attribute macro for automatic span creation:
//!
//! ```ignore
//! use telemetry_extensions_macro::with_telemetry;
//! use actix_web::{web, HttpRequest, Responder};
//!
//! #[with_telemetry]
//! pub async fn get_api(
//!     req: HttpRequest,
//!     api: web::Path<String>,
//!     database: web::Data<DatabaseManager>,
//! ) -> Result<impl Responder, actix_web::Error> {
//!     // Telemetry span is automatically created and managed
//!     // Access via span variable injected by macro
//!     span.add_simple_event("Processing request");
//!
//!     // Your function logic
//!     Ok(web::Json(response))
//! }
//! ```
//!
//! The macro automatically:
//! - Creates a span for the function
//! - Records errors in the span on failure
//! - Properly closes the span when function completes
//!
//! Service-specific exporters live in the `telemetry-exporters` crate so the
//! shared telemetry API stays lightweight for crates that only need metrics and
//! trace context propagation.
//!
//! ## Configuration
//!
//! Environment variables:
//! - `EDEN_OTLP_COLLECTOR` - OpenTelemetry collector endpoint
//! - `EDEN_OTLP_DB_COLLECTOR` - Database collector endpoint
//! - `RUST_LOG` - Logging level (affects trace detail)
//!
//! ## Integration
//!
//! ### Service Setup
//!
//! ```ignore
//! use telemetry::{init_traces, init_metrics, AllMetrics};
//!
//! // Initialize tracing
//! init_traces("eden-service", &otlp_endpoint)?;
//!
//! // Initialize metrics
//! let meter = init_metrics("eden-service", &otlp_endpoint)?;
//! let metrics = Arc::new(AllMetrics::new(meter)?);
//! ```
//!
//! ### Request Tracing
//!
//! ```ignore
//! use telemetry::{FastSpanStatus, TelemetryWrapper};
//!
//! async fn handle_request(
//!     mut telemetry: TelemetryWrapper
//! ) -> Result<Response> {
//!     let span_ctx = telemetry.server_tracer("handle_request".into());
//!     let span = span_ctx.span();
//!
//!     span.add_simple_event("Validating input");
//!     // ... operation ...
//!     span.add_simple_event("Operation complete");
//!
//!     Ok(response)
//! }
//! ```
//!
//! ## Trace Propagation
//!
//! Traces propagate across services via:
//! - **HTTP**: `traceparent` and `tracestate` headers
//! - **gRPC**: Metadata fields
//!
//! [`TraceContext`] handles extraction and injection automatically.

pub mod async_batch;
pub mod connection_tracker;
pub mod duration;
pub mod guards;
pub mod labels;
pub mod metric_event;
pub mod metrics;
pub mod traces;

pub use connection_tracker::{ConnectionGuard, PoolStatusPollerHandle, global_metrics, set_global_metrics, spawn_pool_status_poller};

use actix_web::body::BoxBody;
use actix_web::dev::Payload;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, Responder};
use chrono::{DateTime, Utc};
pub use duration::*;
use format::OrganizationUuid;
use format::timestamp::{DateTimeWrapper, DurationWrapper};
pub use labels::*;
pub use metric_event::{CacheKind, MetricEvent, RecordMetric}; // Easy metric recording
pub use metrics::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::future::{Ready, ready};
use std::sync::Arc;
use tonic::Extensions;
use tonic::metadata::MetadataMap;
pub use traces::*;

// Re-export fast-telemetry span types for convenience
pub use fast_telemetry::span::{
    CompletedSpan as FastCompletedSpan, Span as FastSpan, SpanAttribute as FastSpanAttribute, SpanCollector, SpanEvent as FastSpanEvent,
    SpanKind as FastSpanKind, SpanStatus as FastSpanStatus, SpanValue as FastSpanValue,
};

#[derive(Clone)]
pub struct TelemetryWrapper {
    context: TraceContext,
    metrics: Arc<AllMetrics>,
    labels: TelemetryLabels,
    durations: TelemetryDurations,
}

impl TelemetryWrapper {
    /// Get mutable reference to the trace context (for updating metadata)
    pub fn context_mut(&mut self) -> &mut TraceContext {
        &mut self.context
    }

    pub fn metrics(&self) -> &Arc<AllMetrics> {
        &self.metrics
    }

    /// Get the labels
    pub fn labels(&self) -> TelemetryLabels {
        self.labels.clone()
    }

    pub fn set_org_uuid(&mut self, uuid: OrganizationUuid) {
        self.labels.set_org_uuid(uuid);
    }

    pub fn durations(&self) -> &TelemetryDurations {
        &self.durations
    }

    /// Set Eden request start time (for middleware use)
    pub fn set_eden_request_start(&mut self, start_time: DateTime<Utc>) {
        self.durations.eden_request = Some(start_time.into());
    }

    /// Set Eden request end time and calculate duration (for middleware use)
    pub fn set_eden_request_end(&mut self, end_time: DateTime<Utc>) {
        self.durations.eden_response = Some(end_time.into());
        self.durations.set_eden_duration();
        self.durations.set_exclusive_duration();
    }

    /// Set Endpoint request start time
    pub fn set_endpoint_request_start(&mut self, start_time: DateTime<Utc>) {
        self.durations.endpoint_request = Some(start_time.into());
    }

    /// Set Endpoint request end time and calculate duration
    pub fn set_endpoint_request_end(&mut self, end_time: DateTime<Utc>) {
        self.durations.endpoint_response = Some(end_time.into());
        self.durations.set_endpoint_duration();
    }

    pub fn metadata_map(&self) -> MetadataMapWrapper {
        self.context.metadata.clone().into()
    }

    /// Returns low-cardinality labels as owned string tuples for fast-telemetry.
    pub fn labels_low_cardinality(&self) -> Vec<(String, String)> {
        self.labels.labels_low_cardinality()
    }
}

impl TelemetryWrapper {
    /// New `TelemetryWrapper` with existing metrics
    pub fn new(metrics: Arc<AllMetrics>, labels: TelemetryLabels, durations: TelemetryDurations) -> Self {
        Self { context: TraceContext::new(), metrics, labels, durations }
    }

    /// New `TelemetryWrapper` with existing metrics and `TraceContext`
    pub fn new_with_telemetry(
        context: TraceContext,
        metrics: Arc<AllMetrics>,
        labels: TelemetryLabels,
        durations: TelemetryDurations,
    ) -> Self {
        Self { context, metrics, labels, durations }
    }

    /// Create a client span using fast-telemetry.
    ///
    /// Returns a `FastSpan` that is automatically submitted to the span collector on drop.
    /// The span inherits trace context from the current metadata (traceparent header).
    ///
    /// After creating the span, call `update_traceparent(&span)` to propagate the
    /// trace context to any downstream calls.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut span = telemetry_wrapper.client_tracer("db_query".to_string());
    /// span.add_simple_event("executing query");
    /// // ... do work ...
    /// span.set_status(FastSpanStatus::Ok);
    /// // span automatically submitted on drop
    /// ```
    pub fn client_tracer(&mut self, name: impl Into<Cow<'static, str>>) -> FastSpan {
        let span = self.start_span(name, FastSpanKind::Client);
        self.update_traceparent(&span);
        span
    }

    /// Create a client span with gRPC metadata attributes.
    ///
    /// Like `client_tracer()` but also extracts gRPC metadata headers as span
    /// attributes. Use this for gRPC handlers where metadata context is valuable.
    /// For high-throughput paths (proxies), prefer `client_tracer()` instead.
    pub fn client_tracer_with_metadata(&mut self, name: impl Into<Cow<'static, str>>) -> FastSpan {
        let mut span = self.start_span(name, FastSpanKind::Client);
        self.add_metadata_attributes(&mut span);
        self.update_traceparent(&span);
        span
    }

    /// Create a server span using fast-telemetry.
    ///
    /// Returns a `FastSpan` that is automatically submitted to the span collector on drop.
    /// The span inherits trace context from the current metadata (traceparent header).
    ///
    /// After creating the span, call `update_traceparent(&span)` to propagate the
    /// trace context to any downstream calls.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut span = telemetry_wrapper.server_tracer("handle_request".to_string());
    /// span.add_simple_event("processing request");
    /// // ... do work ...
    /// span.set_status(FastSpanStatus::Ok);
    /// // span automatically submitted on drop
    /// ```
    pub fn server_tracer(&mut self, name: String) -> FastSpan {
        let span = self.start_span(name, FastSpanKind::Server);
        self.update_traceparent(&span);
        span
    }

    /// Create a server span with gRPC metadata attributes.
    ///
    /// Like `server_tracer()` but also extracts gRPC metadata headers as span
    /// attributes. Use this for gRPC handlers where metadata context is valuable.
    /// For high-throughput paths (proxies), prefer `server_tracer()` instead.
    pub fn server_tracer_with_metadata(&mut self, name: String) -> FastSpan {
        let mut span = self.start_span(name, FastSpanKind::Server);
        self.add_metadata_attributes(&mut span);
        self.update_traceparent(&span);
        span
    }

    /// Mutate the durations with a closure
    pub fn mut_durations(&mut self, f: impl FnOnce(&mut TelemetryDurations)) {
        f(&mut self.durations);
    }

    /// Mutate the labels with a closure
    pub fn mut_labels(&mut self, f: impl FnOnce(&mut TelemetryLabels)) {
        f(&mut self.labels);
    }

    #[inline]
    pub fn record_event(&mut self, event: MetricEvent) {
        self.record(event);
    }

    /// Resets the trace context to start fresh spans that are not nested under
    /// the previous parent span. Use this at the start of each iteration in
    /// long-running loops to prevent deeply nested trace trees.
    ///
    /// This preserves the metrics, labels, and durations but clears the trace
    /// metadata so subsequent `client_tracer()` or `server_tracer()` calls
    /// create root-level spans rather than nested children.
    pub fn reset_trace_context(&mut self) {
        self.context = TraceContext::new();
    }

    // =========================================================================
    // Fast-telemetry span methods
    // =========================================================================

    /// Get the span collector for creating fast-telemetry spans.
    pub fn span_collector(&self) -> &Arc<SpanCollector> {
        self.metrics.span_collector()
    }

    /// Create a new fast-telemetry span from the current trace context.
    ///
    /// Extracts the `traceparent` header from the metadata to establish
    /// parent-child relationships across service boundaries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut span = telemetry_wrapper.start_span("db_query", FastSpanKind::Client);
    /// span.enter(); // Set as current span for logging
    /// span.set_attribute("db.system", "postgres");
    /// // span automatically submitted on drop
    /// ```
    pub fn start_span(&self, name: impl Into<Cow<'static, str>>, kind: FastSpanKind) -> FastSpan {
        let traceparent = self.context.metadata.get("traceparent").and_then(|v| v.to_str().ok());
        let mut span = self.metrics.span_collector().start_span_from_traceparent(traceparent, name, kind);
        for (key, value) in self.labels_low_cardinality() {
            span.set_attribute(key, value);
        }
        span
    }

    /// Create a new server span from the current trace context.
    ///
    /// Convenience method for `start_span(name, FastSpanKind::Server)`.
    pub fn start_server_span(&self, name: impl Into<Cow<'static, str>>) -> FastSpan {
        self.start_span(name, FastSpanKind::Server)
    }

    /// Create a new client span from the current trace context.
    ///
    /// Convenience method for `start_span(name, FastSpanKind::Client)`.
    pub fn start_client_span(&self, name: impl Into<Cow<'static, str>>) -> FastSpan {
        self.start_span(name, FastSpanKind::Client)
    }

    /// Add metadata headers as span attributes.
    ///
    /// Extracts gRPC metadata headers and adds them as `metadata.*` attributes
    /// on the span. Authorization headers are sanitized to prevent credential leakage.
    fn add_metadata_attributes(&self, span: &mut FastSpan) {
        span.set_attribute("component", "grpc");

        for key_ref in self.context.metadata.keys() {
            let key_str = match key_ref {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(_) => continue,
            };

            if let Ok(key) = key_str.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
                && let Some(value_ref) = self.context.metadata.get(&key)
                && let Ok(value_str) = value_ref.to_str()
            {
                // Sanitize authorization header to prevent credential leakage
                let sanitized_value = if key_str == "authorization" {
                    if value_str.starts_with("Bearer ") {
                        "Bearer ***"
                    } else if value_str.starts_with("Basic ") {
                        "Basic ***"
                    } else {
                        "***"
                    }
                } else {
                    value_str
                };
                span.set_attribute(format!("metadata.{}", key_str), sanitized_value.to_string());
            }
        }
    }

    /// Update the trace context metadata with a new traceparent from a span.
    ///
    /// Call this after creating a span to propagate the trace context to
    /// downstream calls. This updates the metadata so subsequent spans
    /// created via `start_span()` will be children of the given span.
    pub fn update_traceparent(&mut self, span: &FastSpan) {
        let traceparent = span.traceparent();
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(b"traceparent")
            && let Ok(value) = tonic::metadata::MetadataValue::try_from(&traceparent)
        {
            // Remove old traceparent first
            self.context.metadata.remove("traceparent");
            self.context.metadata.insert(key, value);
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TraceContext {
    metadata: MetadataMap,
    extensions: Arc<Extensions>,
}

impl From<(MetadataMap, Extensions)> for TraceContext {
    fn from((metadata, extensions): (MetadataMap, Extensions)) -> Self {
        Self { metadata, extensions: Arc::new(extensions) }
    }
}

impl From<MetadataMap> for TraceContext {
    fn from(metadata: MetadataMap) -> Self {
        Self { metadata, extensions: Default::default() }
    }
}

#[derive(Clone, Default)]
pub struct MetadataMapWrapper(MetadataMap);

impl MetadataMapWrapper {
    pub fn metadata(&self) -> &MetadataMap {
        &self.0
    }
}

impl From<MetadataMap> for MetadataMapWrapper {
    fn from(metadata_map: MetadataMap) -> Self {
        Self(metadata_map)
    }
}

impl FromRequest for MetadataMapWrapper {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        // Try to get the pre-populated MetadataMap from request extensions first
        if let Some(metadata) = req.extensions().get::<MetadataMapWrapper>() {
            ready(Ok(metadata.clone()))
        } else {
            // Fallback: create default MetadataMap if none exists in extensions
            // This shouldn't happen if the middleware is properly set up
            let metadata = MetadataMapWrapper::default();
            ready(Ok(metadata))
        }
    }
}

impl TraceContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }
    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.metadata
    }
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// Telemetry durations, where all measurements should be made in nanoseconds
pub struct TelemetryDurations {
    // start of eden request
    eden_request: Option<DateTimeWrapper>,
    // end of eden request
    eden_response: Option<DateTimeWrapper>,
    // eden full duration
    eden_duration: Option<DurationWrapper>,
    // start of endpoint request
    endpoint_request: Option<DateTimeWrapper>,
    // end of endpoint request
    endpoint_response: Option<DateTimeWrapper>,
    // endpoint full duration
    endpoint_duration: Option<DurationWrapper>,

    // exclusive eden duration (eden - endpoint)
    exclusive_duration: Option<DurationWrapper>,
}

impl Responder for TelemetryDurations {
    type Body = BoxBody;

    fn respond_to(self, _request: &HttpRequest) -> HttpResponse {
        HttpResponse::Ok().json(self)
    }
}

impl FromRequest for TelemetryDurations {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        if let Some(durations) = req.extensions().get::<TelemetryDurations>() {
            ready(Ok(durations.clone()))
        } else {
            ready(Ok(TelemetryDurations::default()))
        }
    }
}

impl TelemetryDurations {
    #[allow(dead_code)]
    fn new() -> Self {
        Self::default()
    }

    pub fn get_eden_request(&self) -> &Option<DateTimeWrapper> {
        &self.eden_request
    }

    /// Set eden request time
    pub fn set_eden_request(&mut self, start_time: DateTime<Utc>) {
        self.eden_request = Some(start_time.into());
    }

    pub fn get_eden_response(&self) -> &Option<DateTimeWrapper> {
        &self.eden_response
    }

    /// Set eden response time and calculate duration
    pub fn set_eden_response(&mut self, end_time: DateTime<Utc>) {
        self.eden_response = Some(end_time.into());
        self.set_eden_duration();
        self.set_exclusive_duration();
    }

    pub fn get_eden_duration(&self) -> &Option<DurationWrapper> {
        &self.eden_duration
    }

    /// Internal: Calculate eden duration from request/response times
    fn set_eden_duration(&mut self) {
        if let (Some(eden_response), Some(eden_request)) = (self.eden_response.as_ref(), self.eden_request.as_ref()) {
            let duration = eden_response.as_datetime().signed_duration_since(eden_request.as_datetime());
            self.eden_duration.replace(duration.into());
        }
    }

    /// set endpoint request time
    pub fn set_endpoint_request(&mut self, start: DateTime<Utc>) {
        self.endpoint_request.replace(start.into());
    }

    pub fn get_endpoint_request(&self) -> &Option<DateTimeWrapper> {
        &self.endpoint_request
    }

    /// set endpoint response time
    pub fn set_endpoint_response(&mut self, finish: DateTime<Utc>) {
        self.endpoint_response.replace(finish.into());

        self.set_endpoint_duration();
    }

    pub fn get_endpoint_response(&self) -> &Option<DateTimeWrapper> {
        &self.endpoint_response
    }

    /// Internal: Calculate endpoint duration from request/response times
    fn set_endpoint_duration(&mut self) {
        if let (Some(endpoint_response), Some(endpoint_request)) = (self.endpoint_response.as_ref(), self.endpoint_request.as_ref()) {
            self.endpoint_duration
                .replace(endpoint_response.as_datetime().signed_duration_since(endpoint_request.as_datetime()).into());
        }
    }

    pub fn get_endpoint_duration(&self) -> &Option<DurationWrapper> {
        &self.endpoint_duration
    }

    /// when eden request finishes, endpoint request should already have been completed
    fn set_exclusive_duration(&mut self) {
        if let Some(eden_duration) = self.eden_duration.as_ref()
            && let Some(diff) = eden_duration
                .as_duration()
                .checked_sub(&self.endpoint_duration.as_ref().unwrap_or(&DurationWrapper::default()).as_duration().abs())
        {
            self.exclusive_duration.replace(diff.into());
        }
    }

    /// Merge durations from a handler response, preserving middleware-set eden timings
    /// but allowing handlers to set endpoint timings
    pub(crate) fn merge_from_handler(&mut self, handler_durations: TelemetryDurations) {
        // Preserve eden_request and eden_response from middleware - DO NOT overwrite
        // (handlers should not be setting these)

        // Allow handlers to set endpoint timings
        if handler_durations.endpoint_request.is_some() {
            self.endpoint_request = handler_durations.endpoint_request;
        }
        if handler_durations.endpoint_response.is_some() {
            self.endpoint_response = handler_durations.endpoint_response;
        }
        if handler_durations.endpoint_duration.is_some() {
            self.endpoint_duration = handler_durations.endpoint_duration;
        }

        // Recalculate exclusive duration if needed
        if self.eden_duration.is_some() && self.endpoint_duration.is_some() {
            self.set_exclusive_duration();
        }
    }
}
