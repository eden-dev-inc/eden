#[allow(unused_imports)]
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug};
use function_name::named;
use opentelemetry::global::BoxedSpan;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{Span, SpanKind};
use opentelemetry::{Context, KeyValue, global, trace::Tracer};
use std::time::SystemTime;

use tonic::{Extensions, metadata::MetadataMap};

pub struct MetadataMapWrapper<'a>(&'a mut MetadataMap);

impl Injector for MetadataMapWrapper<'_> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
            && let Ok(val) = tonic::metadata::MetadataValue::try_from(&value)
        {
            self.0.insert(key, val);
        }
    }
}

#[named]
pub fn metadata_map_from_context(cx: &Context, original_metadata: &MetadataMap) -> MetadataMap {
    // Clone original to preserve all headers (user-agent, content-type, etc.)
    let mut metadata = original_metadata.clone();

    // Remove old trace propagation headers before injecting new ones
    metadata.remove("traceparent");
    metadata.remove("tracestate");

    let mut wrapper = MetadataMapWrapper(&mut metadata);
    global::get_text_map_propagator(|propagator| propagator.inject_context(cx, &mut wrapper));

    let _ctx = ctx_with_trace!().with_feature("telemetry");

    log_debug!(
        _ctx,
        "Metadata map updated with new trace context while preserving original headers",
        audience = LogAudience::Internal,
        metadata = format!("{:?}", metadata)
    );

    metadata
}

/// Extract trace context and relevant headers from HTTP request into MetadataMap
pub fn metadata_map_from_http_headers(headers: &actix_web::http::header::HeaderMap) -> MetadataMap {
    let mut metadata = MetadataMap::new();

    // List of headers to extract (matching the allowlist in span attributes)
    const HEADERS_TO_EXTRACT: &[&str] = &[
        "traceparent",
        "tracestate",
        "user-agent",
        "x-request-id",
        "x-correlation-id",
        "content-type",
        "authorization",
    ];

    for header_name in HEADERS_TO_EXTRACT {
        if let Some(header_value) = headers.get(*header_name)
            && let Ok(value_str) = header_value.to_str()
            && let Ok(key) = tonic::metadata::MetadataKey::from_bytes(header_name.as_bytes())
            && let Ok(value) = tonic::metadata::MetadataValue::try_from(value_str)
        {
            metadata.insert(key, value);
        }
    }

    log::debug!("Metadata map created from HTTP headers: {:?}", metadata);

    metadata
}

struct MetaMap<'a>(&'a MetadataMap);

impl<'a> Extractor for MetaMap<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

#[named]
pub fn server_tracer_config(name: String, md: &MetadataMap, _ex: &Extensions) -> BoxedSpan {
    // let extensions = ex.get::<Arc<ConnInfo>>().unwrap_or_default();
    let parent_cx = global::get_text_map_propagator(|prop| prop.extract(&MetaMap(md)));

    let _ctx = ctx_with_trace!().with_feature("telemetry");

    log_debug!(
        _ctx.clone(),
        "parent_context for server_span",
        audience = LogAudience::Internal,
        parent_context = format!("{:?}", parent_cx)
    );

    let tracer = global::tracer(name.clone());

    // Extract structured metadata attributes
    let mut attributes = Vec::with_capacity(md.len() + 1);
    attributes.push(KeyValue::new("component", "grpc"));

    for key_ref in md.keys() {
        let key_str = match key_ref {
            tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
            tonic::metadata::KeyRef::Binary(_) => continue,
        };

        if let Ok(key) = key_str.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
            && let Some(value_ref) = md.get(&key)
            && let Ok(value_str) = value_ref.to_str()
        {
            // Only sanitize authorization header to prevent credential leakage
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
            attributes.push(KeyValue::new(format!("metadata.{}", key_str), sanitized_value.to_string()));
        }
    }

    // let certs: Vec<KeyValue> = extensions
    //     .certificates
    //     .iter()
    //     .map(|c| KeyValue::new("certificate", hex::encode(c)))
    //     .collect();

    let span = tracer
        .span_builder(name)
        .with_kind(SpanKind::Server)
        .with_attributes(attributes)
        .with_start_time(SystemTime::now())
        .start_with_context(&tracer, &parent_cx);

    log_debug!(
        _ctx,
        "server_span created",
        audience = LogAudience::Internal,
        span_context = format!("{:?}", span.span_context())
    );

    span
}

#[named]
pub fn client_tracer_config(name: String, md: &MetadataMap) -> BoxedSpan {
    let parent_cx = global::get_text_map_propagator(|prop| prop.extract(&MetaMap(md)));
    let tracer = global::tracer(name.clone());

    let _ctx = ctx_with_trace!().with_feature("telemetry");

    // Extract structured metadata attributes
    let mut attributes = Vec::with_capacity(md.len() + 1);
    attributes.push(KeyValue::new("component", "grpc"));

    for key_ref in md.keys() {
        let key_str = match key_ref {
            tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
            tonic::metadata::KeyRef::Binary(_) => continue,
        };

        if let Ok(key) = key_str.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
            && let Some(value_ref) = md.get(&key)
            && let Ok(value_str) = value_ref.to_str()
        {
            // Only sanitize authorization header to prevent credential leakage
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
            attributes.push(KeyValue::new(format!("metadata.{}", key_str), sanitized_value.to_string()));
        }
    }

    let span = tracer
        .span_builder(name)
        .with_kind(SpanKind::Client)
        .with_attributes(attributes)
        .with_start_time(SystemTime::now())
        .start_with_context(&tracer, &parent_cx);

    log_debug!(
        _ctx,
        "client_span created",
        audience = LogAudience::Internal,
        span_context = format!("{:?}", span.span_context())
    );

    span
}

/// Creates a client span that inherits from the currently active context
/// and preserves metadata attributes from the provided MetadataMap
pub fn client_tracer_with_current_context(name: String, md: &MetadataMap) -> BoxedSpan {
    // Use Context::current() to get the currently attached context (from the handler span)
    let parent_cx = Context::current();
    let tracer = global::tracer(name.clone());

    // Extract structured metadata attributes
    let mut attributes = Vec::with_capacity(md.len() + 1);
    attributes.push(KeyValue::new("component", "grpc"));

    for key_ref in md.keys() {
        let key_str = match key_ref {
            tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
            tonic::metadata::KeyRef::Binary(_) => continue,
        };

        if let Ok(key) = key_str.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
            && let Some(value_ref) = md.get(&key)
            && let Ok(value_str) = value_ref.to_str()
        {
            // Only sanitize authorization header to prevent credential leakage
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
            attributes.push(KeyValue::new(format!("metadata.{}", key_str), sanitized_value.to_string()));
        }
    }

    let span = tracer
        .span_builder(name)
        .with_kind(SpanKind::Client)
        .with_attributes(attributes)
        .with_start_time(SystemTime::now())
        .start_with_context(&tracer, &parent_cx);

    log::debug!("client_span created with current context: {:?}", span.span_context());

    span
}
