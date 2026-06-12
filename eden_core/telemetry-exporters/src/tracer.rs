use std::env;
use std::time::Duration;

use eden_logger_internal::{LogAudience, ctx_with_trace, log_info};
use function_name::named;
use opentelemetry::global;
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;

#[named]
pub async fn initialize_tracer(service_name: &str, collector_endpoint: &str, _db_collector_endpoint: &str) -> SdkTracerProvider {
    global::set_text_map_propagator(TraceContextPropagator::new());

    unsafe {
        env::set_var("OTEL_SERVICE_NAME", service_name);
    }

    let ctx = ctx_with_trace!().with_feature("telemetry");

    let opt_provider = if !collector_endpoint.trim().is_empty() {
        match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_export_config(ExportConfig {
                endpoint: Some(collector_endpoint.to_owned()),
                protocol: opentelemetry_otlp::Protocol::Grpc,
                timeout: Some(Duration::from_secs(3)),
            })
            .build()
        {
            Ok(otlp_exporter) => {
                log_info!(
                    ctx.clone(),
                    "OTLP collector found, sending telemetry data",
                    audience = LogAudience::Internal,
                    collector_endpoint = collector_endpoint
                );
                Some(SdkTracerProvider::builder().with_batch_exporter(otlp_exporter).build())
            }
            Err(err) => {
                log_info!(
                    ctx.clone(),
                    "OTLP collector exporter could not be configured; tracing export disabled",
                    audience = LogAudience::Internal,
                    collector_endpoint = collector_endpoint,
                    error = err.to_string()
                );
                None
            }
        }
    } else {
        None
    };

    let provider = opt_provider.unwrap_or_else(|| {
        log_info!(
            ctx,
            "OTLP tracing export disabled",
            audience = LogAudience::Internal,
            collector_endpoint = collector_endpoint
        );

        SdkTracerProvider::builder().build()
    });
    let _ = global::set_tracer_provider(provider.clone());
    provider
}
