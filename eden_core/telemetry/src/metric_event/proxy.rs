use crate::AllMetrics;
use crate::labels::{LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL};
use std::sync::Arc;

/// Record proxy-related metric events
pub(super) fn record_proxy_event(event: &super::MetricEvent, metrics: &Arc<AllMetrics>) {
    use super::MetricEvent;

    match event {
        MetricEvent::ProxyRequest {
            org_uuid,
            interlay_uuid,
            endpoint_uuid,
            command_type,
            duration_us,
            bytes_read,
            bytes_written,
            command_count,
        } => {
            if let Some(cmd) = command_type {
                let labels: &[(&str, &str)] = &[
                    ("org_uuid", org_uuid),
                    ("interlay_uuid", interlay_uuid),
                    ("endpoint_uuid", endpoint_uuid),
                    ("command_type", cmd),
                    (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL),
                ];
                metrics.proxy().record_request(labels);
                metrics.proxy().record_commands(*command_count, labels);
                metrics.proxy().record_duration(*duration_us, labels);
                metrics.proxy().record_bytes_read(*bytes_read, labels);
                metrics.proxy().record_bytes_written(*bytes_written, labels);
            } else {
                let labels: &[(&str, &str)] = &[
                    ("org_uuid", org_uuid),
                    ("interlay_uuid", interlay_uuid),
                    ("endpoint_uuid", endpoint_uuid),
                    (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL),
                ];
                metrics.proxy().record_request(labels);
                metrics.proxy().record_commands(*command_count, labels);
                metrics.proxy().record_duration(*duration_us, labels);
                metrics.proxy().record_bytes_read(*bytes_read, labels);
                metrics.proxy().record_bytes_written(*bytes_written, labels);
            }
        }

        MetricEvent::NetworkLatency { org_uuid, endpoint_uuid, endpoint_kind, duration_us } => {
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("endpoint_uuid", endpoint_uuid),
                ("endpoint_kind", endpoint_kind),
                (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL),
            ];
            metrics.proxy().record_network_latency(*duration_us, labels);
        }

        MetricEvent::ProxyError { org_uuid, interlay_uuid, error_type } => {
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("interlay_uuid", interlay_uuid),
                ("error_type", error_type),
                (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL),
            ];
            metrics.proxy().record_error(labels);
        }

        MetricEvent::ProxyConnectionFailure { org_uuid, interlay_uuid, error_type } => {
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("interlay_uuid", interlay_uuid),
                ("error_type", error_type),
                (LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL),
            ];
            metrics.proxy().record_connection_failure(labels);
        }

        _ => {
            // This should never happen since we pattern match all proxy events in mod.rs
            // But if it does, it's a no-op
        }
    }
}
