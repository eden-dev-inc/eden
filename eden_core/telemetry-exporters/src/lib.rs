#![cfg_attr(test, allow(clippy::unwrap_used))]
#![deny(unused_must_use)]

pub mod labels {
    pub use telemetry::labels::*;
}

pub mod metrics {
    pub use telemetry::metrics::*;
}

pub mod tracer;

// Re-export fast-telemetry-export
pub use fast_telemetry_export::{dogstatsd, otlp, spans, sweeper};

pub mod clickhouse;

pub use tracer::initialize_tracer;
