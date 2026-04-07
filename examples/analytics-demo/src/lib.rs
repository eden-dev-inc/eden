// Analytics Server Demo Library
//
// Enhanced library for high-performance analytics simulation with 10K+ QPS support.
// Code is organized into redis/ and postgres/ modules with runtime backend selection.

pub mod activity;
pub mod config;
pub mod generators;
pub mod metrics;
pub mod models;
pub mod runtime_controls;
pub mod telemetry;
pub mod validation;
pub mod workers;

mod datadog;
pub mod postgres;
pub mod redis;

// Re-export commonly used types
pub use config::Config;
pub use generators::DataGenerator;
pub use metrics::AppMetrics;
pub use models::*;
pub use postgres::Database;
pub use redis::RedisCache;
pub use telemetry::{
    install_legacy_telemetry_env_aliases, parse_telemetry_provider, TelemetryOptions,
    TelemetryProvider, TelemetryRuntime, TelemetrySpan, TelemetrySpanKind, TelemetryTracer,
};
pub use validation::{run_runtime_validation, ValidationOptions, ValidationReport};
pub use workers::*;
