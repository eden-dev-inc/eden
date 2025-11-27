// Analytics Demo Library
//
// Enhanced library for high-performance analytics simulation with 10K+ QPS support

pub mod config;
pub mod database;
pub mod generators;
pub mod metrics;
pub mod models;
pub mod workers;

// Re-export commonly used types
pub use config::Config;
pub use database::{Database, RedisCache};
pub use generators::DataGenerator;
pub use metrics::AppMetrics;
pub use models::*;
pub use workers::*;