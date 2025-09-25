// Analytics Demo Library
//
// This module organizes all the components needed for the analytics simulation.
// It provides a clean interface for the main application to use all the
// database, caching, metrics, and worker functionality.

pub mod config;
pub mod database;
pub mod generators;
pub mod metrics;
pub mod models;
pub mod workers;

// Re-export commonly used types for convenience
pub use config::Config;
pub use database::{Database, RedisCache};
pub use generators::DataGenerator;
pub use metrics::AppMetrics;
pub use models::*;
pub use workers::*;