#![cfg_attr(test, allow(clippy::unwrap_used))]
//! ClickHouse push infrastructure (batching, retry, circuit breaker).

pub mod batch;
pub mod circuit_breaker;
pub mod clickhouse;
pub mod retry;
pub mod stats;

pub use batch::BatchBuffer;
pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use clickhouse::insert_batch;
pub use retry::{RetryConfig, with_retry};
pub use stats::DeadLetterStats;
