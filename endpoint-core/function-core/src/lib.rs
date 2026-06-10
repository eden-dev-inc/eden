#![cfg_attr(test, allow(clippy::unwrap_used))]
//! Function endpoint core for invoking serverless functions.
//! AWS Lambda is the first provider implementation.

pub mod comm;
pub mod config;
pub mod connection;

use comm::FunctionClient;
use deadpool::unmanaged::Pool;

pub use comm::{FunctionInvocationType, FunctionInvokeRequest, FunctionInvokeResponse, FunctionLogType};
pub use connection::FunctionProvider;

/// Type alias for function async client pool (read operations).
pub type FunctionAsync = Pool<FunctionClient>;

/// Type alias for function client pool (write operations).
pub type FunctionTx = Pool<FunctionClient>;
