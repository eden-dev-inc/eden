#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Eden Core Error Handling
//!
//! Standardized error types and result aliases for the Eve system.
//!
//! ## Overview
//!
//! This crate provides a comprehensive error handling framework for Eve,
//! ensuring consistent error reporting across all system components. All Eden
//! operations should use these error types rather than ad-hoc error handling.
//!
//! ## Architecture
//!
//! The error system is organized into domain-specific error types:
//!
//! - [`EpError`]: Endpoint-related errors (connection failures, query errors, etc.)
//! - [`DBError`]: Database management errors (CRUD operations, cache failures)
//! - [`CommonError`]: Cross-cutting errors (authentication, RBAC, validation)
//! - [`VerificationError`]: Data verification and validation errors
//!
//! Each error type implements `std::error::Error` and provides detailed context
//! about the failure, including the operation attempted and the underlying cause.
//!
//! ## Core Concepts
//!
//! ### Error Propagation
//!
//! Eden uses `Result` types extensively. Each domain has its own `Result` alias:
//!
//! - [`ResultEP<T>`]: Alias for `Result<T, EpError>`
//! - [`ResultDB<T>`]: Alias for `Result<T, DBError>`
//! - [`ResultCommon<T>`]: Alias for `Result<T, CommonError>`
//!
//! ### Error Context
//!
//! Errors carry contextual information to aid debugging:
//! - Operation being performed
//! - Resource identifiers (endpoint ID, user ID, etc.)
//! - Underlying error cause
//! - Suggested remediation when applicable
//!
//! ## Usage Examples
//!
//! ```rust
//! use error::{EpError, ResultEP, ConnectError};
//!
//! fn connect_to_endpoint(endpoint_id: &str) -> ResultEP<()> {
//!     // Operation that might fail
//!     if endpoint_id.is_empty() {
//!         return Err(EpError::Connect(ConnectError::ConnectionNotFound));
//!     }
//!     Ok(())
//! }
//!
//! // Error propagation with ?
//! fn perform_operation() -> ResultEP<()> {
//!     connect_to_endpoint("ep_123")?;
//!     // Continue with operation
//!     Ok(())
//! }
//! ```
//!
//! ## Error Conversion
//!
//! Eden errors implement `From` traits for automatic conversion from
//! underlying library errors (e.g., `sqlx::Error`, `redis::RedisError`).
//!
//! ## HTTP Error Mapping
//!
//! In `eden_service`, errors are automatically converted to HTTP responses
//! via the top-level error handling middleware. See the `eden_service::error_handling`
//! function for implementation details.

mod common;
mod db;
mod ep;
mod logging;
mod types;
mod verification;

pub use common::{CommonError, EntityType, RbacErrorType, ResultCommon};
pub use db::{DBError, ResultDB};
pub use ep::{EpError, ResultEP};
pub use types::*;
pub use verification::{ResultVerification, VerificationError};
