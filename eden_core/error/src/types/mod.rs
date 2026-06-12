//! Specific error type definitions for each error category.
//!
//! This module contains individual error enums for each domain in Eve.
//! Each error type has its own error codes (0x01-0xFF) within its category.

// Individual error type modules
pub mod api;
pub mod auth;
pub mod cache;
pub mod connection;
pub mod data;
pub mod database;
pub mod fs;
pub mod init;
pub mod interlay;
pub mod io;
pub mod lock;
pub mod metadata;
pub mod parse;
pub mod protocol;
pub mod provider;
pub mod rbac;
pub mod redis;
pub mod request;
pub mod serde;
pub mod template;
pub mod timeout;
pub mod tools;
pub mod transaction;
pub mod workflow;

// Re-export all error types
pub use api::ApiError;
pub use auth::AuthError;
pub use cache::CacheError;
pub use connection::ConnectError;
pub use data::DataError;
pub use database::{DatabaseError, DatabaseType};
pub use fs::FsError;
pub use init::InitError;
pub use interlay::InterlayError;
pub use io::IoError;
pub use lock::LockError;
pub use metadata::MetadataError;
pub use parse::ParseError;
pub use protocol::ProtocolError;
pub use provider::LlmProviderError;
pub use rbac::RbacError;
pub use redis::RedisError;
pub use request::RequestError;
pub use serde::SerdeError;
pub use template::TemplateError;
pub use timeout::TimeoutError;
pub use tools::ToolsError;
pub use transaction::TransactionError;
pub use workflow::WorkflowError;
