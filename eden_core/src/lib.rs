#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Eden Core
//!
//! Core foundational library for the Eve.
//!
//! ## Overview
//!
//! Eden Core provides the essential building blocks used throughout the Eve ecosystem.
//! It contains shared types, utilities, and abstractions that enable the Eden system to provide
//! a unified API across multiple database types while maintaining strong type safety and
//! comprehensive error handling.
//!
//! ## Architecture
//!
//! Eden Core is organized into focused modules, each handling a specific aspect of the system:
//!
//! ### Core Modules
//!
//! - **[`auth`]** - Authentication and authorization primitives
//!   - JWT token generation and validation
//!   - Password hashing and verification
//!   - Bearer token support
//!   - Organization-scoped authentication
//!
//! - **[`error`]** - Standardized error types
//!   - [`EpError`](error::EpError) - Endpoint operation errors
//!   - [`DBError`](error::DBError) - Database management errors
//!   - [`CommonError`](error::CommonError) - Cross-cutting errors
//!   - Consistent error handling across all Eden components
//!
//! - **[`format`]** - Data format utilities and type-safe identifiers
//!   - UUID wrappers for different entity types
//!   - Cache ID types for Redis operations
//!   - Timestamp handling
//!   - RBAC subject types
//!
//! - **[`request`] / [`response`]** - Request and response structures
//!   - Shared data types for HTTP requests
//!   - Standardized response formats
//!   - Serialization support
//!
//! ### Communication & Telemetry
//!
//! - **[`comm`]** - Communication and node management
//!   - Node data structures
//!   - Inter-service communication types
//!
//! - **[`telemetry`]** - OpenTelemetry integration
//!   - Distributed tracing
//!   - Metrics collection
//!   - Performance monitoring
//!   - OTLP (OpenTelemetry Protocol) support
//!
//! - **[`proto`]** - Protocol buffer definitions
//!   - gRPC service definitions
//!   - Message types for inter-service communication
//!
//! ### Data Handling
//!
//! - **[`json`]** - JSON manipulation utilities
//!   - Filtering
//!   - Mapping
//!   - Parsing
//!   - Reducing
//!
//! - **[`db`]** - Database abstractions
//!   - Connection management
//!   - Query building
//!   - Transaction support
//!
//! - **[`block`]** - Block-based data structures
//!   - Immutable data blocks
//!   - Cryptographic hashing
//!
//! ### Developer Tools
//!
//! - **[`macros`]** - Procedural macros
//!   - Code generation helpers
//!   - Boilerplate reduction
//!
//! - **[`telemetry_macro`]** - Telemetry-specific macros
//!   - Automatic instrumentation
//!   - Span generation
//!
//! ## Integration with Other Eden Components
//!
//! Eden Core is used by:
//!
//! - **`eden_service`** - Main HTTP API service
//!   - Uses error types for HTTP error mapping
//!   - Uses auth types for authentication middleware
//!   - Uses telemetry for request tracing
//!
//! - **`database`** - Database management layer
//!   - Uses error types for database operations
//!   - Uses format types for cache keys
//!   - Uses telemetry for query monitoring
//!
//! - **`communication`** - gRPC communication layer
//!   - Uses proto definitions for service interfaces
//!   - Uses error types for RPC errors
//!   - Uses telemetry for distributed tracing
//!
//! - **`endpoint-core`** - Endpoint abstractions
//!   - Uses format types for endpoint IDs
//!   - Uses error types for connection failures
//!   - Uses db abstractions for query building

pub use auth;
pub use block;
pub use comm;
pub use db;
pub use error;
pub use format;
pub use json;
pub use macros;
pub use node;
pub use proto;
pub use request;
pub use response;
pub use telemetry;
pub use telemetry_macro;
