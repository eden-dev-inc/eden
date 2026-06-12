#![allow(async_fn_in_trait)]

//! Weaviate Wire Protocol parser (HTTP REST + gRPC).
//!
//! This crate provides parsing for Weaviate's protocols:
//! - HTTP REST API (default port 8080)
//! - gRPC API (default port 50051)
//!
//! # Features
//!
//! - **Route classification**: Identify which API endpoint is being hit
//! - **Read/write detection**: Classify operations for proxy routing
//! - **Header extraction**: Parse auth, tenant, and module API keys
//! - **gRPC method classification**: Classify gRPC calls

pub mod error;

pub mod http;

#[cfg(feature = "grpc")]
pub mod grpc;

// ============================================================================
// Shared types
// ============================================================================

/// Type of operation for proxy routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationType {
    /// Read operation (safe to route to replicas).
    Read,
    /// Write operation (must route to primary).
    Write,
    /// Health/meta operation (can go anywhere).
    Meta,
}

// ============================================================================
// Re-exports
// ============================================================================

pub use error::WeaviateWireError;
