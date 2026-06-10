//! Weaviate gRPC API support (default port 50051).
//!
//! This module provides classification and metadata parsing for
//! Weaviate's gRPC service calls.

pub mod metadata;
pub mod method;

pub use metadata::GrpcMetadata;
pub use method::{WeaviateGrpcMethod, classify_grpc_method};
