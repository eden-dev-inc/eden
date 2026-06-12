//! Weaviate HTTP REST API support (default port 8080).
//!
//! This module provides parsing for Weaviate-specific HTTP headers,
//! URL route classification, and query parameters.

pub mod headers;
pub mod query_params;
pub mod route;

pub use headers::{WeaviateRequestHeaders, WeaviateResponseHeaders};
pub use query_params::QueryParams;
pub use route::{WeaviateRoute, parse_route};
