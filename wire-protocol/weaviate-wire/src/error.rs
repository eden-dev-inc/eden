//! Weaviate Wire Protocol error types.

/// Errors that can occur when parsing Weaviate wire protocol messages.
#[derive(Clone, Debug, thiserror::Error)]
pub enum WeaviateWireError {
    // ========================================================================
    // Header errors
    // ========================================================================
    /// Invalid header value.
    #[error("invalid header value: {header}: {value}")]
    InvalidHeader {
        /// Header name.
        header: String,
        /// Invalid header value.
        value: String,
    },

    // ========================================================================
    // Route/path errors
    // ========================================================================
    /// Unrecognized API route.
    #[error("unrecognized route: {method} {path}")]
    UnrecognizedRoute {
        /// HTTP method.
        method: String,
        /// Request path.
        path: String,
    },

    // ========================================================================
    // Query parameter errors
    // ========================================================================
    /// Missing required parameter.
    #[error("missing required parameter: {0}")]
    MissingParameter(String),

    /// Invalid query parameter value.
    #[error("invalid query parameter: {param}: {value}")]
    InvalidQueryParam {
        /// Parameter name.
        param: String,
        /// Invalid value.
        value: String,
    },

    // ========================================================================
    // gRPC errors
    // ========================================================================
    /// Unrecognized gRPC method.
    #[error("unrecognized gRPC method: {0}")]
    UnrecognizedGrpcMethod(String),

    // ========================================================================
    // Body parsing errors
    // ========================================================================
    /// Invalid request body structure.
    #[error("invalid request body: {0}")]
    InvalidBody(String),
}
