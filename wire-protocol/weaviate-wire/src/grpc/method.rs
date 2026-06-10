//! Weaviate gRPC method classification.
//!
//! Classifies gRPC method paths for proxy routing.

use crate::OperationType;

/// Known Weaviate gRPC service paths.
///
/// From `weaviate.v1.Weaviate` service in `weaviate.proto`.
pub mod paths {
    /// Search/get operation.
    pub const SEARCH: &str = "/weaviate.v1.Weaviate/Search";
    /// Batch objects import.
    pub const BATCH_OBJECTS: &str = "/weaviate.v1.Weaviate/BatchObjects";
    /// Batch references import.
    pub const BATCH_REFERENCES: &str = "/weaviate.v1.Weaviate/BatchReferences";
    /// Batch delete.
    pub const BATCH_DELETE: &str = "/weaviate.v1.Weaviate/BatchDelete";
    /// Bidirectional streaming batch import.
    pub const BATCH_STREAM: &str = "/weaviate.v1.Weaviate/BatchStream";
    /// Get tenants.
    pub const TENANTS_GET: &str = "/weaviate.v1.Weaviate/TenantsGet";
    /// Aggregate operation.
    pub const AGGREGATE: &str = "/weaviate.v1.Weaviate/Aggregate";
}

/// Known Weaviate gRPC methods.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WeaviateGrpcMethod {
    /// Search/get operation (read).
    Search,
    /// Aggregate operation (read).
    Aggregate,
    /// Batch object import (write).
    BatchObjects,
    /// Batch references import (write).
    BatchReferences,
    /// Batch delete (write).
    BatchDelete,
    /// Bidirectional streaming batch import (write).
    BatchStream,
    /// Get tenants (read).
    TenantsGet,
    /// Unknown method.
    Unknown(String),
}

impl WeaviateGrpcMethod {
    /// Classify the operation type for proxy routing.
    pub fn operation_type(&self) -> OperationType {
        match self {
            Self::Search | Self::Aggregate | Self::TenantsGet => OperationType::Read,
            Self::BatchObjects | Self::BatchReferences | Self::BatchDelete | Self::BatchStream => OperationType::Write,
            // Conservative default: unknown methods are treated as writes.
            Self::Unknown(_) => OperationType::Write,
        }
    }
}

/// Parse a gRPC method path into a classified method.
///
/// gRPC paths look like: `/weaviate.v1.Weaviate/Search`
pub fn classify_grpc_method(path: &str) -> WeaviateGrpcMethod {
    match path {
        paths::SEARCH => WeaviateGrpcMethod::Search,
        paths::AGGREGATE => WeaviateGrpcMethod::Aggregate,
        paths::BATCH_OBJECTS => WeaviateGrpcMethod::BatchObjects,
        paths::BATCH_REFERENCES => WeaviateGrpcMethod::BatchReferences,
        paths::BATCH_DELETE => WeaviateGrpcMethod::BatchDelete,
        paths::BATCH_STREAM => WeaviateGrpcMethod::BatchStream,
        paths::TENANTS_GET => WeaviateGrpcMethod::TenantsGet,
        _ => WeaviateGrpcMethod::Unknown(path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperationType;

    #[test]
    fn test_classify_search() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/Search");
        assert_eq!(method, WeaviateGrpcMethod::Search);
        assert_eq!(method.operation_type(), OperationType::Read);
    }

    #[test]
    fn test_classify_batch_objects() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/BatchObjects");
        assert_eq!(method, WeaviateGrpcMethod::BatchObjects);
        assert_eq!(method.operation_type(), OperationType::Write);
    }

    #[test]
    fn test_classify_batch_references() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/BatchReferences");
        assert_eq!(method, WeaviateGrpcMethod::BatchReferences);
        assert_eq!(method.operation_type(), OperationType::Write);
    }

    #[test]
    fn test_classify_batch_delete() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/BatchDelete");
        assert_eq!(method, WeaviateGrpcMethod::BatchDelete);
        assert_eq!(method.operation_type(), OperationType::Write);
    }

    #[test]
    fn test_classify_batch_stream() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/BatchStream");
        assert_eq!(method, WeaviateGrpcMethod::BatchStream);
        assert_eq!(method.operation_type(), OperationType::Write);
    }

    #[test]
    fn test_classify_tenants_get() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/TenantsGet");
        assert_eq!(method, WeaviateGrpcMethod::TenantsGet);
        assert_eq!(method.operation_type(), OperationType::Read);
    }

    #[test]
    fn test_classify_aggregate() {
        let method = classify_grpc_method("/weaviate.v1.Weaviate/Aggregate");
        assert_eq!(method, WeaviateGrpcMethod::Aggregate);
        assert_eq!(method.operation_type(), OperationType::Read);
    }

    #[test]
    fn test_classify_unknown() {
        let method = classify_grpc_method("/some.other.Service/Method");
        assert_eq!(method, WeaviateGrpcMethod::Unknown("/some.other.Service/Method".to_string()));
        assert_eq!(method.operation_type(), OperationType::Write);
    }
}
