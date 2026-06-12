//! Weaviate REST API route classification.
//!
//! Parses HTTP method + path into a classified route for proxy routing.
//! Routes sourced from Weaviate handler registrations in
//! `adapters/handlers/rest/` (handlers_objects.go, handlers_schema.go,
//! handlers_batch_objects.go, handlers_backup.go, handlers_nodes.go,
//! handlers_classification.go, handlers_misc.go, handlers_aliases.go).

use crate::OperationType;

/// Classified Weaviate REST API route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WeaviateRoute {
    // ========================================================================
    // Object CRUD
    // ========================================================================
    /// `GET /v1/objects`
    ListObjects,
    /// `GET /v1/objects/{className}/{id}` or `GET /v1/objects/{id}`
    GetObject { class_name: Option<String>, id: String },
    /// `POST /v1/objects`
    CreateObject,
    /// `POST /v1/objects/validate`
    ValidateObject,
    /// `PUT /v1/objects/{className}/{id}`
    UpdateObject { class_name: String, id: String },
    /// `PATCH /v1/objects/{className}/{id}`
    PatchObject { class_name: String, id: String },
    /// `DELETE /v1/objects/{className}/{id}`
    DeleteObject { class_name: String, id: String },
    /// `HEAD /v1/objects/{className}/{id}`
    ObjectHead { class_name: String, id: String },
    /// `POST /v1/objects/{className}/{id}/references/{property}`
    AddReference { class_name: String, id: String, property: String },
    /// `PUT /v1/objects/{className}/{id}/references/{property}`
    UpdateReferences { class_name: String, id: String, property: String },
    /// `DELETE /v1/objects/{className}/{id}/references/{property}`
    DeleteReference { class_name: String, id: String, property: String },

    // ========================================================================
    // Batch operations
    // ========================================================================
    /// `POST /v1/batch/objects`
    BatchObjects,
    /// `POST /v1/batch/references`
    BatchReferences,
    /// `DELETE /v1/batch/objects`
    BatchDelete,

    // ========================================================================
    // Schema
    // ========================================================================
    /// `GET /v1/schema`
    GetSchema,
    /// `GET /v1/schema/{className}`
    GetClassSchema { class_name: String },
    /// `POST /v1/schema`
    CreateClass,
    /// `PUT /v1/schema/{className}`
    UpdateClass { class_name: String },
    /// `DELETE /v1/schema/{className}`
    DeleteClass { class_name: String },
    /// `POST /v1/schema/{className}/properties`
    AddProperty { class_name: String },
    /// `POST /v1/schema/{className}/tenants`
    AddTenants { class_name: String },
    /// `GET /v1/schema/{className}/tenants`
    GetTenants { class_name: String },
    /// `GET /v1/schema/{className}/tenants/{tenantName}`
    GetTenant { class_name: String, tenant_name: String },
    /// `HEAD /v1/schema/{className}/tenants/{tenantName}`
    TenantExists { class_name: String, tenant_name: String },
    /// `PUT /v1/schema/{className}/tenants`
    UpdateTenants { class_name: String },
    /// `DELETE /v1/schema/{className}/tenants`
    DeleteTenants { class_name: String },
    /// `POST /v1/schema/{className}/shards`
    AddShards { class_name: String },
    /// `GET /v1/schema/{className}/shards`
    GetShards { class_name: String },
    /// `PUT /v1/schema/{className}/shards/{shardName}`
    UpdateShard { class_name: String, shard_name: String },

    // ========================================================================
    // Schema aliases
    // ========================================================================
    /// `GET /v1/schema/aliases`
    ListAliases,
    /// `GET /v1/schema/aliases/{aliasName}`
    GetAlias { alias_name: String },
    /// `POST /v1/schema/aliases`
    CreateAlias,
    /// `PUT /v1/schema/aliases/{aliasName}`
    UpdateAlias { alias_name: String },
    /// `DELETE /v1/schema/aliases/{aliasName}`
    DeleteAlias { alias_name: String },

    // ========================================================================
    // GraphQL
    // ========================================================================
    /// `POST /v1/graphql`
    GraphQL,

    // ========================================================================
    // Classification
    // ========================================================================
    /// `POST /v1/classifications`
    StartClassification,
    /// `GET /v1/classifications/{id}`
    GetClassification { id: String },

    // ========================================================================
    // Backups
    // ========================================================================
    /// `POST /v1/backups/{backend}`
    CreateBackup { backend: String },
    /// `GET /v1/backups/{backend}`
    ListBackups { backend: String },
    /// `GET /v1/backups/{backend}/{id}`
    GetBackupStatus { backend: String, id: String },
    /// `DELETE /v1/backups/{backend}/{id}`
    CancelBackup { backend: String, id: String },
    /// `POST /v1/backups/{backend}/{id}/restore`
    RestoreBackup { backend: String, id: String },
    /// `GET /v1/backups/{backend}/{id}/restore`
    GetRestoreStatus { backend: String, id: String },
    /// `DELETE /v1/backups/{backend}/{id}/restore`
    CancelRestore { backend: String, id: String },

    // ========================================================================
    // Cluster / Nodes
    // ========================================================================
    /// `GET /v1/nodes`
    GetNodes,
    /// `GET /v1/nodes/{className}`
    GetNodesByClass { class_name: String },
    /// `GET /v1/cluster/statistics`
    GetClusterStatistics,

    // ========================================================================
    // Meta / Health
    // ========================================================================
    /// `GET /v1/meta`
    GetMeta,
    /// `GET /v1/.well-known/live`
    LiveCheck,
    /// `GET /v1/.well-known/ready`
    ReadyCheck,
    /// `GET /v1/.well-known/openid-configuration`
    OpenIDConfig,

    // ========================================================================
    // Fallback
    // ========================================================================
    /// Unrecognized route.
    Unknown { method: String, path: String },
}

impl WeaviateRoute {
    /// Classify the operation type for proxy routing.
    pub fn operation_type(&self) -> OperationType {
        match self {
            // Reads
            Self::ListObjects
            | Self::GetObject { .. }
            | Self::ObjectHead { .. }
            | Self::ValidateObject
            | Self::GetSchema
            | Self::GetClassSchema { .. }
            | Self::GetTenants { .. }
            | Self::GetTenant { .. }
            | Self::TenantExists { .. }
            | Self::GetShards { .. }
            | Self::ListAliases
            | Self::GetAlias { .. }
            | Self::GetClassification { .. }
            | Self::ListBackups { .. }
            | Self::GetBackupStatus { .. }
            | Self::GetRestoreStatus { .. }
            | Self::GetNodes
            | Self::GetNodesByClass { .. }
            | Self::GetClusterStatistics => OperationType::Read,

            // GraphQL is read-only in Weaviate (Get, Aggregate, Explore).
            Self::GraphQL => OperationType::Read,

            // Meta/health
            Self::GetMeta | Self::LiveCheck | Self::ReadyCheck | Self::OpenIDConfig => OperationType::Meta,

            // Everything else is a write.
            _ => OperationType::Write,
        }
    }

    /// Extract the class name if present in the route.
    pub fn class_name(&self) -> Option<&str> {
        match self {
            Self::GetObject { class_name, .. } => class_name.as_deref(),
            Self::UpdateObject { class_name, .. }
            | Self::PatchObject { class_name, .. }
            | Self::DeleteObject { class_name, .. }
            | Self::ObjectHead { class_name, .. }
            | Self::AddReference { class_name, .. }
            | Self::UpdateReferences { class_name, .. }
            | Self::DeleteReference { class_name, .. }
            | Self::GetClassSchema { class_name, .. }
            | Self::UpdateClass { class_name, .. }
            | Self::DeleteClass { class_name, .. }
            | Self::AddProperty { class_name, .. }
            | Self::AddTenants { class_name, .. }
            | Self::GetTenants { class_name, .. }
            | Self::GetTenant { class_name, .. }
            | Self::TenantExists { class_name, .. }
            | Self::UpdateTenants { class_name, .. }
            | Self::DeleteTenants { class_name, .. }
            | Self::AddShards { class_name, .. }
            | Self::GetShards { class_name, .. }
            | Self::UpdateShard { class_name, .. }
            | Self::GetNodesByClass { class_name, .. } => Some(class_name),
            _ => None,
        }
    }

    /// Extract the object ID if present in the route.
    pub fn object_id(&self) -> Option<&str> {
        match self {
            Self::GetObject { id, .. }
            | Self::UpdateObject { id, .. }
            | Self::PatchObject { id, .. }
            | Self::DeleteObject { id, .. }
            | Self::ObjectHead { id, .. }
            | Self::AddReference { id, .. }
            | Self::UpdateReferences { id, .. }
            | Self::DeleteReference { id, .. }
            | Self::GetClassification { id, .. }
            | Self::GetBackupStatus { id, .. }
            | Self::CancelBackup { id, .. }
            | Self::RestoreBackup { id, .. }
            | Self::GetRestoreStatus { id, .. }
            | Self::CancelRestore { id, .. } => Some(id),
            _ => None,
        }
    }
}

/// Parse an HTTP method + path into a classified route.
///
/// # Arguments
/// * `method` - HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD)
/// * `path` - URL path (e.g., "/v1/objects/MyClass/abc-123")
pub fn parse_route(method: &str, path: &str) -> WeaviateRoute {
    // Strip query string if present.
    let path_clean = path.split('?').next().unwrap_or(path);
    // Strip trailing slash.
    let path_clean = path_clean.trim_end_matches('/');

    // Split path into segments.
    let segments: Vec<&str> = path_clean.split('/').filter(|s| !s.is_empty()).collect();

    match (method, segments.as_slice()) {
        // ====================================================================
        // Meta / Health
        // ====================================================================
        ("GET", ["v1", "meta"]) => WeaviateRoute::GetMeta,
        ("GET", ["v1", ".well-known", "ready"]) => WeaviateRoute::ReadyCheck,
        ("GET", ["v1", ".well-known", "live"]) => WeaviateRoute::LiveCheck,
        ("GET", ["v1", ".well-known", "openid-configuration"]) => WeaviateRoute::OpenIDConfig,

        // ====================================================================
        // Cluster / Nodes
        // ====================================================================
        ("GET", ["v1", "nodes"]) => WeaviateRoute::GetNodes,
        ("GET", ["v1", "nodes", class_name]) => WeaviateRoute::GetNodesByClass { class_name: class_name.to_string() },
        ("GET", ["v1", "cluster", "statistics"]) => WeaviateRoute::GetClusterStatistics,

        // ====================================================================
        // GraphQL
        // ====================================================================
        ("POST", ["v1", "graphql"]) => WeaviateRoute::GraphQL,

        // ====================================================================
        // Classifications
        // ====================================================================
        ("POST", ["v1", "classifications"]) => WeaviateRoute::StartClassification,
        ("GET", ["v1", "classifications", id]) => WeaviateRoute::GetClassification { id: id.to_string() },

        // ====================================================================
        // Backups (longer paths first)
        // ====================================================================
        ("POST", ["v1", "backups", backend, id, "restore"]) => {
            WeaviateRoute::RestoreBackup { backend: backend.to_string(), id: id.to_string() }
        }
        ("GET", ["v1", "backups", backend, id, "restore"]) => {
            WeaviateRoute::GetRestoreStatus { backend: backend.to_string(), id: id.to_string() }
        }
        ("DELETE", ["v1", "backups", backend, id, "restore"]) => {
            WeaviateRoute::CancelRestore { backend: backend.to_string(), id: id.to_string() }
        }
        ("POST", ["v1", "backups", backend]) => WeaviateRoute::CreateBackup { backend: backend.to_string() },
        ("GET", ["v1", "backups", backend]) => WeaviateRoute::ListBackups { backend: backend.to_string() },
        ("GET", ["v1", "backups", backend, id]) => WeaviateRoute::GetBackupStatus { backend: backend.to_string(), id: id.to_string() },
        ("DELETE", ["v1", "backups", backend, id]) => WeaviateRoute::CancelBackup { backend: backend.to_string(), id: id.to_string() },

        // ====================================================================
        // Schema — aliases (must match before generic class-level)
        // ====================================================================
        ("GET", ["v1", "schema", "aliases"]) => WeaviateRoute::ListAliases,
        ("POST", ["v1", "schema", "aliases"]) => WeaviateRoute::CreateAlias,
        ("GET", ["v1", "schema", "aliases", alias_name]) => WeaviateRoute::GetAlias { alias_name: alias_name.to_string() },
        ("PUT", ["v1", "schema", "aliases", alias_name]) => WeaviateRoute::UpdateAlias { alias_name: alias_name.to_string() },
        ("DELETE", ["v1", "schema", "aliases", alias_name]) => WeaviateRoute::DeleteAlias { alias_name: alias_name.to_string() },

        // ====================================================================
        // Schema — tenants (individual tenant before collection)
        // ====================================================================
        ("HEAD", ["v1", "schema", class_name, "tenants", tenant_name]) => WeaviateRoute::TenantExists {
            class_name: class_name.to_string(),
            tenant_name: tenant_name.to_string(),
        },
        ("GET", ["v1", "schema", class_name, "tenants", tenant_name]) => WeaviateRoute::GetTenant {
            class_name: class_name.to_string(),
            tenant_name: tenant_name.to_string(),
        },
        ("POST", ["v1", "schema", class_name, "tenants"]) => WeaviateRoute::AddTenants { class_name: class_name.to_string() },
        ("GET", ["v1", "schema", class_name, "tenants"]) => WeaviateRoute::GetTenants { class_name: class_name.to_string() },
        ("PUT", ["v1", "schema", class_name, "tenants"]) => WeaviateRoute::UpdateTenants { class_name: class_name.to_string() },
        ("DELETE", ["v1", "schema", class_name, "tenants"]) => WeaviateRoute::DeleteTenants { class_name: class_name.to_string() },

        // ====================================================================
        // Schema — properties, shards
        // ====================================================================
        ("POST", ["v1", "schema", class_name, "properties"]) => WeaviateRoute::AddProperty { class_name: class_name.to_string() },
        ("POST", ["v1", "schema", class_name, "shards"]) => WeaviateRoute::AddShards { class_name: class_name.to_string() },
        ("GET", ["v1", "schema", class_name, "shards"]) => WeaviateRoute::GetShards { class_name: class_name.to_string() },
        ("PUT", ["v1", "schema", class_name, "shards", shard_name]) => WeaviateRoute::UpdateShard {
            class_name: class_name.to_string(),
            shard_name: shard_name.to_string(),
        },

        // ====================================================================
        // Schema — class-level
        // ====================================================================
        ("GET", ["v1", "schema"]) => WeaviateRoute::GetSchema,
        ("POST", ["v1", "schema"]) => WeaviateRoute::CreateClass,
        ("GET", ["v1", "schema", class_name]) => WeaviateRoute::GetClassSchema { class_name: class_name.to_string() },
        ("PUT", ["v1", "schema", class_name]) => WeaviateRoute::UpdateClass { class_name: class_name.to_string() },
        ("DELETE", ["v1", "schema", class_name]) => WeaviateRoute::DeleteClass { class_name: class_name.to_string() },

        // ====================================================================
        // Batch
        // ====================================================================
        ("POST", ["v1", "batch", "objects"]) => WeaviateRoute::BatchObjects,
        ("DELETE", ["v1", "batch", "objects"]) => WeaviateRoute::BatchDelete,
        ("POST", ["v1", "batch", "references"]) => WeaviateRoute::BatchReferences,

        // ====================================================================
        // Objects — validate (must match before object CRUD)
        // ====================================================================
        ("POST", ["v1", "objects", "validate"]) => WeaviateRoute::ValidateObject,

        // ====================================================================
        // Objects — references (must match before object-level)
        // ====================================================================
        ("POST", ["v1", "objects", class_name, id, "references", property]) => WeaviateRoute::AddReference {
            class_name: class_name.to_string(),
            id: id.to_string(),
            property: property.to_string(),
        },
        ("PUT", ["v1", "objects", class_name, id, "references", property]) => WeaviateRoute::UpdateReferences {
            class_name: class_name.to_string(),
            id: id.to_string(),
            property: property.to_string(),
        },
        ("DELETE", ["v1", "objects", class_name, id, "references", property]) => WeaviateRoute::DeleteReference {
            class_name: class_name.to_string(),
            id: id.to_string(),
            property: property.to_string(),
        },

        // ====================================================================
        // Objects — CRUD
        // ====================================================================
        ("GET", ["v1", "objects"]) => WeaviateRoute::ListObjects,
        ("POST", ["v1", "objects"]) => WeaviateRoute::CreateObject,
        ("GET", ["v1", "objects", class_name, id]) => {
            WeaviateRoute::GetObject { class_name: Some(class_name.to_string()), id: id.to_string() }
        }
        ("PUT", ["v1", "objects", class_name, id]) => {
            WeaviateRoute::UpdateObject { class_name: class_name.to_string(), id: id.to_string() }
        }
        ("PATCH", ["v1", "objects", class_name, id]) => {
            WeaviateRoute::PatchObject { class_name: class_name.to_string(), id: id.to_string() }
        }
        ("DELETE", ["v1", "objects", class_name, id]) => {
            WeaviateRoute::DeleteObject { class_name: class_name.to_string(), id: id.to_string() }
        }
        ("HEAD", ["v1", "objects", class_name, id]) => WeaviateRoute::ObjectHead { class_name: class_name.to_string(), id: id.to_string() },
        // Legacy: GET /v1/objects/{id} without class name.
        ("GET", ["v1", "objects", id]) => WeaviateRoute::GetObject { class_name: None, id: id.to_string() },

        // ====================================================================
        // Fallback
        // ====================================================================
        _ => WeaviateRoute::Unknown { method: method.to_string(), path: path.to_string() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperationType;

    // ========================================================================
    // Route parsing
    // ========================================================================

    #[test]
    fn test_meta_routes() {
        assert_eq!(parse_route("GET", "/v1/meta"), WeaviateRoute::GetMeta);
        assert_eq!(parse_route("GET", "/v1/.well-known/ready"), WeaviateRoute::ReadyCheck);
        assert_eq!(parse_route("GET", "/v1/.well-known/live"), WeaviateRoute::LiveCheck);
        assert_eq!(parse_route("GET", "/v1/.well-known/openid-configuration"), WeaviateRoute::OpenIDConfig);
    }

    #[test]
    fn test_object_routes() {
        assert_eq!(parse_route("GET", "/v1/objects"), WeaviateRoute::ListObjects);
        assert_eq!(parse_route("POST", "/v1/objects"), WeaviateRoute::CreateObject);
        assert_eq!(
            parse_route("GET", "/v1/objects/Article/abc-123"),
            WeaviateRoute::GetObject {
                class_name: Some("Article".to_string()),
                id: "abc-123".to_string(),
            }
        );
        assert_eq!(
            parse_route("PUT", "/v1/objects/Article/abc-123"),
            WeaviateRoute::UpdateObject { class_name: "Article".to_string(), id: "abc-123".to_string() }
        );
        assert_eq!(
            parse_route("PATCH", "/v1/objects/Article/abc-123"),
            WeaviateRoute::PatchObject { class_name: "Article".to_string(), id: "abc-123".to_string() }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/objects/Article/abc-123"),
            WeaviateRoute::DeleteObject { class_name: "Article".to_string(), id: "abc-123".to_string() }
        );
        assert_eq!(
            parse_route("HEAD", "/v1/objects/Article/abc-123"),
            WeaviateRoute::ObjectHead { class_name: "Article".to_string(), id: "abc-123".to_string() }
        );
    }

    #[test]
    fn test_object_validate() {
        assert_eq!(parse_route("POST", "/v1/objects/validate"), WeaviateRoute::ValidateObject);
    }

    #[test]
    fn test_object_legacy_get() {
        assert_eq!(
            parse_route("GET", "/v1/objects/abc-123"),
            WeaviateRoute::GetObject { class_name: None, id: "abc-123".to_string() }
        );
    }

    #[test]
    fn test_reference_routes() {
        assert_eq!(
            parse_route("POST", "/v1/objects/Article/abc-123/references/author"),
            WeaviateRoute::AddReference {
                class_name: "Article".to_string(),
                id: "abc-123".to_string(),
                property: "author".to_string(),
            }
        );
        assert_eq!(
            parse_route("PUT", "/v1/objects/Article/abc-123/references/author"),
            WeaviateRoute::UpdateReferences {
                class_name: "Article".to_string(),
                id: "abc-123".to_string(),
                property: "author".to_string(),
            }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/objects/Article/abc-123/references/author"),
            WeaviateRoute::DeleteReference {
                class_name: "Article".to_string(),
                id: "abc-123".to_string(),
                property: "author".to_string(),
            }
        );
    }

    #[test]
    fn test_batch_routes() {
        assert_eq!(parse_route("POST", "/v1/batch/objects"), WeaviateRoute::BatchObjects);
        assert_eq!(parse_route("POST", "/v1/batch/references"), WeaviateRoute::BatchReferences);
        assert_eq!(parse_route("DELETE", "/v1/batch/objects"), WeaviateRoute::BatchDelete);
    }

    #[test]
    fn test_schema_routes() {
        assert_eq!(parse_route("GET", "/v1/schema"), WeaviateRoute::GetSchema);
        assert_eq!(parse_route("POST", "/v1/schema"), WeaviateRoute::CreateClass);
        assert_eq!(
            parse_route("GET", "/v1/schema/Article"),
            WeaviateRoute::GetClassSchema { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("PUT", "/v1/schema/Article"),
            WeaviateRoute::UpdateClass { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/schema/Article"),
            WeaviateRoute::DeleteClass { class_name: "Article".to_string() }
        );
    }

    #[test]
    fn test_schema_alias_routes() {
        assert_eq!(parse_route("GET", "/v1/schema/aliases"), WeaviateRoute::ListAliases);
        assert_eq!(parse_route("POST", "/v1/schema/aliases"), WeaviateRoute::CreateAlias);
        assert_eq!(
            parse_route("GET", "/v1/schema/aliases/MyAlias"),
            WeaviateRoute::GetAlias { alias_name: "MyAlias".to_string() }
        );
        assert_eq!(
            parse_route("PUT", "/v1/schema/aliases/MyAlias"),
            WeaviateRoute::UpdateAlias { alias_name: "MyAlias".to_string() }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/schema/aliases/MyAlias"),
            WeaviateRoute::DeleteAlias { alias_name: "MyAlias".to_string() }
        );
    }

    #[test]
    fn test_schema_tenant_routes() {
        assert_eq!(
            parse_route("POST", "/v1/schema/Article/tenants"),
            WeaviateRoute::AddTenants { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("GET", "/v1/schema/Article/tenants"),
            WeaviateRoute::GetTenants { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("PUT", "/v1/schema/Article/tenants"),
            WeaviateRoute::UpdateTenants { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/schema/Article/tenants"),
            WeaviateRoute::DeleteTenants { class_name: "Article".to_string() }
        );
    }

    #[test]
    fn test_schema_individual_tenant_routes() {
        assert_eq!(
            parse_route("GET", "/v1/schema/Article/tenants/tenantA"),
            WeaviateRoute::GetTenant {
                class_name: "Article".to_string(),
                tenant_name: "tenantA".to_string(),
            }
        );
        assert_eq!(
            parse_route("HEAD", "/v1/schema/Article/tenants/tenantA"),
            WeaviateRoute::TenantExists {
                class_name: "Article".to_string(),
                tenant_name: "tenantA".to_string(),
            }
        );
    }

    #[test]
    fn test_schema_property_routes() {
        assert_eq!(
            parse_route("POST", "/v1/schema/Article/properties"),
            WeaviateRoute::AddProperty { class_name: "Article".to_string() }
        );
    }

    #[test]
    fn test_schema_shard_routes() {
        assert_eq!(
            parse_route("POST", "/v1/schema/Article/shards"),
            WeaviateRoute::AddShards { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("GET", "/v1/schema/Article/shards"),
            WeaviateRoute::GetShards { class_name: "Article".to_string() }
        );
        assert_eq!(
            parse_route("PUT", "/v1/schema/Article/shards/shard1"),
            WeaviateRoute::UpdateShard {
                class_name: "Article".to_string(),
                shard_name: "shard1".to_string(),
            }
        );
    }

    #[test]
    fn test_graphql_route() {
        assert_eq!(parse_route("POST", "/v1/graphql"), WeaviateRoute::GraphQL);
    }

    #[test]
    fn test_classification_routes() {
        assert_eq!(parse_route("POST", "/v1/classifications"), WeaviateRoute::StartClassification);
        assert_eq!(
            parse_route("GET", "/v1/classifications/cls-123"),
            WeaviateRoute::GetClassification { id: "cls-123".to_string() }
        );
    }

    #[test]
    fn test_backup_routes() {
        assert_eq!(parse_route("POST", "/v1/backups/s3"), WeaviateRoute::CreateBackup { backend: "s3".to_string() });
        assert_eq!(parse_route("GET", "/v1/backups/s3"), WeaviateRoute::ListBackups { backend: "s3".to_string() });
        assert_eq!(
            parse_route("GET", "/v1/backups/s3/backup-123"),
            WeaviateRoute::GetBackupStatus { backend: "s3".to_string(), id: "backup-123".to_string() }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/backups/s3/backup-123"),
            WeaviateRoute::CancelBackup { backend: "s3".to_string(), id: "backup-123".to_string() }
        );
        assert_eq!(
            parse_route("POST", "/v1/backups/s3/backup-123/restore"),
            WeaviateRoute::RestoreBackup { backend: "s3".to_string(), id: "backup-123".to_string() }
        );
        assert_eq!(
            parse_route("GET", "/v1/backups/s3/backup-123/restore"),
            WeaviateRoute::GetRestoreStatus { backend: "s3".to_string(), id: "backup-123".to_string() }
        );
        assert_eq!(
            parse_route("DELETE", "/v1/backups/s3/backup-123/restore"),
            WeaviateRoute::CancelRestore { backend: "s3".to_string(), id: "backup-123".to_string() }
        );
    }

    #[test]
    fn test_cluster_routes() {
        assert_eq!(parse_route("GET", "/v1/nodes"), WeaviateRoute::GetNodes);
        assert_eq!(
            parse_route("GET", "/v1/nodes/Article"),
            WeaviateRoute::GetNodesByClass { class_name: "Article".to_string() }
        );
        assert_eq!(parse_route("GET", "/v1/cluster/statistics"), WeaviateRoute::GetClusterStatistics);
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    #[test]
    fn test_trailing_slash() {
        assert_eq!(parse_route("GET", "/v1/objects/"), WeaviateRoute::ListObjects);
        assert_eq!(parse_route("GET", "/v1/schema/"), WeaviateRoute::GetSchema);
    }

    #[test]
    fn test_query_string_stripped() {
        assert_eq!(parse_route("GET", "/v1/objects?limit=10&offset=0"), WeaviateRoute::ListObjects);
        assert_eq!(
            parse_route("GET", "/v1/objects/Article/abc?include=vector"),
            WeaviateRoute::GetObject {
                class_name: Some("Article".to_string()),
                id: "abc".to_string(),
            }
        );
    }

    #[test]
    fn test_unknown_route() {
        let route = parse_route("GET", "/v2/something/new");
        assert!(matches!(route, WeaviateRoute::Unknown { .. }));
    }

    // ========================================================================
    // Operation type classification
    // ========================================================================

    #[test]
    fn test_read_operations() {
        assert_eq!(parse_route("GET", "/v1/objects").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/objects/Cls/id").operation_type(), OperationType::Read);
        assert_eq!(parse_route("HEAD", "/v1/objects/Cls/id").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/schema").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/schema/Cls/tenants").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/schema/Cls/tenants/t1").operation_type(), OperationType::Read);
        assert_eq!(parse_route("HEAD", "/v1/schema/Cls/tenants/t1").operation_type(), OperationType::Read);
        assert_eq!(parse_route("POST", "/v1/graphql").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/nodes").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/nodes/Article").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/schema/aliases").operation_type(), OperationType::Read);
        assert_eq!(parse_route("GET", "/v1/backups/s3").operation_type(), OperationType::Read);
        assert_eq!(parse_route("POST", "/v1/objects/validate").operation_type(), OperationType::Read);
    }

    #[test]
    fn test_write_operations() {
        assert_eq!(parse_route("POST", "/v1/objects").operation_type(), OperationType::Write);
        assert_eq!(parse_route("PUT", "/v1/objects/Cls/id").operation_type(), OperationType::Write);
        assert_eq!(parse_route("PATCH", "/v1/objects/Cls/id").operation_type(), OperationType::Write);
        assert_eq!(parse_route("DELETE", "/v1/objects/Cls/id").operation_type(), OperationType::Write);
        assert_eq!(parse_route("POST", "/v1/batch/objects").operation_type(), OperationType::Write);
        assert_eq!(parse_route("POST", "/v1/schema").operation_type(), OperationType::Write);
        assert_eq!(parse_route("POST", "/v1/schema/aliases").operation_type(), OperationType::Write);
        assert_eq!(parse_route("DELETE", "/v1/backups/s3/b1").operation_type(), OperationType::Write);
    }

    #[test]
    fn test_meta_operations() {
        assert_eq!(parse_route("GET", "/v1/meta").operation_type(), OperationType::Meta);
        assert_eq!(parse_route("GET", "/v1/.well-known/live").operation_type(), OperationType::Meta);
        assert_eq!(parse_route("GET", "/v1/.well-known/ready").operation_type(), OperationType::Meta);
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    #[test]
    fn test_class_name_extraction() {
        let route = parse_route("GET", "/v1/objects/Article/abc-123");
        assert_eq!(route.class_name(), Some("Article"));

        let route = parse_route("GET", "/v1/schema/Article/tenants");
        assert_eq!(route.class_name(), Some("Article"));

        let route = parse_route("GET", "/v1/nodes/Article");
        assert_eq!(route.class_name(), Some("Article"));

        let route = parse_route("GET", "/v1/objects");
        assert_eq!(route.class_name(), None);
    }

    #[test]
    fn test_object_id_extraction() {
        let route = parse_route("GET", "/v1/objects/Article/abc-123");
        assert_eq!(route.object_id(), Some("abc-123"));

        let route = parse_route("GET", "/v1/backups/s3/backup-1");
        assert_eq!(route.object_id(), Some("backup-1"));

        let route = parse_route("DELETE", "/v1/backups/s3/backup-1");
        assert_eq!(route.object_id(), Some("backup-1"));

        let route = parse_route("GET", "/v1/objects");
        assert_eq!(route.object_id(), None);
    }
}
