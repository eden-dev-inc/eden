use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::backups::EncryptedBackupData;
use crate::db::internal_cache::InternalCacheSnapshot;

/// Current artifact format version.
pub const ARTIFACT_VERSION: u32 = 1;

/// Portable artifact containing all data for an organization transfer.
///
/// Serialized to JSON, then encrypted with AES-256-GCM before writing to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgTransferArtifact {
    /// Artifact format version for forward compatibility.
    pub version: u32,
    /// Unix timestamp when the artifact was created.
    pub created_at: i64,
    /// Optional identifier of the source Eden node.
    pub source_node: Option<String>,
    /// Optional human-readable description.
    pub description: Option<String>,

    // -- Postgres data (full rows as JSON) --
    /// The organization row (should contain exactly one element).
    pub organization: Vec<serde_json::Value>,
    /// All users belonging to this organization.
    pub users: Vec<serde_json::Value>,
    /// UUIDs of users who are admins of this organization.
    pub admins: Vec<Uuid>,
    /// All endpoints belonging to this organization.
    pub endpoints: Vec<serde_json::Value>,
    /// All auths linked to the organization's endpoints.
    pub auths: Vec<serde_json::Value>,
    /// All templates belonging to this organization.
    pub templates: Vec<serde_json::Value>,
    /// All workflows belonging to this organization.
    pub workflows: Vec<serde_json::Value>,
    /// Workflow-template junction rows.
    pub workflow_templates: Vec<serde_json::Value>,
    /// Eden-node-endpoint junction rows.
    pub eden_node_endpoints: Vec<serde_json::Value>,
    /// Organization-eden-node junction rows.
    pub organization_eden_nodes: Vec<serde_json::Value>,
    /// Organization-API junction rows.
    pub organization_apis: Vec<serde_json::Value>,
    /// Organization-interlay junction rows.
    pub organization_interlays: Vec<serde_json::Value>,
    /// Organization-migration junction rows.
    pub organization_migrations: Vec<serde_json::Value>,
    /// Full interlay rows belonging to this organization
    pub interlays: Vec<serde_json::Value>,
    /// Full robot rows belonging to this organization
    pub robots: Vec<serde_json::Value>,

    // -- Internal cache data --
    /// Compatibility internal KV cache entries scoped to the exported organization.
    ///
    /// Process-local typed cache namespaces are derived from transferred Postgres
    /// rows and are rebuilt on demand after import.
    pub redis_cache: InternalCacheSnapshot,
    /// Compatibility field for the former RBAC Redis DB.
    pub redis_rbac: InternalCacheSnapshot,
}

/// Configuration for an organization transfer export.
#[derive(Debug, Clone)]
pub struct OrgTransferConfig {
    /// Directory to write the artifact file into.
    pub output_dir: PathBuf,
    /// Optional human-readable description stored in artifact metadata.
    pub description: Option<String>,
    /// Optional source node identifier stored in artifact metadata.
    pub source_node: Option<String>,
    /// When true, artifact files persist on disk (not cleaned up on drop).
    pub persistent: bool,
}

impl Default for OrgTransferConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("transfers"),
            description: None,
            source_node: None,
            persistent: true,
        }
    }
}

impl OrgTransferConfig {
    pub fn new(output_dir: impl Into<PathBuf>) -> Self {
        Self { output_dir: output_dir.into(), ..Default::default() }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn with_source_node(mut self, node: impl Into<String>) -> Self {
        self.source_node = Some(node.into());
        self
    }

    pub fn ephemeral(mut self) -> Self {
        self.persistent = false;
        self
    }
}

/// Metadata written alongside the encrypted artifact file.
///
/// Reuses `EncryptedBackupData` for the encrypted dump and `EncryptionSettings`
/// for salt/nonce storage, following the same pattern as the backups module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgTransferMetadata {
    /// Unix timestamp when the export was created.
    pub created_at: i64,
    /// Organization UUID that was exported.
    pub organization_uuid: Uuid,
    /// Human-readable description.
    pub description: Option<String>,
    /// Source node identifier.
    pub source_node: Option<String>,
    /// Encrypted artifact file metadata.
    pub artifact: EncryptedBackupData,
}

impl OrgTransferMetadata {
    pub fn metadata_filename(created_at: i64, org_uuid: &Uuid) -> String {
        format!("org-transfer-{}-{}.metadata.json", org_uuid, created_at)
    }

    pub fn artifact_filename(created_at: i64, org_uuid: &Uuid) -> String {
        format!("org-transfer-{}-{}.dump", org_uuid, created_at)
    }
}

/// Strategy for handling UUID/ID conflicts during import.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportConflictStrategy {
    /// Abort the import if any conflicts are detected.
    Abort,
}

/// Result of a successful import operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ImportResult {
    pub organization_uuid: Uuid,
    pub users_imported: usize,
    pub endpoints_imported: usize,
    pub auths_imported: usize,
    pub templates_imported: usize,
    pub workflows_imported: usize,
    pub interlays_imported: usize,
    pub robots_imported: usize,
    pub redis_cache_keys_restored: usize,
    pub redis_rbac_keys_restored: usize,
}

/// Export mode: copy keeps the source, move deletes it after export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExportMode {
    Copy,
    Move,
}
