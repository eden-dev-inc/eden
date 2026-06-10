use std::path::PathBuf;

use chrono::Utc;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::OrganizationUuid;
use eden_logger_internal::{LogAudience, log_warn, trace_context};
use uuid::Uuid;

use crate::backups::helpers::{checksum, encrypt, write_file};
use crate::backups::{EncryptedBackupData, EncryptionSettings};
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::lib::ShardCache;
use crate::sql_files;

use super::artifact::{ARTIFACT_VERSION, OrgTransferArtifact, OrgTransferConfig, OrgTransferMetadata};

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Export all data belonging to an organization into an encrypted portable artifact.
    ///
    /// Extracts Postgres rows via a `REPEATABLE READ` transaction and internal cache entries,
    /// encrypts the combined artifact with AES-256-GCM, and writes the result to disk.
    ///
    /// Cache extraction failures are non-fatal: the export continues with empty cache data
    /// since the cache layer rebuilds from Postgres on miss.
    pub async fn export_organization(
        &self,
        organization_uuid: &OrganizationUuid,
        encrypt_password: &str,
        config: OrgTransferConfig,
    ) -> ResultEP<OrgTransferMetadata> {
        let _ctx = trace_context().with_feature("org_transfer.export");
        tokio::fs::create_dir_all(&config.output_dir).await?;

        let created_at = Utc::now().timestamp();
        let org_uuid_val: Uuid = **organization_uuid;

        // -- 1. Extract Postgres data in a REPEATABLE READ transaction --
        let mut conn = self.pg_connection().await?;
        let transaction =
            conn.build_transaction().isolation_level(tokio_postgres::IsolationLevel::RepeatableRead).start().await.map_err(|e| {
                EpError::Database(eden_core::error::DatabaseError::Custom(format!("Failed to begin REPEATABLE READ transaction: {e}")))
            })?;

        let export_sql = sql_files!("select", "organization", "export");
        let row = transaction
            .query_one(export_sql, &[organization_uuid])
            .await
            .map_err(|e| EpError::Database(eden_core::error::DatabaseError::Custom(format!("Failed to export organization data: {e}"))))?;

        let export_data: serde_json::Value = row.get("export_data");

        transaction
            .commit()
            .await
            .map_err(|e| EpError::Database(eden_core::error::DatabaseError::Custom(format!("Failed to commit export transaction: {e}"))))?;

        let organization = json_array_or_empty(&export_data, "organization");
        if organization.is_empty() {
            return Err(EpError::database_organization_not_found());
        }

        let users = json_array_or_empty(&export_data, "users");
        let admins: Vec<uuid::Uuid> = export_data.get("admins").and_then(|v| serde_json::from_value(v.clone()).ok()).unwrap_or_default();
        let endpoints = json_array_or_empty(&export_data, "endpoints");
        let auths = json_array_or_empty(&export_data, "auths");
        let templates = json_array_or_empty(&export_data, "templates");
        let workflows = json_array_or_empty(&export_data, "workflows");
        let workflow_templates = json_array_or_empty(&export_data, "workflow_templates");
        let eden_node_endpoints = json_array_or_empty(&export_data, "eden_node_endpoints");
        let organization_eden_nodes = json_array_or_empty(&export_data, "organization_eden_nodes");
        let organization_apis = json_array_or_empty(&export_data, "organization_apis");
        let organization_interlays = json_array_or_empty(&export_data, "organization_interlays");
        let organization_migrations = json_array_or_empty(&export_data, "organization_migrations");
        let interlays = json_array_or_empty(&export_data, "interlays");
        let robots = json_array_or_empty(&export_data, "robots");

        // -- 2. Extract ShardMap cache keys matching the org prefix --
        let org_prefix = format!("org:{org_uuid_val}:");
        let redis_cache = match snapshot_internal_cache(self, &org_prefix).await {
            Ok(data) => data,
            Err(e) => {
                log_warn!(
                    _ctx,
                    "Failed to export internal cache keys, continuing with empty cache data",
                    audience = LogAudience::Internal,
                    org_uuid = org_uuid_val.to_string(),
                    error = e.to_string()
                );
                Default::default()
            }
        };

        // -- 3. Preserve the compatibility RBAC cache slot with the same unified snapshot.
        let redis_rbac = match snapshot_internal_cache(self, &org_prefix).await {
            Ok(data) => data,
            Err(e) => {
                log_warn!(
                    _ctx,
                    "Failed to export internal RBAC cache keys, continuing with empty RBAC data",
                    audience = LogAudience::Internal,
                    org_uuid = org_uuid_val.to_string(),
                    error = e.to_string()
                );
                Default::default()
            }
        };

        // -- 4. Build artifact --
        let artifact = OrgTransferArtifact {
            version: ARTIFACT_VERSION,
            created_at,
            source_node: config.source_node.clone(),
            description: config.description.clone(),
            organization,
            users,
            admins,
            endpoints,
            auths,
            templates,
            workflows,
            workflow_templates,
            eden_node_endpoints,
            organization_eden_nodes,
            organization_apis,
            organization_interlays,
            organization_migrations,
            interlays,
            robots,
            redis_cache,
            redis_rbac,
        };

        // -- 5. Serialize, encrypt, write --
        let plaintext = serde_json::to_vec(&artifact).map_err(|e| EpError::serde(format!("Failed to serialize artifact: {e}")))?;

        let checksum_val = checksum(&plaintext);
        let (ciphertext, salt, nonce_bytes) = encrypt(&plaintext, encrypt_password)?;

        let artifact_filename = OrgTransferMetadata::artifact_filename(created_at, &org_uuid_val);
        let artifact_path = config.output_dir.join(&artifact_filename);
        write_file(&artifact_path, &ciphertext).await?;

        let encrypted_data = EncryptedBackupData::new(
            PathBuf::from(&artifact_filename),
            checksum_val,
            EncryptionSettings::new(&salt, &nonce_bytes),
            config.persistent,
        );

        // -- 6. Write metadata JSON --
        let metadata = OrgTransferMetadata {
            created_at,
            organization_uuid: org_uuid_val,
            description: config.description,
            source_node: config.source_node,
            artifact: encrypted_data,
        };

        let metadata_filename = OrgTransferMetadata::metadata_filename(created_at, &org_uuid_val);
        let metadata_path = config.output_dir.join(metadata_filename);
        let metadata_json =
            serde_json::to_vec_pretty(&metadata).map_err(|e| EpError::serde(format!("Failed to serialize metadata: {e}")))?;
        write_file(&metadata_path, &metadata_json).await?;

        Ok(metadata)
    }
}

async fn snapshot_internal_cache<R, P, C>(
    db: &DatabaseManager<R, P, C>,
    prefix: &str,
) -> ResultEP<crate::db::internal_cache::InternalCacheSnapshot>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    db.internal_cache().snapshot_with_key_prefix(prefix).await
}

fn json_array_or_empty(data: &serde_json::Value, key: &str) -> Vec<serde_json::Value> {
    data.get(key).and_then(|v| v.as_array().cloned()).unwrap_or_default()
}
