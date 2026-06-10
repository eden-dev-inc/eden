use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::EdenNodeUuid;
use eden_logger_internal::{LogAudience, log_warn, trace_context};
use uuid::Uuid;

use crate::backups::helpers::decrypt_and_verify;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::lib::ShardCache;

use super::artifact::{ARTIFACT_VERSION, ImportConflictStrategy, ImportResult, OrgTransferArtifact, OrgTransferMetadata};

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Import an organization from an encrypted transfer artifact into this deployment.
    ///
    /// Decrypts the artifact, validates it, checks for conflicts, then inserts all Postgres
    /// data in a single transaction (respecting FK order). After Postgres commit, restores
    /// organization-scoped internal cache keys. Cache failures are non-fatal.
    pub async fn import_organization(
        &self,
        artifact_path: &Path,
        encrypt_password: &str,
        target_eden_node_uuid: &EdenNodeUuid,
        conflict_strategy: ImportConflictStrategy,
    ) -> ResultEP<ImportResult> {
        let _ctx = trace_context().with_feature("org_transfer.import");
        // -- 1. Load and decrypt artifact --
        let metadata_bytes = tokio::fs::read(artifact_path).await?;
        let metadata: OrgTransferMetadata =
            serde_json::from_slice(&metadata_bytes).map_err(|e| EpError::serde(format!("Failed to parse metadata: {e}")))?;

        let metadata_dir = artifact_path.parent().unwrap_or(Path::new("."));
        let dump_path = metadata_dir.join(&metadata.artifact.dump_path);

        let dump_path =
            tokio::fs::canonicalize(&dump_path).await.map_err(|e| EpError::init(format!("Artifact dump file not found: {e}")))?;
        let canonical_metadata_dir = tokio::fs::canonicalize(artifact_path.parent().unwrap_or(Path::new(".")))
            .await
            .unwrap_or_else(|_| artifact_path.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from(".")));
        if !dump_path.starts_with(&canonical_metadata_dir) {
            return Err(EpError::auth("Artifact dump file must reside within the artifact directory".to_string()));
        }

        let plaintext = decrypt_and_verify(
            &dump_path,
            &metadata.artifact.checksum,
            &metadata.artifact.settings.salt_b64,
            &metadata.artifact.settings.nonce_prefix_b64,
            encrypt_password,
        )
        .await?;

        let artifact: OrgTransferArtifact =
            serde_json::from_slice(&plaintext).map_err(|e| EpError::serde(format!("Failed to parse artifact: {e}")))?;

        // -- 2. Validate version --
        if artifact.version > ARTIFACT_VERSION {
            return Err(EpError::init(format!(
                "Artifact version {} is newer than supported version {}",
                artifact.version, ARTIFACT_VERSION
            )));
        }

        let org_row = artifact.organization.first().ok_or_else(EpError::database_organization_not_found)?;

        let org_uuid: Uuid = extract_uuid(org_row, "uuid")?;

        // Validate artifact org UUID matches metadata to detect tampering
        if org_uuid != metadata.organization_uuid {
            return Err(EpError::serde(format!(
                "Artifact org UUID {} does not match metadata org UUID {}",
                org_uuid, metadata.organization_uuid
            )));
        }

        let org_id: String = org_row
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::serde("Organization missing 'id' field"))?;

        // -- 3 & 4. Conflict detection + insert in a single SERIALIZABLE transaction --
        let mut conn = self.pg_connection().await?;
        let transaction = conn
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|e| EpError::transaction(format!("Failed to begin import transaction: {e}")))?;

        // Conflict detection (inside the transaction to prevent TOCTOU races)
        let existing_org = transaction
            .query_opt("SELECT uuid FROM organizations WHERE uuid = $1", &[&org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Conflict check failed: {e}")))?;

        if existing_org.is_some() && conflict_strategy == ImportConflictStrategy::Abort {
            return Err(EpError::database_duplicate_organization());
        }

        let existing_org_id = transaction
            .query_opt("SELECT id FROM organizations WHERE id = $1", &[&org_id])
            .await
            .map_err(|e| EpError::database(format!("Conflict check failed: {e}")))?;

        if existing_org_id.is_some() && conflict_strategy == ImportConflictStrategy::Abort {
            return Err(EpError::database_duplicate_organization());
        }

        for user in &artifact.users {
            let user_uuid: Uuid = extract_uuid(user, "uuid")?;
            let username: &str =
                user.get("username").and_then(|v| v.as_str()).ok_or_else(|| EpError::serde("User missing 'username' field"))?;

            let existing = transaction
                .query_opt(
                    "SELECT uuid FROM users WHERE uuid = $1 OR (username = $2 AND organization_uuid = $3)",
                    &[&user_uuid, &username, &org_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Conflict check failed: {e}")))?;

            if existing.is_some() && conflict_strategy == ImportConflictStrategy::Abort {
                return Err(EpError::database_duplicate_user());
            }
        }

        for endpoint in &artifact.endpoints {
            let ep_uuid: Uuid = extract_uuid(endpoint, "uuid")?;
            let ep_id: &str = endpoint.get("id").and_then(|v| v.as_str()).ok_or_else(|| EpError::serde("Endpoint missing 'id' field"))?;

            let existing = transaction
                .query_opt("SELECT uuid FROM endpoints WHERE uuid = $1 OR id = $2", &[&ep_uuid, &ep_id])
                .await
                .map_err(|e| EpError::database(format!("Conflict check failed: {e}")))?;

            if existing.is_some() && conflict_strategy == ImportConflictStrategy::Abort {
                return Err(EpError::database_duplicate_endpoint());
            }
        }

        for interlay in &artifact.interlays {
            let il_uuid: Uuid = extract_uuid(interlay, "uuid")?;
            let il_id: &str = interlay.get("id").and_then(|v| v.as_str()).ok_or_else(|| EpError::serde("Interlay missing 'id' field"))?;

            let existing = transaction
                .query_opt("SELECT uuid FROM interlays WHERE uuid = $1 OR id = $2", &[&il_uuid, &il_id])
                .await
                .map_err(|e| EpError::database(format!("Conflict check failed: {e}")))?;

            if existing.is_some() && conflict_strategy == ImportConflictStrategy::Abort {
                return Err(EpError::database(format!("Interlay '{}' already exists in the target deployment", il_id)));
            }
        }

        for robot in &artifact.robots {
            let robot_uuid: Uuid = extract_uuid(robot, "uuid")?;
            let username: &str =
                robot.get("username").and_then(|v| v.as_str()).ok_or_else(|| EpError::serde("Robot missing 'username' field"))?;

            let existing = transaction
                .query_opt(
                    "SELECT uuid FROM robots WHERE uuid = $1 OR (username = $2 AND organization_uuid = $3)",
                    &[&robot_uuid, &username, &org_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Conflict check failed: {e}")))?;

            if existing.is_some() && conflict_strategy == ImportConflictStrategy::Abort {
                return Err(EpError::database(format!("Robot '{}' already exists in the target deployment", username)));
            }
        }

        // 4a. Insert organization
        let org_description: Option<String> = org_row.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
        let org_created_at: Option<DateTime<Utc>> = parse_timestamp(org_row, "created_at");
        let org_updated_at: Option<DateTime<Utc>> = parse_timestamp(org_row, "updated_at");

        transaction
            .execute(
                "INSERT INTO organizations (id, uuid, description, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)",
                &[&org_id, &org_uuid, &org_description, &org_created_at, &org_updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert organization: {e}")))?;

        // 4b. Insert organization_eden_nodes for target eden node.
        // NOTE: We intentionally ignore the source artifact's eden_node associations and
        // instead link the org and all its endpoints to `target_eden_node_uuid`. This
        // correctly remaps the org to the new deployment's eden node.
        transaction
            .execute(
                "INSERT INTO organization_eden_nodes (organization_uuid, eden_node_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&org_uuid, target_eden_node_uuid],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert organization_eden_nodes: {e}")))?;

        // 4c. Insert users + organization_users
        let mut users_imported = 0;
        for user in &artifact.users {
            let user_uuid: Uuid = extract_uuid(user, "uuid")?;
            let username: String = extract_string(user, "username")?;
            let password: Option<serde_json::Value> = user.get("password").cloned();
            let description: Option<String> = user.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            let email: Option<String> = user.get("email").and_then(|v| v.as_str()).map(|s| s.to_string());
            let display_name: Option<String> = user.get("display_name").and_then(|v| v.as_str()).map(|s| s.to_string());
            let created_by: Uuid = extract_uuid(user, "created_by")?;
            let updated_by: Uuid = extract_uuid(user, "updated_by")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(user, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(user, "updated_at");

            transaction
            .execute(
                "INSERT INTO users (uuid, username, organization_uuid, password, description, email, display_name, created_by, updated_by, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[&user_uuid, &username, &org_uuid, &password, &description, &email, &display_name, &created_by, &updated_by, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert user '{}': {e}", username)))?;

            transaction
                .execute(
                    "INSERT INTO organization_users (organization_uuid, user_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &user_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_users: {e}")))?;

            users_imported += 1;
        }

        // 4d. Insert organization_admins
        for admin_uuid in &artifact.admins {
            transaction
                .execute(
                    "INSERT INTO organization_admins (organization_uuid, user_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, admin_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_admins: {e}")))?;
        }

        // 4e. Insert endpoints + organization_endpoints
        let mut endpoints_imported = 0;
        for endpoint in &artifact.endpoints {
            let ep_uuid: Uuid = extract_uuid(endpoint, "uuid")?;
            let ep_id: String = extract_string(endpoint, "id")?;
            let kind: Option<String> = endpoint.get("kind").and_then(|v| v.as_str()).map(|s| s.to_string());
            let config: Option<Vec<u8>> = endpoint.get("config").and_then(|v| {
                if v.is_null() {
                    return None;
                }
                // config is stored as BYTEA; row_to_json encodes it as a hex-escaped string like "\\x..."
                let decoded = if let Some(s) = v.as_str() {
                    if let Some(hex_str) = s.strip_prefix("\\x") {
                        hex::decode(hex_str).ok()
                    } else {
                        // Try base64 as fallback
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.decode(s).ok()
                    }
                } else {
                    v.as_array().map(|arr| arr.iter().filter_map(|x| x.as_u64().map(|b| b as u8)).collect())
                };
                if decoded.is_none() {
                    log_warn!(
                        _ctx,
                        "Failed to decode endpoint config, unexpected format",
                        audience = LogAudience::Internal,
                        endpoint_id = ep_id.to_string()
                    );
                }
                decoded
            });
            let description: Option<String> = endpoint.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            let created_by: Uuid = extract_uuid(endpoint, "created_by")?;
            let updated_by: Uuid = extract_uuid(endpoint, "updated_by")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(endpoint, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(endpoint, "updated_at");

            transaction
            .execute(
                "INSERT INTO endpoints (id, uuid, kind, config, description, created_by, updated_by, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[&ep_id, &ep_uuid, &kind, &config, &description, &created_by, &updated_by, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert endpoint '{}': {e}", ep_id)))?;

            transaction
                .execute(
                    "INSERT INTO organization_endpoints (organization_uuid, endpoint_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &ep_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_endpoints: {e}")))?;

            // Link endpoint to target eden node
            let now = Utc::now();
            transaction
            .execute(
                "INSERT INTO eden_node_endpoints (eden_node_uuid, endpoint_uuid, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[target_eden_node_uuid, &ep_uuid, &now, &now],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert eden_node_endpoints: {e}")))?;

            endpoints_imported += 1;
        }

        // 4f. Insert auths
        let mut auths_imported = 0;
        for auth in &artifact.auths {
            let auth_uuid: Uuid = extract_uuid(auth, "uuid")?;
            let auth_id: String = extract_string(auth, "id")?;
            let auth_val: Option<String> = auth.get("auth").and_then(|v| v.as_str()).map(|s| s.to_string());
            let endpoint_uuid: Uuid = extract_uuid(auth, "endpoint_uuid")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(auth, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(auth, "updated_at");

            transaction
                .execute(
                    "INSERT INTO auths (id, uuid, auth, endpoint_uuid, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)",
                    &[&auth_id, &auth_uuid, &auth_val, &endpoint_uuid, &created_at, &updated_at],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert auth '{}': {e}", auth_id)))?;

            auths_imported += 1;
        }

        // 4g. Insert templates + organization_templates
        let mut templates_imported = 0;
        for template in &artifact.templates {
            let tmpl_uuid: Uuid = extract_uuid(template, "uuid")?;
            let tmpl_id: String = extract_string(template, "id")?;
            let tmpl_body: Option<serde_json::Value> = template.get("template").cloned();
            let description: Option<String> = template.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            let llm_recommendation: Option<String> = template.get("llm_recommendation").and_then(|v| v.as_str()).map(|s| s.to_string());
            let created_by: Uuid = extract_uuid(template, "created_by")?;
            let updated_by: Uuid = extract_uuid(template, "updated_by")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(template, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(template, "updated_at");

            transaction
            .execute(
                "INSERT INTO templates (id, uuid, template, description, llm_recommendation, created_by, updated_by, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[&tmpl_id, &tmpl_uuid, &tmpl_body, &description, &llm_recommendation, &created_by, &updated_by, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert template '{}': {e}", tmpl_id)))?;

            transaction
                .execute(
                    "INSERT INTO organization_templates (organization_uuid, template_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &tmpl_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_templates: {e}")))?;

            templates_imported += 1;
        }

        // 4h. Insert workflows + organization_workflows
        let mut workflows_imported = 0;
        for workflow in &artifact.workflows {
            let wf_uuid: Uuid = extract_uuid(workflow, "uuid")?;
            let wf_id: String = extract_string(workflow, "id")?;
            let dag: Option<serde_json::Value> = workflow.get("dag").cloned();
            let description: Option<String> = workflow.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            let created_by: Uuid = extract_uuid(workflow, "created_by")?;
            let updated_by: Uuid = extract_uuid(workflow, "updated_by")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(workflow, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(workflow, "updated_at");

            transaction
            .execute(
                "INSERT INTO workflows (id, uuid, dag, description, created_by, updated_by, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[&wf_id, &wf_uuid, &dag, &description, &created_by, &updated_by, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert workflow '{}': {e}", wf_id)))?;

            transaction
                .execute(
                    "INSERT INTO organization_workflows (organization_uuid, workflow_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &wf_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_workflows: {e}")))?;

            workflows_imported += 1;
        }

        // 4i. Insert workflow_templates junctions
        for wt in &artifact.workflow_templates {
            let wf_uuid: Uuid = extract_uuid(wt, "workflow_uuid")?;
            let tmpl_uuid: Uuid = extract_uuid(wt, "template_uuid")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(wt, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(wt, "updated_at");

            transaction
            .execute(
                "INSERT INTO workflow_templates (workflow_uuid, template_uuid, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[&wf_uuid, &tmpl_uuid, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert workflow_templates: {e}")))?;
        }

        // 4j. Insert organization_apis
        for oa in &artifact.organization_apis {
            let api_uuid: Uuid = extract_uuid(oa, "api_uuid")?;
            transaction
                .execute(
                    "INSERT INTO organization_apis (organization_uuid, api_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &api_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_apis: {e}")))?;
        }

        // 4k. Insert interlays (must precede organization_interlays FK)
        let mut interlays_imported = 0;
        for interlay in &artifact.interlays {
            let il_uuid: Uuid = extract_uuid(interlay, "uuid")?;
            let il_id: String = extract_string(interlay, "id")?;
            let description: Option<String> = interlay.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            let endpoint: Option<Uuid> = interlay.get("endpoint").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok());
            let port: Option<i32> = interlay.get("port").and_then(|v| v.as_i64()).map(|n| n as i32);
            let listeners: Option<serde_json::Value> = interlay.get("listeners").cloned();
            let advertise_host: Option<String> = interlay.get("advertise_host").and_then(|v| v.as_str()).map(|s| s.to_string());
            let tls: Option<serde_json::Value> = interlay.get("tls").cloned();
            let settings: Option<serde_json::Value> = interlay.get("settings").cloned();
            let migration: Option<serde_json::Value> = interlay.get("migration").cloned();
            let created_by: Uuid = extract_uuid(interlay, "created_by")?;
            let updated_by: Uuid = extract_uuid(interlay, "updated_by")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(interlay, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(interlay, "updated_at");

            transaction
            .execute(
                "INSERT INTO interlays (id, uuid, description, endpoint, port, listeners, advertise_host, tls, settings, migration, created_by, updated_by, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                &[&il_id, &il_uuid, &description, &endpoint, &port, &listeners, &advertise_host, &tls, &settings, &migration, &created_by, &updated_by, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert interlay '{}': {e}", il_id)))?;

            interlays_imported += 1;
        }

        // 4l. Insert organization_interlays
        for oi in &artifact.organization_interlays {
            let interlay_uuid: Uuid = extract_uuid(oi, "interlay_uuid")?;
            transaction
                .execute(
                    "INSERT INTO organization_interlays (organization_uuid, interlay_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &interlay_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_interlays: {e}")))?;
        }

        // 4m. Insert organization_migrations
        for om in &artifact.organization_migrations {
            let migration_uuid: Uuid = extract_uuid(om, "migration_uuid")?;
            transaction
                .execute(
                    "INSERT INTO organization_migrations (organization_uuid, migration_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &migration_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_migrations: {e}")))?;
        }

        // 4n. Insert robots + organization_robots
        let mut robots_imported = 0;
        for robot in &artifact.robots {
            let robot_uuid: Uuid = extract_uuid(robot, "uuid")?;
            let username: String = extract_string(robot, "username")?;
            let api_key: serde_json::Value = robot.get("api_key").cloned().unwrap_or(serde_json::Value::Null);
            let description: Option<String> = robot.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            let ttl: Option<i64> = robot.get("ttl").and_then(|v| v.as_i64());
            let expires_at: Option<DateTime<Utc>> = parse_timestamp(robot, "expires_at");
            let created_by: Uuid = extract_uuid(robot, "created_by")?;
            let updated_by: Uuid = extract_uuid(robot, "updated_by")?;
            let created_at: Option<DateTime<Utc>> = parse_timestamp(robot, "created_at");
            let updated_at: Option<DateTime<Utc>> = parse_timestamp(robot, "updated_at");

            transaction
            .execute(
                "INSERT INTO robots (uuid, username, organization_uuid, api_key, description, ttl, expires_at, created_by, updated_by, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[&robot_uuid, &username, &org_uuid, &api_key, &description, &ttl, &expires_at, &created_by, &updated_by, &created_at, &updated_at],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to insert robot '{}': {e}", username)))?;

            transaction
                .execute(
                    "INSERT INTO organization_robots (organization_uuid, robot_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&org_uuid, &robot_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to insert organization_robots: {e}")))?;

            robots_imported += 1;
        }

        // Commit the Postgres transaction
        transaction.commit().await.map_err(|e| EpError::transaction(format!("Failed to commit import transaction: {e}")))?;

        // -- 5. Restore internal cache keys (non-fatal) --
        let expected_prefix = format!("org:{}:", org_uuid);
        let redis_cache_keys_restored = restore_internal_cache_snapshot(self, "cache", &artifact.redis_cache, &expected_prefix).await;
        let redis_rbac_keys_restored = restore_internal_cache_snapshot(self, "RBAC", &artifact.redis_rbac, &expected_prefix).await;

        Ok(ImportResult {
            organization_uuid: org_uuid,
            users_imported,
            endpoints_imported,
            auths_imported,
            templates_imported,
            workflows_imported,
            interlays_imported,
            robots_imported,
            redis_cache_keys_restored,
            redis_rbac_keys_restored,
        })
    }
}

/// Restore internal cache keys from a ShardMap snapshot.
/// Returns the number of keys successfully restored.
async fn restore_internal_cache_snapshot<R, P, C>(
    db: &DatabaseManager<R, P, C>,
    cache_name: &str,
    snapshot: &crate::db::internal_cache::InternalCacheSnapshot,
    expected_prefix: &str,
) -> usize
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let _ctx = trace_context().with_feature("org_transfer.import");

    let total = snapshot.entry_count();
    match db.internal_cache().restore_snapshot_with_key_prefix(snapshot, expected_prefix).await {
        Ok(restored) => {
            if restored < total {
                log_warn!(
                    _ctx,
                    "Skipped internal cache entries that did not match expected org prefix",
                    audience = LogAudience::Internal,
                    cache_name = cache_name.to_string(),
                    expected_prefix = expected_prefix.to_string(),
                    skipped = total.saturating_sub(restored)
                );
            }
            restored
        }
        Err(error) => {
            log_warn!(
                _ctx,
                "Failed to restore internal cache snapshot",
                audience = LogAudience::Internal,
                cache_name = cache_name.to_string(),
                error = error.to_string()
            );
            0
        }
    }
}

fn extract_uuid(value: &serde_json::Value, field: &str) -> ResultEP<Uuid> {
    value
        .get(field)
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| EpError::serde(format!("Missing or invalid UUID field '{field}'")))
}

fn extract_string(value: &serde_json::Value, field: &str) -> ResultEP<String> {
    value
        .get(field)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| EpError::serde(format!("Missing string field '{field}'")))
}

fn parse_timestamp(value: &serde_json::Value, field: &str) -> Option<DateTime<Utc>> {
    value.get(field).and_then(|v| v.as_str()).and_then(|s| s.parse::<DateTime<Utc>>().ok())
}
