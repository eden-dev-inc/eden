//! # RBAC — Control Plane / Data Plane Permission Split
//!
//! Composable permission bits replacing the retired tiered RBAC model.
//!
//! - **Control plane** (`ControlPerms`): who can configure Eden — generally
//!   verified against Postgres directly for consistency. The auth layer adds a
//!   narrow in-process org-membership cache to keep bearer revalidation
//!   cheap without broadening control-plane cache semantics.
//! - **Data plane** (`DataPerms`): what operations a user can run at request time
//!   — verified via cache-aside ShardMap backed by Postgres.
//!
//! ## Tables
//!
//! - `rbac_control(org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid, perms)`
//! - `rbac_data(org_uuid, endpoint_uuid, subject_kind, subject_uuid, perms)`
//!
//! Each also has replay-guard tombstone tables. These are not the authoritative
//! mutation log for the domain model; they only record the highest delete
//! version seen so stale replayed grants cannot resurrect access. Higher-version
//! grants can still re-provision access later.

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
    } else {
        use crate::sql_file;
    }
}
use eden_core::error::{EpError, ResultEP};
use eden_core::format::IdKind;
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData, DataPerms, DataPlaneRbacData};
use eden_logger_internal::{LogAudience, log_warn, trace_context};
use std::future::Future;
use uuid::Uuid;

/// Control plane RBAC — always hits Postgres directly for control plane consistency.
pub trait ControlPlaneRbac {
    /// Check whether `subject` holds at least `required` permissions on the
    /// given entity. Returns `false` if no active row exists.
    fn control_plane_verify(
        &self,
        org_uuid: Uuid,
        entity_kind: IdKind,
        entity_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
        required: ControlPerms,
    ) -> impl Future<Output = ResultEP<bool>> + Send;

    /// Grant control plane permissions. Upserts — merges with existing perms.
    ///
    /// # Authorization
    ///
    /// **Callers MUST verify the granter holds `ControlPerms::GRANT` on the target
    /// entity before calling this method.** This method does not perform its own
    /// authorization check — it trusts the caller to have validated permissions
    /// (e.g. via handler-level permission verification before invoking the DB path).
    fn control_plane_grant(
        &self,
        data: &ControlPlaneRbacData,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;

    /// Get the control plane permissions for a specific entity-subject pair.
    fn control_plane_get(
        &self,
        org_uuid: Uuid,
        entity_kind: IdKind,
        entity_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
    ) -> impl Future<Output = ResultEP<ControlPerms>> + Send;

    /// List all entities a subject has control plane access to within an org.
    fn control_plane_list_by_subject(
        &self,
        org_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
    ) -> impl Future<Output = ResultEP<Vec<ControlPlaneRbacData>>> + Send;

    /// List all subjects with control plane access to a specific entity.
    fn control_plane_list_by_entity(
        &self,
        org_uuid: Uuid,
        entity_kind: IdKind,
        entity_uuid: Uuid,
    ) -> impl Future<Output = ResultEP<Vec<ControlPlaneRbacData>>> + Send;

    /// Revoke (soft-delete) a single control plane permission.
    #[allow(clippy::too_many_arguments)]
    fn control_plane_revoke(
        &self,
        org_uuid: Uuid,
        entity_kind: IdKind,
        entity_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;

    /// Remove all control plane permissions for a subject across all entities in an org.
    fn control_plane_remove_subject(
        &self,
        org_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;

    /// Remove all control plane permissions for an entity (all subjects lose access).
    fn control_plane_remove_entity(
        &self,
        org_uuid: Uuid,
        entity_kind: IdKind,
        entity_uuid: Uuid,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;
}

/// Data plane RBAC — uses ShardMap as a cache over Postgres.
pub trait DataPlaneRbac {
    /// Check whether `subject` holds at least `required` permissions on the
    /// given endpoint. Uses cached path for performance.
    fn data_plane_verify(
        &self,
        org_uuid: Uuid,
        endpoint_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
        required: DataPerms,
    ) -> impl Future<Output = ResultEP<bool>> + Send;

    /// Grant data plane permissions. Writes to the internal cache and Postgres.
    fn data_plane_grant(&self, data: &DataPlaneRbacData, version_ms: i64, version_seq: i64) -> impl Future<Output = ResultEP<()>> + Send;

    /// Get the data plane permissions for a specific endpoint-subject pair.
    fn data_plane_get(
        &self,
        org_uuid: Uuid,
        endpoint_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
    ) -> impl Future<Output = ResultEP<DataPerms>> + Send;

    /// List all shared data plane grants for a subject across endpoints.
    fn data_plane_list_by_subject(
        &self,
        org_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
    ) -> impl Future<Output = ResultEP<Vec<DataPlaneRbacData>>> + Send;

    /// List all shared data plane grants on a specific endpoint.
    fn data_plane_list_by_endpoint(
        &self,
        org_uuid: Uuid,
        endpoint_uuid: Uuid,
    ) -> impl Future<Output = ResultEP<Vec<DataPlaneRbacData>>> + Send;

    /// Revoke (soft-delete) a single data plane permission.
    fn data_plane_revoke(
        &self,
        org_uuid: Uuid,
        endpoint_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;

    /// Remove all data plane permissions for an endpoint (all subjects lose access).
    fn data_plane_remove_endpoint(
        &self,
        org_uuid: Uuid,
        endpoint_uuid: Uuid,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;

    /// Remove all data plane permissions for a subject across all endpoints.
    fn data_plane_remove_subject(
        &self,
        org_uuid: Uuid,
        subject_kind: IdKind,
        subject_uuid: Uuid,
        version_ms: i64,
        version_seq: i64,
    ) -> impl Future<Output = ResultEP<()>> + Send;
}

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
    } else {
mod pg_impl {
    use super::*;
    use crate::db::els::ElsCommands;
        use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection, ShardCache};
        use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid};
        use eden_core::format::{EdenUuid, UserUuid};

    impl<R, P, C> ControlPlaneRbac for DatabaseManager<R, P, C>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        async fn control_plane_verify(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            required: ControlPerms,
        ) -> ResultEP<bool> {
            let perms = self.control_plane_get(org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid).await?;
            Ok(perms.contains(required))
        }

        async fn control_plane_grant(&self, data: &ControlPlaneRbacData, version_ms: i64, version_seq: i64) -> ResultEP<()> {
            let perms_str = data.perms.to_perm_string();
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                sql_file!("insert", "rbac_control"),
                &[
                    &data.org_uuid,
                    &data.entity_kind.as_str(),
                    &data.entity_uuid,
                    &data.subject_kind.as_str(),
                    &data.subject_uuid,
                    &perms_str.as_str(),
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to grant control plane RBAC: {e}")))?;

            if rows_affected > 0 && data.entity_kind == IdKind::Organization.as_str() && data.entity_uuid == data.org_uuid {
                self.update_org_membership_cache(
                    data.org_uuid,
                    &data.subject_kind,
                    data.subject_uuid,
                    !data.perms.is_empty(),
                    version_ms,
                    version_seq,
                )
                .await?;
            }
            Ok(())
        }

        async fn control_plane_get(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<ControlPerms> {
            let ek = entity_kind.as_str();
            let sk = subject_kind.as_str();
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_control_verify"), &[&org_uuid, &ek, &entity_uuid, &sk, &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to query control plane RBAC: {e}")))?;

            match rows.first() {
                Some(row) => {
                    let perms_str: String = row.get("perms");
                    ControlPerms::from_perm_str(&perms_str)
                }
                None => Ok(ControlPerms::empty()),
            }
        }

        async fn control_plane_list_by_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<Vec<ControlPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_control_by_subject"), &[&org_uuid, &subject_kind.as_str(), &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list control plane RBAC by subject: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let entity_kind: String = row.get("entity_kind");
                let entity_uuid: Uuid = row.get("entity_uuid");
                let perms_str: String = row.get("perms");
                let perms = ControlPerms::from_perm_str(&perms_str)?;
                result.push(ControlPlaneRbacData {
                    org_uuid,
                    entity_kind,
                    entity_uuid,
                    subject_kind: subject_kind.to_string(),
                    subject_uuid,
                    perms,
                });
            }
            Ok(result)
        }

        async fn control_plane_list_by_entity(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
        ) -> ResultEP<Vec<ControlPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_control_by_entity"), &[&org_uuid, &entity_kind.as_str(), &entity_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list control plane RBAC by entity: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let subject_kind: String = row.get("subject_kind");
                let subject_uuid: Uuid = row.get("subject_uuid");
                let perms_str: String = row.get("perms");
                let perms = ControlPerms::from_perm_str(&perms_str)?;
                result.push(ControlPlaneRbacData {
                    org_uuid,
                    entity_kind: entity_kind.to_string(),
                    entity_uuid,
                    subject_kind,
                    subject_uuid,
                    perms,
                });
            }
            Ok(result)
        }

        async fn control_plane_revoke(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                sql_file!("delete", "rbac_control"),
                &[
                    &org_uuid,
                    &entity_kind.as_str(),
                    &entity_uuid,
                    &subject_kind.as_str(),
                    &subject_uuid,
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to revoke control plane RBAC: {e}")))?;

            if rows_affected > 0 && entity_kind == IdKind::Organization && entity_uuid == org_uuid {
                self.update_org_membership_cache(org_uuid, subject_kind.as_str(), subject_uuid, false, version_ms, version_seq).await?;
            }
            Ok(())
        }

        async fn control_plane_remove_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let subject_kind = subject_kind.as_str();
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                sql_file!("delete", "rbac_control_subject"),
                &[&org_uuid, &subject_kind, &subject_uuid, &version_ms, &version_seq],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to remove control plane RBAC subject: {e}")))?;

            if rows_affected > 0 {
                self.update_org_membership_cache(org_uuid, subject_kind, subject_uuid, false, version_ms, version_seq).await?;
            }
            Ok(())
        }

        async fn control_plane_remove_entity(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                sql_file!("delete", "rbac_control_entity"),
                &[&org_uuid, &entity_kind.as_str(), &entity_uuid, &version_ms, &version_seq],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to remove control plane RBAC entity: {e}")))?;
            if rows_affected > 0 && entity_kind == IdKind::Organization && entity_uuid == org_uuid {
                self.internal_cache().rbac_org_membership_clear_org(org_uuid).await?;
            }
            Ok(())
        }
    }

    impl<R, P, C> DataPlaneRbac for DatabaseManager<R, P, C>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        async fn data_plane_verify(
            &self,
            _org_uuid: Uuid,
            endpoint_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            required: DataPerms,
        ) -> ResultEP<bool> {
            let perms = self.data_plane_get(_org_uuid, endpoint_uuid, subject_kind, subject_uuid).await?;
            Ok(perms.contains(required))
        }

        async fn data_plane_grant(&self, data: &DataPlaneRbacData, version_ms: i64, version_seq: i64) -> ResultEP<()> {
            let perms_str = data.perms.to_perm_string();

            // Write to Postgres first (source of truth)
            let conn = self.pg_connection().await?;
            conn.execute(
                sql_file!("insert", "rbac_data"),
                &[
                    &data.org_uuid,
                    &data.endpoint_uuid,
                    &data.subject_kind.as_str(),
                    &data.subject_uuid,
                    &perms_str.as_str(),
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to grant data plane RBAC: {e}")))?;

            // Then update cache (best-effort — next data_plane_get miss will re-populate)
            let _ = self.internal_cache().rbac_data_set(data.endpoint_uuid, data.subject_uuid, data.perms).await;

            Ok(())
        }

        async fn data_plane_get(
            &self,
            org_uuid: Uuid,
            endpoint_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<DataPerms> {
            match self.internal_cache().rbac_data_get(endpoint_uuid, subject_uuid).await {
                Ok(Some(perms)) => return Ok(perms),
                Ok(None) => {}
                Err(error) => {
                    let _ctx = trace_context().with_feature("rbac.data_plane");
                    log_warn!(
                        _ctx,
                        "RBAC data-plane cache read failed; falling back to Postgres",
                        audience = LogAudience::Internal,
                        endpoint_uuid = endpoint_uuid.to_string(),
                        subject_uuid = subject_uuid.to_string(),
                        error = error.to_string()
                    );
                }
            }

            // Fallback to Postgres
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(
                    sql_file!("select", "rbac_data_verify"),
                    &[&org_uuid, &endpoint_uuid, &subject_kind.as_str(), &subject_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to query data plane RBAC: {e}")))?;

            match rows.first() {
                Some(row) => {
                    let perms_str: String = row.get("perms");
                    let perms = DataPerms::from_perm_str(&perms_str)?;

                    let _ = self.internal_cache().rbac_data_set(endpoint_uuid, subject_uuid, perms).await;

                    Ok(perms)
                }
                None => Ok(DataPerms::empty()),
            }
        }

        async fn data_plane_list_by_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<Vec<DataPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_data_by_subject"), &[&org_uuid, &subject_kind.as_str(), &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list data plane RBAC by subject: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let endpoint_uuid: Uuid = row.get("endpoint_uuid");
                let perms_str: String = row.get("perms");
                let perms = DataPerms::from_perm_str(&perms_str)?;
                result.push(DataPlaneRbacData {
                    org_uuid,
                    endpoint_uuid,
                    subject_kind: subject_kind.to_string(),
                    subject_uuid,
                    perms,
                });
            }
            Ok(result)
        }

        async fn data_plane_list_by_endpoint(&self, org_uuid: Uuid, endpoint_uuid: Uuid) -> ResultEP<Vec<DataPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_data_by_endpoint"), &[&org_uuid, &endpoint_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list data plane RBAC by endpoint: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let subject_kind: String = row.get("subject_kind");
                let subject_uuid: Uuid = row.get("subject_uuid");
                let perms_str: String = row.get("perms");
                let perms = DataPerms::from_perm_str(&perms_str)?;
                result.push(DataPlaneRbacData { org_uuid, endpoint_uuid, subject_kind, subject_uuid, perms });
            }
            Ok(result)
        }

        async fn data_plane_revoke(
            &self,
            org_uuid: Uuid,
            endpoint_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            // Soft-delete in Postgres first (source of truth)
            let conn = self.pg_connection().await?;
            conn.execute(
                sql_file!("delete", "rbac_data"),
                &[
                    &org_uuid,
                    &endpoint_uuid,
                    &subject_kind.as_str(),
                    &subject_uuid,
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to revoke data plane RBAC: {e}")))?;

            // Remove from cache after Postgres commit so stale permissions do not survive.
            self.internal_cache().rbac_data_del(endpoint_uuid, subject_uuid).await?;

            Ok(())
        }

        async fn data_plane_remove_endpoint(&self, org_uuid: Uuid, endpoint_uuid: Uuid, version_ms: i64, version_seq: i64) -> ResultEP<()> {
            // Soft-delete in Postgres first (source of truth)
            let conn = self.pg_connection().await?;
            conn.execute(sql_file!("delete", "rbac_data_entity"), &[&org_uuid, &endpoint_uuid, &version_ms, &version_seq])
                .await
                .map_err(|e| EpError::database(format!("Failed to remove data plane RBAC endpoint: {e}")))?;

            // Then clear cached entries for this endpoint (best-effort)
            let _ = self.internal_cache().rbac_data_clear_endpoint(endpoint_uuid).await;

            Ok(())
        }

        async fn data_plane_remove_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let subject_kind = subject_kind.as_str();
            let mut conn = self.pg_connection().await?;
            let tx = conn
                .transaction()
                .await
                .map_err(|e| EpError::database(format!("Failed to start transaction for data plane RBAC subject remove: {e}")))?;

            // Query affected endpoints inside the transaction (snapshot-isolated
            // from concurrent grants) so the cached entries we clear match exactly
            // what the soft-delete will deactivate.
            let affected = tx
                .query(sql_file!("select", "rbac_data_by_subject"), &[&org_uuid, &subject_kind, &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to query data plane RBAC subject endpoints: {e}")))?;

            // Soft-delete in Postgres
            tx.execute(
                sql_file!("delete", "rbac_data_subject"),
                &[&org_uuid, &subject_kind, &subject_uuid, &version_ms, &version_seq],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to remove data plane RBAC subject: {e}")))?;

            tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit data plane RBAC subject remove: {e}")))?;

            // Clear cache entries for each affected endpoint (best-effort)
            if !affected.is_empty() {
                for row in &affected {
                    let endpoint_uuid: Uuid = row.get("endpoint_uuid");
                    let _ = self.internal_cache().rbac_data_del(endpoint_uuid, subject_uuid).await;
                }
            }

            Ok(())
        }
    }

    impl<R, P, C> DatabaseManager<R, P, C>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        async fn update_org_membership_cache(
            &self,
            org_uuid: Uuid,
            subject_kind: &str,
            subject_uuid: Uuid,
            is_member: bool,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            self.internal_cache()
                .rbac_org_membership_set(org_uuid, subject_kind, subject_uuid, is_member, version_ms, version_seq)
                .await?;

            Ok(())
        }

        /// Fast org-membership check for auth revalidation.
        ///
        /// This is intentionally narrower than general control-plane RBAC:
        /// it uses the internal cache as a versioned membership-state cache
        /// for the `(organization, subject)` row, then falls back to the
        /// authoritative Postgres lookup on cache miss or decode failure.
        pub async fn control_plane_has_org_access_cached(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<bool> {
            match self
                .internal_cache()
                .rbac_org_membership_get(org_uuid, subject_kind.as_str(), subject_uuid)
                .await
            {
                Ok(Some(cached_value)) => return Ok(cached_value.is_member),
                Ok(None) => {}
                Err(error) => {
                    let _ctx = trace_context().with_feature("rbac.control_plane");
                    log_warn!(
                        _ctx,
                        "RBAC org-membership cache read failed; falling back to Postgres",
                        audience = LogAudience::Internal,
                        org_uuid = org_uuid.to_string(),
                        subject_uuid = subject_uuid.to_string(),
                        error = error.to_string()
                    );
                }
            }

            let perms = self.control_plane_get(org_uuid, IdKind::Organization, org_uuid, subject_kind, subject_uuid).await?;
            Ok(!perms.is_empty())
        }

        pub async fn control_plane_grant_endpoint_users_exclusive(
            &self,
            endpoint: &EndpointCacheUuid,
            grants: &[(UserUuid, ControlPerms)],
            version_ms: i64,
        ) -> ResultEP<()> {
            if grants.is_empty() {
                return Ok(());
            }

            let org_uuid = endpoint.org().ok_or_else(|| EpError::parse("Endpoint cache key is missing org context".to_string()))?.uuid();
            let endpoint_uuid = endpoint.uuid();

            let mut conn = self.pg_connection().await?;
            let tx = conn
                .transaction()
                .await
                .map_err(|e| EpError::database(format!("Failed to start endpoint RBAC exclusivity transaction: {e}")))?;

            for (index, (user_uuid, perms)) in grants.iter().enumerate() {
                let version_seq = i64::try_from(index).map_err(|e| EpError::database(format!("RBAC grant version overflow: {e}")))?;
                let perms_str = perms.to_perm_string();

                tx.execute(
                    sql_file!("insert", "rbac_control"),
                    &[
                        &org_uuid,
                        &IdKind::Endpoint.as_str(),
                        &endpoint_uuid,
                        &IdKind::User.as_str(),
                        &user_uuid.uuid(),
                        &perms_str.as_str(),
                        &version_ms,
                        &version_seq,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to grant endpoint RBAC: {e}")))?;

                tx.execute(sql_file!("delete", "els_policy_assignment"), &[&endpoint_uuid, &user_uuid, &org_uuid])
                    .await
                    .map_err(|e| EpError::database(format!("Failed to clear ELS assignment during RBAC grant: {e}")))?;
            }

            tx.commit()
                .await
                .map_err(|e| EpError::database(format!("Failed to commit endpoint RBAC exclusivity transaction: {e}")))?;

            let user_uuids = grants.iter().map(|(user_uuid, _)| user_uuid.clone()).collect::<Vec<_>>();
            self.els_uncache_users(endpoint, &user_uuids).await?;

            Ok(())
        }

        pub async fn data_plane_grant_endpoint_users_exclusive(
            &self,
            endpoint: &EndpointCacheUuid,
            grants: &[(UserUuid, DataPerms)],
            version_ms: i64,
        ) -> ResultEP<()> {
            if grants.is_empty() {
                return Ok(());
            }

            let org_uuid = endpoint.org().ok_or_else(|| EpError::parse("Endpoint cache key is missing org context".to_string()))?.uuid();
            let endpoint_uuid = endpoint.uuid();

            let mut conn = self.pg_connection().await?;
            let tx = conn
                .transaction()
                .await
                .map_err(|e| EpError::database(format!("Failed to start endpoint data-plane RBAC transaction: {e}")))?;

            for (index, (user_uuid, perms)) in grants.iter().enumerate() {
                let version_seq = i64::try_from(index).map_err(|e| EpError::database(format!("RBAC grant version overflow: {e}")))?;
                let perms_str = perms.to_perm_string();

                tx.execute(
                    sql_file!("insert", "rbac_data"),
                    &[
                        &org_uuid,
                        &endpoint_uuid,
                        &IdKind::User.as_str(),
                        &user_uuid.uuid(),
                        &perms_str.as_str(),
                        &version_ms,
                        &version_seq,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to grant endpoint data-plane RBAC: {e}")))?;

                tx.execute(sql_file!("delete", "els_policy_assignment"), &[&endpoint_uuid, &user_uuid, &org_uuid])
                    .await
                    .map_err(|e| EpError::database(format!("Failed to clear ELS assignment during data-plane RBAC grant: {e}")))?;
            }

            tx.commit()
                .await
                .map_err(|e| EpError::database(format!("Failed to commit endpoint data-plane RBAC transaction: {e}")))?;

            let user_uuids = grants.iter().map(|(user_uuid, _)| user_uuid.clone()).collect::<Vec<_>>();
            self.els_uncache_users(endpoint, &user_uuids).await?;

            for (user_uuid, perms) in grants {
                let _ = self.internal_cache().rbac_data_set(endpoint_uuid, user_uuid.uuid(), *perms).await;
            }

            Ok(())
        }
    }
}
    }
}

#[cfg(embedded_db)]
mod local_impl {
    use super::*;
    use crate::db::els::ElsCommands;
    use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection, ShardCache};
    use crate::sql_file;
    use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid};
    use eden_core::format::{EdenUuid, UserUuid};

    impl<R, P, C> ControlPlaneRbac for DatabaseManager<R, P, C>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        async fn control_plane_verify(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            required: ControlPerms,
        ) -> ResultEP<bool> {
            let perms = self.control_plane_get(org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid).await?;
            Ok(perms.contains(required))
        }

        async fn control_plane_grant(&self, data: &ControlPlaneRbacData, version_ms: i64, version_seq: i64) -> ResultEP<()> {
            let perms_str = data.perms.to_perm_string();
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                    sql_file!("insert", "rbac_control"),
                    &[
                        &data.org_uuid,
                        &data.entity_kind.as_str(),
                        &data.entity_uuid,
                        &data.subject_kind.as_str(),
                        &data.subject_uuid,
                        &perms_str.as_str(),
                        &version_ms,
                        &version_seq,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to grant control plane RBAC: {e}")))?;

            if rows_affected > 0 && data.entity_kind == IdKind::Organization.as_str() && data.entity_uuid == data.org_uuid {
                self.update_org_membership_cache_local(
                    data.org_uuid,
                    &data.subject_kind,
                    data.subject_uuid,
                    !data.perms.is_empty(),
                    version_ms,
                    version_seq,
                )
                .await?;
            }
            Ok(())
        }

        async fn control_plane_get(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<ControlPerms> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(
                    sql_file!("select", "rbac_control_verify"),
                    &[
                        &org_uuid,
                        &entity_kind.as_str(),
                        &entity_uuid,
                        &subject_kind.as_str(),
                        &subject_uuid,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to query control plane RBAC: {e}")))?;

            match rows.first() {
                Some(row) => {
                    let perms_str: String = row.get("perms");
                    ControlPerms::from_perm_str(&perms_str)
                }
                None => Ok(ControlPerms::empty()),
            }
        }

        async fn control_plane_list_by_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<Vec<ControlPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_control_by_subject"), &[&org_uuid, &subject_kind.as_str(), &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list control plane RBAC by subject: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let entity_kind: String = row.get("entity_kind");
                let entity_uuid: Uuid = row.get("entity_uuid");
                let perms_str: String = row.get("perms");
                let perms = ControlPerms::from_perm_str(&perms_str)?;
                result.push(ControlPlaneRbacData {
                    org_uuid,
                    entity_kind,
                    entity_uuid,
                    subject_kind: subject_kind.to_string(),
                    subject_uuid,
                    perms,
                });
            }
            Ok(result)
        }

        async fn control_plane_list_by_entity(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
        ) -> ResultEP<Vec<ControlPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_control_by_entity"), &[&org_uuid, &entity_kind.as_str(), &entity_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list control plane RBAC by entity: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let subject_kind: String = row.get("subject_kind");
                let subject_uuid: Uuid = row.get("subject_uuid");
                let perms_str: String = row.get("perms");
                let perms = ControlPerms::from_perm_str(&perms_str)?;
                result.push(ControlPlaneRbacData {
                    org_uuid,
                    entity_kind: entity_kind.to_string(),
                    entity_uuid,
                    subject_kind,
                    subject_uuid,
                    perms,
                });
            }
            Ok(result)
        }

        async fn control_plane_revoke(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                    sql_file!("delete", "rbac_control"),
                    &[
                        &org_uuid,
                        &entity_kind.as_str(),
                        &entity_uuid,
                        &subject_kind.as_str(),
                        &subject_uuid,
                        &version_ms,
                        &version_seq,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to revoke control plane RBAC: {e}")))?;

            if rows_affected > 0 && entity_kind == IdKind::Organization && entity_uuid == org_uuid {
                self.update_org_membership_cache_local(org_uuid, subject_kind.as_str(), subject_uuid, false, version_ms, version_seq)
                    .await?;
            }
            Ok(())
        }

        async fn control_plane_remove_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                    sql_file!("delete", "rbac_control_subject"),
                    &[&org_uuid, &subject_kind.as_str(), &subject_uuid, &version_ms, &version_seq],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to remove control plane RBAC subject: {e}")))?;

            if rows_affected > 0 {
                self.update_org_membership_cache_local(org_uuid, subject_kind.as_str(), subject_uuid, false, version_ms, version_seq)
                    .await?;
            }
            Ok(())
        }

        async fn control_plane_remove_entity(
            &self,
            org_uuid: Uuid,
            entity_kind: IdKind,
            entity_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            let rows_affected = conn
                .execute(
                    sql_file!("delete", "rbac_control_entity"),
                    &[&org_uuid, &entity_kind.as_str(), &entity_uuid, &version_ms, &version_seq],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to remove control plane RBAC entity: {e}")))?;
            if rows_affected > 0 && entity_kind == IdKind::Organization && entity_uuid == org_uuid {
                self.internal_cache().rbac_org_membership_clear_org(org_uuid).await?;
            }
            Ok(())
        }
    }

    impl<R, P, C> DataPlaneRbac for DatabaseManager<R, P, C>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        async fn data_plane_verify(
            &self,
            org_uuid: Uuid,
            endpoint_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            required: DataPerms,
        ) -> ResultEP<bool> {
            let perms = self.data_plane_get(org_uuid, endpoint_uuid, subject_kind, subject_uuid).await?;
            Ok(perms.contains(required))
        }

        async fn data_plane_grant(&self, data: &DataPlaneRbacData, version_ms: i64, version_seq: i64) -> ResultEP<()> {
            let perms_str = data.perms.to_perm_string();
            let conn = self.pg_connection().await?;
            conn.execute(
                sql_file!("insert", "rbac_data"),
                &[
                    &data.org_uuid,
                    &data.endpoint_uuid,
                    &data.subject_kind.as_str(),
                    &data.subject_uuid,
                    &perms_str.as_str(),
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to grant data plane RBAC: {e}")))?;

            let _ = self.internal_cache().rbac_data_set(data.endpoint_uuid, data.subject_uuid, data.perms).await;

            Ok(())
        }

        async fn data_plane_get(
            &self,
            org_uuid: Uuid,
            endpoint_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<DataPerms> {
            match self.internal_cache().rbac_data_get(endpoint_uuid, subject_uuid).await {
                Ok(Some(perms)) => return Ok(perms),
                Ok(None) => {}
                Err(error) => {
                    let _ctx = trace_context().with_feature("rbac.data_plane");
                    log_warn!(
                        _ctx,
                        "RBAC data-plane cache read failed; falling back to Postgres",
                        audience = LogAudience::Internal,
                        endpoint_uuid = endpoint_uuid.to_string(),
                        subject_uuid = subject_uuid.to_string(),
                        error = error.to_string()
                    );
                }
            }

            let conn = self.pg_connection().await?;
            let rows = conn
                .query(
                    sql_file!("select", "rbac_data_verify"),
                    &[&org_uuid, &endpoint_uuid, &subject_kind.as_str(), &subject_uuid],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to query data plane RBAC: {e}")))?;

            match rows.first() {
                Some(row) => {
                    let perms_str: String = row.get("perms");
                    let perms = DataPerms::from_perm_str(&perms_str)?;
                    let _ = self.internal_cache().rbac_data_set(endpoint_uuid, subject_uuid, perms).await;
                    Ok(perms)
                }
                None => Ok(DataPerms::empty()),
            }
        }

        async fn data_plane_list_by_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<Vec<DataPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_data_by_subject"), &[&org_uuid, &subject_kind.as_str(), &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list data plane RBAC by subject: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let endpoint_uuid: Uuid = row.get("endpoint_uuid");
                let perms_str: String = row.get("perms");
                let perms = DataPerms::from_perm_str(&perms_str)?;
                result.push(DataPlaneRbacData {
                    org_uuid,
                    endpoint_uuid,
                    subject_kind: subject_kind.to_string(),
                    subject_uuid,
                    perms,
                });
            }
            Ok(result)
        }

        async fn data_plane_list_by_endpoint(&self, org_uuid: Uuid, endpoint_uuid: Uuid) -> ResultEP<Vec<DataPlaneRbacData>> {
            let conn = self.pg_connection().await?;
            let rows = conn
                .query(sql_file!("select", "rbac_data_by_endpoint"), &[&org_uuid, &endpoint_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to list data plane RBAC by endpoint: {e}")))?;

            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let subject_kind: String = row.get("subject_kind");
                let subject_uuid: Uuid = row.get("subject_uuid");
                let perms_str: String = row.get("perms");
                let perms = DataPerms::from_perm_str(&perms_str)?;
                result.push(DataPlaneRbacData { org_uuid, endpoint_uuid, subject_kind, subject_uuid, perms });
            }
            Ok(result)
        }

        async fn data_plane_revoke(
            &self,
            org_uuid: Uuid,
            endpoint_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            conn.execute(
                sql_file!("delete", "rbac_data"),
                &[
                    &org_uuid,
                    &endpoint_uuid,
                    &subject_kind.as_str(),
                    &subject_uuid,
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to revoke data plane RBAC: {e}")))?;

            self.internal_cache().rbac_data_del(endpoint_uuid, subject_uuid).await?;

            Ok(())
        }

        async fn data_plane_remove_endpoint(&self, org_uuid: Uuid, endpoint_uuid: Uuid, version_ms: i64, version_seq: i64) -> ResultEP<()> {
            let conn = self.pg_connection().await?;
            conn.execute(sql_file!("delete", "rbac_data_entity"), &[&org_uuid, &endpoint_uuid, &version_ms, &version_seq])
                .await
                .map_err(|e| EpError::database(format!("Failed to remove data plane RBAC endpoint: {e}")))?;

            self.internal_cache().rbac_data_clear_endpoint(endpoint_uuid).await?;

            Ok(())
        }

        async fn data_plane_remove_subject(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            let subject_kind = subject_kind.as_str();
            let conn = self.pg_connection().await?;
            let tx = conn
                .transaction()
                .await
                .map_err(|e| EpError::database(format!("Failed to start transaction for data plane RBAC subject remove: {e}")))?;

            let affected = tx
                .query(sql_file!("select", "rbac_data_by_subject"), &[&org_uuid, &subject_kind, &subject_uuid])
                .await
                .map_err(|e| EpError::database(format!("Failed to query data plane RBAC subject endpoints: {e}")))?;

            tx.execute(
                sql_file!("delete", "rbac_data_subject"),
                &[&org_uuid, &subject_kind, &subject_uuid, &version_ms, &version_seq],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to remove data plane RBAC subject: {e}")))?;

            tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit data plane RBAC subject remove: {e}")))?;

            for row in &affected {
                let endpoint_uuid: Uuid = row.get("endpoint_uuid");
                let _ = self.internal_cache().rbac_data_del(endpoint_uuid, subject_uuid).await;
            }

            Ok(())
        }
    }

    impl<R, P, C> DatabaseManager<R, P, C>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        async fn update_org_membership_cache_local(
            &self,
            org_uuid: Uuid,
            subject_kind: &str,
            subject_uuid: Uuid,
            is_member: bool,
            version_ms: i64,
            version_seq: i64,
        ) -> ResultEP<()> {
            self.internal_cache()
                .rbac_org_membership_set(org_uuid, subject_kind, subject_uuid, is_member, version_ms, version_seq)
                .await
        }

        pub async fn control_plane_has_org_access_cached(
            &self,
            org_uuid: Uuid,
            subject_kind: IdKind,
            subject_uuid: Uuid,
        ) -> ResultEP<bool> {
            match self.internal_cache().rbac_org_membership_get(org_uuid, subject_kind.as_str(), subject_uuid).await {
                Ok(Some(cached_value)) => return Ok(cached_value.is_member),
                Ok(None) => {}
                Err(error) => {
                    let _ctx = trace_context().with_feature("rbac.control_plane");
                    log_warn!(
                        _ctx,
                        "RBAC org-membership cache read failed; falling back to Postgres",
                        audience = LogAudience::Internal,
                        org_uuid = org_uuid.to_string(),
                        subject_uuid = subject_uuid.to_string(),
                        error = error.to_string()
                    );
                }
            }

            let perms = self.control_plane_get(org_uuid, IdKind::Organization, org_uuid, subject_kind, subject_uuid).await?;
            Ok(!perms.is_empty())
        }

        pub async fn control_plane_grant_endpoint_users_exclusive(
            &self,
            endpoint: &EndpointCacheUuid,
            grants: &[(UserUuid, ControlPerms)],
            version_ms: i64,
        ) -> ResultEP<()> {
            if grants.is_empty() {
                return Ok(());
            }

            let org_uuid = endpoint.org().ok_or_else(|| EpError::parse("Endpoint cache key is missing org context".to_string()))?.uuid();
            let endpoint_uuid = endpoint.uuid();

            let conn = self.pg_connection().await?;
            let tx = conn
                .transaction()
                .await
                .map_err(|e| EpError::database(format!("Failed to start endpoint RBAC exclusivity transaction: {e}")))?;

            for (index, (user_uuid, perms)) in grants.iter().enumerate() {
                let version_seq = i64::try_from(index).map_err(|e| EpError::database(format!("RBAC grant version overflow: {e}")))?;
                let perms_str = perms.to_perm_string();

                tx.execute(
                    sql_file!("insert", "rbac_control"),
                    &[
                        &org_uuid,
                        &IdKind::Endpoint.as_str(),
                        &endpoint_uuid,
                        &IdKind::User.as_str(),
                        &user_uuid.uuid(),
                        &perms_str.as_str(),
                        &version_ms,
                        &version_seq,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to grant endpoint RBAC: {e}")))?;

                tx.execute(sql_file!("delete", "els_policy_assignment"), &[&endpoint_uuid, &user_uuid, &org_uuid])
                    .await
                    .map_err(|e| EpError::database(format!("Failed to clear ELS assignment during RBAC grant: {e}")))?;
            }

            tx.commit()
                .await
                .map_err(|e| EpError::database(format!("Failed to commit endpoint RBAC exclusivity transaction: {e}")))?;

            let user_uuids = grants.iter().map(|(user_uuid, _)| user_uuid.clone()).collect::<Vec<_>>();
            self.els_uncache_users(endpoint, &user_uuids).await?;

            Ok(())
        }

        pub async fn data_plane_grant_endpoint_users_exclusive(
            &self,
            endpoint: &EndpointCacheUuid,
            grants: &[(UserUuid, DataPerms)],
            version_ms: i64,
        ) -> ResultEP<()> {
            if grants.is_empty() {
                return Ok(());
            }

            let org_uuid = endpoint.org().ok_or_else(|| EpError::parse("Endpoint cache key is missing org context".to_string()))?.uuid();
            let endpoint_uuid = endpoint.uuid();

            let conn = self.pg_connection().await?;
            let tx = conn
                .transaction()
                .await
                .map_err(|e| EpError::database(format!("Failed to start endpoint data-plane RBAC transaction: {e}")))?;

            for (index, (user_uuid, perms)) in grants.iter().enumerate() {
                let version_seq = i64::try_from(index).map_err(|e| EpError::database(format!("RBAC grant version overflow: {e}")))?;
                let perms_str = perms.to_perm_string();

                tx.execute(
                    sql_file!("insert", "rbac_data"),
                    &[
                        &org_uuid,
                        &endpoint_uuid,
                        &IdKind::User.as_str(),
                        &user_uuid.uuid(),
                        &perms_str.as_str(),
                        &version_ms,
                        &version_seq,
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to grant endpoint data-plane RBAC: {e}")))?;

                tx.execute(sql_file!("delete", "els_policy_assignment"), &[&endpoint_uuid, &user_uuid, &org_uuid])
                    .await
                    .map_err(|e| EpError::database(format!("Failed to clear ELS assignment during data-plane RBAC grant: {e}")))?;
            }

            tx.commit()
                .await
                .map_err(|e| EpError::database(format!("Failed to commit endpoint data-plane RBAC transaction: {e}")))?;

            let user_uuids = grants.iter().map(|(user_uuid, _)| user_uuid.clone()).collect::<Vec<_>>();
            self.els_uncache_users(endpoint, &user_uuids).await?;

            for (user_uuid, perms) in grants {
                let _ = self.internal_cache().rbac_data_set(endpoint_uuid, user_uuid.uuid(), *perms).await;
            }

            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_plane_rbac_data_construction() {
        let data = ControlPlaneRbacData {
            org_uuid: Uuid::nil(),
            entity_kind: "endpoint".to_string(),
            entity_uuid: Uuid::nil(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::nil(),
            perms: ControlPerms::READ | ControlPerms::CONFIGURE,
        };
        assert!(data.perms.contains(ControlPerms::READ));
        assert!(data.perms.contains(ControlPerms::CONFIGURE));
        assert!(!data.perms.contains(ControlPerms::PROMOTE));
    }

    #[test]
    fn test_data_plane_rbac_data_construction() {
        let data = DataPlaneRbacData {
            org_uuid: Uuid::nil(),
            endpoint_uuid: Uuid::nil(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::nil(),
            perms: DataPerms::READ | DataPerms::WRITE,
        };
        assert!(data.perms.contains(DataPerms::READ));
        assert!(data.perms.contains(DataPerms::WRITE));
        assert!(!data.perms.contains(DataPerms::EXECUTE));
    }
}

// ---------------------------------------------------------------------------
// Integration tests (require Postgres via testcontainers)
// ---------------------------------------------------------------------------

cfg_if::cfg_if! {
    if #[cfg(all(test, feature = "infra-tests", embedded_db))] {
        mod infra_tests {}
    } else if #[cfg(all(test, feature = "infra-tests"))] {
mod infra_tests {
    use super::*;
    use crate::db::lib::mocks::{MockClickhouseConnection, MockRedisConnection};
    use crate::db::lib::{CacheTtl, DatabaseManager, EdenPostgresConnection, PgConn, create_postgres_connection};
    use crate::sql_file;
    use crate::test_utils::database_test_utils::create_postgres;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
    use std::sync::Mutex;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::ContainerAsync;
    use tokio::sync::OnceCell;

    type TestDb = DatabaseManager<MockRedisConnection, PgConn, MockClickhouseConnection>;

    struct SharedPg {
        container: Mutex<Option<ContainerAsync<Postgres>>>,
        pg_connection_string: String,
    }

    static SHARED_DB: OnceCell<SharedPg> = OnceCell::const_new();

    async fn shared_db() -> &'static SharedPg {
        SHARED_DB
            .get_or_init(|| async {
                let (pg_container, pg_connection_string) = create_postgres().await;
                let pg_pool = create_postgres_connection(&pg_connection_string).await.expect("create postgres pool");
                pg_pool.batch_execute(sql_file!("create", "organizations")).await.expect("create organizations");
                pg_pool.batch_execute(sql_file!("create", "rbac_types")).await.expect("create rbac_types");
                pg_pool.batch_execute(sql_file!("create", "rbac_control")).await.expect("create rbac_control");
                pg_pool
                    .batch_execute(sql_file!("create", "rbac_control_entity_tombstones"))
                    .await
                    .expect("create rbac_control_entity_tombstones");
                pg_pool
                    .batch_execute(sql_file!("create", "rbac_control_subject_tombstones"))
                    .await
                    .expect("create rbac_control_subject_tombstones");
                pg_pool.batch_execute(sql_file!("create", "rbac_data")).await.expect("create rbac_data");
                pg_pool
                    .batch_execute(sql_file!("create", "rbac_data_entity_tombstones"))
                    .await
                    .expect("create rbac_data_entity_tombstones");
                pg_pool
                    .batch_execute(sql_file!("create", "rbac_data_subject_tombstones"))
                    .await
                    .expect("create rbac_data_subject_tombstones");

                SharedPg {
                    container: Mutex::new(Some(pg_container)),
                    pg_connection_string,
                }
            })
            .await
    }

    async fn test_db() -> TestDb {
        let db = shared_db().await;
        let pg_pool = create_postgres_connection(&db.pg_connection_string).await.expect("create postgres pool");
        DatabaseManager::new_with_connections(
            MockRedisConnection::new(true), // cache connection — local cache handle
            MockRedisConnection::new(true), // rbac connection — local cache handle
            pg_pool,
            MockClickhouseConnection::new(false),
            CacheTtl::from_secs(60),
            None,
        )
    }

    #[tokio::test]
    async fn control_plane_has_org_access_cached_falls_back_to_postgres() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();

        assert!(!db.control_plane_has_org_access_cached(org, IdKind::User, subject).await.expect("check access before grant"));

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            },
            1,
            0,
        )
        .await
        .expect("grant org membership");

        assert!(db.control_plane_has_org_access_cached(org, IdKind::User, subject).await.expect("check access after grant"));
    }

    #[tokio::test]
    async fn control_plane_has_org_access_cached_ignores_empty_org_grants() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::empty(),
            },
            1,
            0,
        )
        .await
        .expect("grant empty org membership row");

        assert!(
            !db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
                .expect("empty org grants must not count as membership")
        );
    }

    #[tokio::test]
    async fn control_plane_has_org_access_cached_tracks_revocation() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            },
            1,
            0,
        )
        .await
        .expect("grant org membership");

        assert!(db.control_plane_has_org_access_cached(org, IdKind::User, subject).await.expect("check access after grant"));

        db.control_plane_revoke(org, IdKind::Organization, org, IdKind::User, subject, 2, 0)
            .await
            .expect("revoke org membership");

        assert!(
            !db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
            .expect("revoked org membership should fail access check")
        );
    }

    #[tokio::test]
    async fn org_membership_cache_ignores_stale_grant() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            },
            20,
            0,
        )
        .await
        .expect("grant newer org membership");

        assert!(db.control_plane_has_org_access_cached(org, IdKind::User, subject).await.expect("newer grant is cached"));

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::empty(),
            },
            10,
            0,
        )
        .await
        .expect("stale grant is ignored by database");

        assert!(
            db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
                .expect("stale grant must not overwrite membership cache")
        );
    }

    #[tokio::test]
    async fn org_membership_cache_ignores_stale_revoke() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            },
            20,
            0,
        )
        .await
        .expect("grant newer org membership");

        db.control_plane_revoke(org, IdKind::Organization, org, IdKind::User, subject, 10, 0)
            .await
            .expect("stale revoke is ignored by database");

        assert!(
            db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
                .expect("stale revoke must not overwrite membership cache")
        );

        db.control_plane_revoke(org, IdKind::Organization, org, IdKind::User, subject, 30, 0)
            .await
            .expect("fresh revoke applies");

        assert!(
            !db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
                .expect("fresh revoke should overwrite membership cache")
        );
    }

    #[tokio::test]
    async fn org_membership_cache_ignores_stale_org_removal() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: IdKind::Organization.to_string(),
                entity_uuid: org,
                subject_kind: IdKind::User.to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            },
            20,
            0,
        )
        .await
        .expect("grant newer org membership");

        db.control_plane_remove_entity(org, IdKind::Organization, org, 10, 0)
            .await
            .expect("stale org removal is ignored by database");

        assert!(
            db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
                .expect("stale org removal must not clear membership cache")
        );

        db.control_plane_remove_entity(org, IdKind::Organization, org, 30, 0)
            .await
            .expect("fresh org removal applies");

        assert!(
            !db.control_plane_has_org_access_cached(org, IdKind::User, subject)
                .await
                .expect("fresh org removal should clear membership cache")
        );
    }

    #[ctor::dtor]
    fn shutdown_shared_pg_container() {
        let Some(shared) = SHARED_DB.get() else {
            return;
        };
        let Some(container) = shared.container.lock().expect("lock shared pg container").take() else {
            return;
        };
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("create teardown runtime");
            runtime.block_on(async move {
                let _ = container.rm().await;
            });
        })
        .join();
    }

    async fn insert_org(db: &TestDb, org_uuid: Uuid) {
        let conn = db.pg_connection().await.expect("postgres connection");
        let org_id = format!("org-{org_uuid}");
        conn.execute(
            "INSERT INTO organizations (id, uuid, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) ON CONFLICT (uuid) DO NOTHING",
            &[&org_id, &org_uuid],
        )
        .await
        .expect("insert organization");
    }

    // -----------------------------------------------------------------------
    // Control Plane tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_grant_and_verify() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ | ControlPerms::CONFIGURE,
        };

        db.control_plane_grant(&data, 100, 0).await.expect("control_plane_grant");

        // Verify passes for subset
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, ControlPerms::READ)
            .await
            .expect("control_plane_verify READ");
        assert!(ok);

        // Verify passes for exact set
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, ControlPerms::READ | ControlPerms::CONFIGURE)
            .await
            .expect("control_plane_verify RC");
        assert!(ok);

        // Verify fails for superset
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, ControlPerms::GRANT)
            .await
            .expect("control_plane_verify GRANT");
        assert!(!ok);
    }

    #[tokio::test]
    async fn control_plane_get_returns_empty_for_unknown() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let perms = db
            .control_plane_get(org, IdKind::Endpoint, Uuid::new_v4(), IdKind::User, Uuid::new_v4())
            .await
            .expect("control_plane_get unknown");
        assert_eq!(perms, ControlPerms::empty());
    }

    #[tokio::test]
    async fn control_plane_revoke_soft_deletes() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };

        db.control_plane_grant(&data, 100, 0).await.expect("grant");
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, subject, 200, 0).await.expect("revoke");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get after revoke");
        assert_eq!(perms, ControlPerms::empty());
    }

    #[tokio::test]
    async fn control_plane_revoke_older_version_is_ignored() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };

        db.control_plane_grant(&data, 200, 0).await.expect("grant v200");
        // Revoke with older version should be ignored
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, subject, 100, 0).await.expect("revoke v100");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::READ, "older revoke should not take effect");
    }

    #[tokio::test]
    async fn control_plane_list_by_subject_and_entity() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();
        let entity1 = Uuid::new_v4();
        let entity2 = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity1,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        let d2 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity2,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::CONFIGURE,
        };

        db.control_plane_grant(&d1, 100, 0).await.expect("grant d1");
        db.control_plane_grant(&d2, 100, 1).await.expect("grant d2");

        let by_subject = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list by subject");
        assert_eq!(by_subject.len(), 2);

        let by_entity = db.control_plane_list_by_entity(org, IdKind::Endpoint, entity1).await.expect("list by entity");
        assert_eq!(by_entity.len(), 1);
        assert_eq!(by_entity[0].perms, ControlPerms::READ);
    }

    #[tokio::test]
    async fn control_plane_remove_subject_deletes_all_and_blocks_older_inserts() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();
        let entity1 = Uuid::new_v4();
        let entity2 = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity1,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        let d2 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "workflow".to_string(),
            entity_uuid: entity2,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::CONFIGURE,
        };

        db.control_plane_grant(&d1, 100, 0).await.expect("grant d1");
        db.control_plane_grant(&d2, 100, 1).await.expect("grant d2");

        // Remove subject with higher version
        db.control_plane_remove_subject(org, IdKind::User, subject, 300, 0).await.expect("remove subject");

        let by_subject = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list after remove");
        assert_eq!(by_subject.len(), 0, "all rows should be deactivated");

        // Grant with lower version should be blocked by tombstone
        db.control_plane_grant(&d1, 200, 0).await.expect("grant after remove");
        let by_subject = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list after blocked grant");
        assert_eq!(by_subject.len(), 0, "older grant should be blocked by tombstone");
    }

    #[tokio::test]
    async fn control_plane_remove_entity_deletes_all_subjects() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject1 = Uuid::new_v4();
        let subject2 = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject1,
            perms: ControlPerms::READ,
        };
        let d2 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "robot".to_string(),
            subject_uuid: subject2,
            perms: ControlPerms::CONFIGURE,
        };

        db.control_plane_grant(&d1, 100, 0).await.expect("grant d1");
        db.control_plane_grant(&d2, 100, 1).await.expect("grant d2");

        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 300, 0).await.expect("remove entity");

        let by_entity = db.control_plane_list_by_entity(org, IdKind::Endpoint, entity).await.expect("list after remove");
        assert_eq!(by_entity.len(), 0, "all subjects should be deactivated");
    }

    #[tokio::test]
    async fn control_plane_grant_upsert_updates_perms() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let mut data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };

        db.control_plane_grant(&data, 100, 0).await.expect("grant v100");

        // Upsert with higher version and different perms
        data.perms = ControlPerms::all();
        db.control_plane_grant(&data, 200, 0).await.expect("grant v200");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::all(), "should have updated perms");
    }

    #[tokio::test]
    async fn control_plane_grant_older_version_does_not_overwrite() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let mut data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };

        db.control_plane_grant(&data, 200, 0).await.expect("grant v200");

        // Try to overwrite with older version
        data.perms = ControlPerms::READ;
        db.control_plane_grant(&data, 100, 0).await.expect("grant v100");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::all(), "older grant should not overwrite");
    }

    // -----------------------------------------------------------------------
    // Data Plane tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_grant_and_verify() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ | DataPerms::WRITE,
        };

        db.data_plane_grant(&data, 100, 0).await.expect("data_plane_grant");

        // Verify subset
        let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::READ).await.expect("data_plane_verify r");
        assert!(ok);

        // Verify exact
        let ok = db
            .data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::READ | DataPerms::WRITE)
            .await
            .expect("data_plane_verify rw");
        assert!(ok);

        // Verify superset fails
        let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::EXECUTE).await.expect("data_plane_verify x");
        assert!(!ok);
    }

    #[tokio::test]
    async fn data_plane_get_returns_empty_for_unknown() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let perms = db.data_plane_get(org, Uuid::new_v4(), IdKind::User, Uuid::new_v4()).await.expect("data_plane_get unknown");
        assert_eq!(perms, DataPerms::empty());
    }

    #[tokio::test]
    async fn data_plane_revoke_soft_deletes() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };

        db.data_plane_grant(&data, 100, 0).await.expect("grant");
        db.data_plane_revoke(org, endpoint, IdKind::User, subject, 200, 0).await.expect("revoke");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get after revoke");
        assert_eq!(perms, DataPerms::empty());
    }

    #[tokio::test]
    async fn data_plane_remove_endpoint_deletes_all() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: s1,
            perms: DataPerms::READ,
        };
        let d2 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "robot".to_string(),
            subject_uuid: s2,
            perms: DataPerms::all(),
        };

        db.data_plane_grant(&d1, 100, 0).await.expect("grant d1");
        db.data_plane_grant(&d2, 100, 1).await.expect("grant d2");

        db.data_plane_remove_endpoint(org, endpoint, 300, 0).await.expect("remove endpoint");

        let p1 = db.data_plane_get(org, endpoint, IdKind::User, s1).await.expect("get s1");
        let p2 = db.data_plane_get(org, endpoint, IdKind::Robot, s2).await.expect("get s2");
        assert_eq!(p1, DataPerms::empty());
        assert_eq!(p2, DataPerms::empty());
    }

    #[tokio::test]
    async fn data_plane_remove_subject_deletes_all_endpoints() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let ep1 = Uuid::new_v4();
        let ep2 = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: ep1,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ,
        };
        let d2 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: ep2,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };

        db.data_plane_grant(&d1, 100, 0).await.expect("grant d1");
        db.data_plane_grant(&d2, 100, 1).await.expect("grant d2");

        db.data_plane_remove_subject(org, IdKind::User, subject, 300, 0).await.expect("remove subject");

        let p1 = db.data_plane_get(org, ep1, IdKind::User, subject).await.expect("get ep1");
        let p2 = db.data_plane_get(org, ep2, IdKind::User, subject).await.expect("get ep2");
        assert_eq!(p1, DataPerms::empty());
        assert_eq!(p2, DataPerms::empty());
    }

    // -----------------------------------------------------------------------
    // Version ordering / tombstone guard tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_subject_tombstone_blocks_older_grant() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Delete subject first with high version (creates tombstone)
        db.data_plane_remove_subject(org, IdKind::User, subject, 300, 0).await.expect("remove subject");

        // Grant with lower version should be blocked
        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 200, 0).await.expect("grant after tombstone");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::empty(), "tombstone should block older grant");
    }

    #[tokio::test]
    async fn data_plane_entity_tombstone_blocks_older_grant() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Delete endpoint first with high version
        db.data_plane_remove_endpoint(org, endpoint, 300, 0).await.expect("remove endpoint");

        // Grant with lower version should be blocked
        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 200, 0).await.expect("grant after tombstone");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::empty(), "entity tombstone should block older grant");
    }

    #[tokio::test]
    async fn control_plane_entity_tombstone_blocks_older_grant() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Remove entity with high version
        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 300, 0).await.expect("remove entity");

        // Grant with lower version should be blocked
        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };
        db.control_plane_grant(&data, 200, 0).await.expect("grant after entity tombstone");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::empty(), "entity tombstone should block older grant");
    }

    // -----------------------------------------------------------------------
    // PG sync event tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn upsert_event_writes_both_tables_for_endpoint() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((100, 0)),
        };

        db.upsert_rbac_pg_event(&event).await.expect("upsert event");

        // Verify rbac_control was written
        let control_plane_perms =
            db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert!(control_plane_perms.contains(ControlPerms::READ));
        assert!(control_plane_perms.contains(ControlPerms::CONFIGURE));
        assert!(control_plane_perms.contains(ControlPerms::GRANT));

        // Verify rbac_data was also written (endpoint entity)
        let data_plane_perms = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert!(data_plane_perms.contains(DataPerms::READ));
        assert!(data_plane_perms.contains(DataPerms::WRITE));
        assert!(data_plane_perms.contains(DataPerms::EXECUTE));
    }

    #[tokio::test]
    async fn upsert_event_skips_rbac_data_for_non_endpoint() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("workflow".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((100, 0)),
        };

        db.upsert_rbac_pg_event(&event).await.expect("upsert workflow");

        // rbac_control should have been written
        let control_plane_perms =
            db.control_plane_get(org, IdKind::Workflow, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert!(control_plane_perms.contains(ControlPerms::READ));

        // rbac_data should NOT have been written (workflow, not endpoint)
        let data_plane_perms = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(data_plane_perms, DataPerms::empty());
    }

    #[tokio::test]
    async fn delete_row_event_soft_deletes_both_tables() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // First insert
        let upsert = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCA".to_string()),
            data_perms: Some("rw".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&upsert).await.expect("upsert");

        // Then delete
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteRow,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: None,
            data_perms: None,
            version: Some((200, 0)),
        };
        db.delete_rbac_row_event(&delete).await.expect("delete");

        // Both tables should show empty
        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(control_plane, ControlPerms::empty());
        assert_eq!(data_plane, DataPerms::empty());
    }

    #[tokio::test]
    async fn delete_entity_event_cascades_to_rbac_data() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant two subjects
        for (s, seq) in [(s1, 0i64), (s2, 1)] {
            let ev = RbacPgSyncEvent {
                op: RbacPgSyncOp::Upsert,
                org_uuid: org,
                entity_kind: Some("endpoint".to_string()),
                entity_uuid: Some(endpoint),
                subject_kind: Some("user".to_string()),
                subject_uuid: Some(s),
                control_perms: Some("R".to_string()),
                data_perms: Some("r".to_string()),
                version: Some((100, seq)),
            };
            db.upsert_rbac_pg_event(&ev).await.expect("upsert");
        }

        // Delete the entity
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(endpoint),
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((300, 0)),
        };
        db.delete_rbac_entity_event(&delete).await.expect("entity delete");

        // Both control plane and data plane should be cleared for both subjects
        for s in [s1, s2] {
            let control_plane = db.control_plane_get(org, IdKind::Endpoint, endpoint, IdKind::User, s).await.expect("control_plane");
            let data_plane = db.data_plane_get(org, endpoint, IdKind::User, s).await.expect("data_plane");
            assert_eq!(control_plane, ControlPerms::empty());
            assert_eq!(data_plane, DataPerms::empty());
        }
    }

    #[tokio::test]
    async fn delete_subject_event_removes_from_both_tables() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let ep1 = Uuid::new_v4();
        let ep2 = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant subject on two endpoints
        for (ep, seq) in [(ep1, 0i64), (ep2, 1)] {
            let ev = RbacPgSyncEvent {
                op: RbacPgSyncOp::Upsert,
                org_uuid: org,
                entity_kind: Some("endpoint".to_string()),
                entity_uuid: Some(ep),
                subject_kind: Some("user".to_string()),
                subject_uuid: Some(subject),
                control_perms: Some("RCPGDA".to_string()),
                data_perms: Some("rwx".to_string()),
                version: Some((100, seq)),
            };
            db.upsert_rbac_pg_event(&ev).await.expect("upsert");
        }

        // Delete subject
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteSubject,
            org_uuid: org,
            entity_kind: None,
            entity_uuid: None,
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: None,
            data_perms: None,
            version: Some((300, 0)),
        };
        db.delete_rbac_subject_event(&delete).await.expect("subject delete");

        // Both endpoints should be cleared
        for ep in [ep1, ep2] {
            let control_plane = db.control_plane_get(org, IdKind::Endpoint, ep, IdKind::User, subject).await.expect("control_plane");
            let data_plane = db.data_plane_get(org, ep, IdKind::User, subject).await.expect("data_plane");
            assert_eq!(control_plane, ControlPerms::empty());
            assert_eq!(data_plane, DataPerms::empty());
        }
    }

    #[tokio::test]
    async fn version_sequence_breaks_ties() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let mut data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };

        // Same ms, lower seq
        db.control_plane_grant(&data, 100, 0).await.expect("grant seq=0");

        // Same ms, higher seq with different perms
        data.perms = ControlPerms::all();
        db.control_plane_grant(&data, 100, 1).await.expect("grant seq=1");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::all(), "higher seq should win");

        // Same ms, lower seq should NOT overwrite
        data.perms = ControlPerms::AUDIT;
        db.control_plane_grant(&data, 100, 0).await.expect("grant seq=0 again");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get after old seq");
        assert_eq!(perms, ControlPerms::all(), "lower seq should not overwrite");
    }

    // -----------------------------------------------------------------------
    // List operation edge cases
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_list_by_subject_returns_empty_for_unknown() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let result = db.control_plane_list_by_subject(org, IdKind::User, Uuid::new_v4()).await.expect("list");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn control_plane_list_by_entity_returns_empty_for_unknown() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let result = db.control_plane_list_by_entity(org, IdKind::Endpoint, Uuid::new_v4()).await.expect("list");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn control_plane_list_by_subject_excludes_soft_deleted_rows() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };

        db.control_plane_grant(&data, 100, 0).await.expect("grant");
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, subject, 200, 0).await.expect("revoke");

        let result = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list");
        assert!(result.is_empty(), "soft-deleted rows should not appear in list");
    }

    #[tokio::test]
    async fn control_plane_list_by_entity_excludes_soft_deleted_rows() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        insert_org(&db, org).await;

        for (s, seq) in [(s1, 0i64), (s2, 1)] {
            let d = ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: "endpoint".to_string(),
                entity_uuid: entity,
                subject_kind: "user".to_string(),
                subject_uuid: s,
                perms: ControlPerms::READ,
            };
            db.control_plane_grant(&d, 100, seq).await.expect("grant");
        }

        // Revoke s1 only
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, s1, 200, 0).await.expect("revoke");

        let result = db.control_plane_list_by_entity(org, IdKind::Endpoint, entity).await.expect("list");
        assert_eq!(result.len(), 1, "only active row should appear");
        assert_eq!(result[0].subject_uuid, s2);
    }

    #[tokio::test]
    async fn control_plane_list_by_subject_returns_multiple_entity_types() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();
        let endpoint_uuid = Uuid::new_v4();
        let workflow_uuid = Uuid::new_v4();
        let template_uuid = Uuid::new_v4();
        insert_org(&db, org).await;

        for (kind, uuid, seq) in [
            ("endpoint", endpoint_uuid, 0i64),
            ("workflow", workflow_uuid, 1),
            ("template", template_uuid, 2),
        ] {
            let d = ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: kind.to_string(),
                entity_uuid: uuid,
                subject_kind: "user".to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            };
            db.control_plane_grant(&d, 100, seq).await.expect("grant");
        }

        let result = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list");
        assert_eq!(result.len(), 3);

        let kinds: Vec<&str> = result.iter().map(|r| r.entity_kind.as_str()).collect();
        assert!(kinds.contains(&"endpoint"));
        assert!(kinds.contains(&"workflow"));
        assert!(kinds.contains(&"template"));
    }

    // -----------------------------------------------------------------------
    // Data plane version ordering
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_grant_older_version_does_not_overwrite() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let mut data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };

        db.data_plane_grant(&data, 200, 0).await.expect("grant v200");

        data.perms = DataPerms::READ;
        db.data_plane_grant(&data, 100, 0).await.expect("grant v100");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::all(), "older grant should not overwrite");
    }

    #[tokio::test]
    async fn data_plane_revoke_older_version_is_ignored() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };

        db.data_plane_grant(&data, 200, 0).await.expect("grant v200");
        db.data_plane_revoke(org, endpoint, IdKind::User, subject, 100, 0).await.expect("revoke v100");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::all(), "older revoke should not take effect");
    }

    // -----------------------------------------------------------------------
    // Re-grant after revoke (re-activation)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_re_grant_after_revoke_reactivates() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };

        db.control_plane_grant(&data, 100, 0).await.expect("grant");
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, subject, 200, 0).await.expect("revoke");

        // Re-grant with newer version
        let redata = ControlPlaneRbacData { perms: ControlPerms::READ | ControlPerms::CONFIGURE, ..data };
        db.control_plane_grant(&redata, 300, 0).await.expect("re-grant");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::READ | ControlPerms::CONFIGURE, "re-grant should reactivate with new perms");

        let listed = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list");
        assert_eq!(listed.len(), 1, "re-granted row should appear in list");
    }

    #[tokio::test]
    async fn data_plane_re_grant_after_revoke_reactivates() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ,
        };

        db.data_plane_grant(&data, 100, 0).await.expect("grant");
        db.data_plane_revoke(org, endpoint, IdKind::User, subject, 200, 0).await.expect("revoke");

        let redata = DataPlaneRbacData { perms: DataPerms::all(), ..data };
        db.data_plane_grant(&redata, 300, 0).await.expect("re-grant");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::all(), "re-grant should reactivate with new perms");
    }

    // -----------------------------------------------------------------------
    // data_plane_remove_subject partial — only deactivates rows with older versions
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_remove_subject_respects_version_ordering() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let ep1 = Uuid::new_v4();
        let ep2 = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant on ep1 with low version
        let d1 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: ep1,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ,
        };
        db.data_plane_grant(&d1, 100, 0).await.expect("grant ep1");

        // Grant on ep2 with HIGH version (higher than the remove)
        let d2 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: ep2,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&d2, 400, 0).await.expect("grant ep2");

        // Remove subject with version between the two grants
        db.data_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("remove subject");

        // ep1 (v100) should be deactivated by remove (v200)
        let p1 = db.data_plane_get(org, ep1, IdKind::User, subject).await.expect("get ep1");
        assert_eq!(p1, DataPerms::empty(), "lower-version row should be deactivated");

        // ep2 (v400) should survive the remove (v200) — version guard protects it
        let p2 = db.data_plane_get(org, ep2, IdKind::User, subject).await.expect("get ep2");
        assert_eq!(p2, DataPerms::all(), "higher-version row should survive remove");
    }

    #[tokio::test]
    async fn control_plane_remove_subject_respects_version_ordering() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let e1 = Uuid::new_v4();
        let e2 = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: e1,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&d1, 100, 0).await.expect("grant e1");

        let d2 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: e2,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };
        db.control_plane_grant(&d2, 400, 0).await.expect("grant e2");

        db.control_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("remove subject");

        let p1 = db.control_plane_get(org, IdKind::Endpoint, e1, IdKind::User, subject).await.expect("get e1");
        assert_eq!(p1, ControlPerms::empty(), "lower-version row should be deactivated");

        let p2 = db.control_plane_get(org, IdKind::Endpoint, e2, IdKind::User, subject).await.expect("get e2");
        assert_eq!(p2, ControlPerms::all(), "higher-version row should survive remove");
    }

    // -----------------------------------------------------------------------
    // Cache graceful degradation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_operations_succeed_with_local_cache_handle() {
        // The test_db() uses MockRedisConnection(true). The flag is retained for
        // compatibility, but internal cache access is always local.
        // This test verifies the full data plane lifecycle works via PG fallback alone.
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ | DataPerms::WRITE,
        };

        // All data plane operations should succeed with the local cache handle.
        db.data_plane_grant(&data, 100, 0).await.expect("grant with local cache");
        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get falls back to PG");
        assert_eq!(perms, DataPerms::READ | DataPerms::WRITE);

        let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::READ).await.expect("verify falls back to PG");
        assert!(ok);

        db.data_plane_revoke(org, endpoint, IdKind::User, subject, 200, 0).await.expect("revoke with local cache");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get after revoke");
        assert_eq!(perms, DataPerms::empty());

        // Re-grant for remove tests
        db.data_plane_grant(&data, 300, 0).await.expect("re-grant");
        db.data_plane_remove_endpoint(org, endpoint, 400, 0).await.expect("remove endpoint with local cache");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get after endpoint remove");
        assert_eq!(perms, DataPerms::empty());
    }

    // -----------------------------------------------------------------------
    // Mixed subject kinds
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_list_by_entity_returns_mixed_subject_kinds() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let user = Uuid::new_v4();
        let robot = Uuid::new_v4();
        insert_org(&db, org).await;

        let d_user = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: user,
            perms: ControlPerms::READ,
        };
        let d_robot = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "robot".to_string(),
            subject_uuid: robot,
            perms: ControlPerms::CONFIGURE,
        };

        db.control_plane_grant(&d_user, 100, 0).await.expect("grant user");
        db.control_plane_grant(&d_robot, 100, 1).await.expect("grant robot");

        let result = db.control_plane_list_by_entity(org, IdKind::Endpoint, entity).await.expect("list");
        assert_eq!(result.len(), 2);

        let kinds: Vec<&str> = result.iter().map(|r| r.subject_kind.as_str()).collect();
        assert!(kinds.contains(&"user"));
        assert!(kinds.contains(&"robot"));
    }

    #[tokio::test]
    async fn data_plane_remove_endpoint_blocks_older_grant_via_tombstone() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Remove endpoint first with high version
        db.data_plane_remove_endpoint(org, endpoint, 300, 0).await.expect("remove endpoint");

        // Grant with lower version should be blocked by entity tombstone
        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 200, 0).await.expect("grant after tombstone");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::empty(), "endpoint tombstone should block older grant");
    }

    #[tokio::test]
    async fn data_plane_remove_subject_blocks_older_grant_via_tombstone() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Remove subject first with high version
        db.data_plane_remove_subject(org, IdKind::User, subject, 300, 0).await.expect("remove subject");

        // Grant with lower version should be blocked
        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 200, 0).await.expect("grant after subject tombstone");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::empty(), "subject tombstone should block older grant");
    }

    // -----------------------------------------------------------------------
    // PG sync version ordering
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn delete_row_event_older_version_does_not_deactivate() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Upsert with high version
        let upsert = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((200, 0)),
        };
        db.upsert_rbac_pg_event(&upsert).await.expect("upsert v200");

        // Delete with lower version should be ignored
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteRow,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };
        db.delete_rbac_row_event(&delete).await.expect("delete v100");

        // Should still be active
        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane");
        assert!(control_plane.contains(ControlPerms::READ), "older delete should not deactivate");
        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane");
        assert!(data_plane.contains(DataPerms::READ), "older delete should not deactivate data plane");
    }

    #[tokio::test]
    async fn upsert_event_after_higher_version_entity_delete_is_blocked() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Entity delete with high version
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((300, 0)),
        };
        db.delete_rbac_entity_event(&delete).await.expect("entity delete v300");

        // Upsert with lower version should be blocked by tombstone
        let upsert = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGDA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((200, 0)),
        };
        db.upsert_rbac_pg_event(&upsert).await.expect("upsert v200");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane");
        assert_eq!(control_plane, ControlPerms::empty(), "entity tombstone should block older upsert");
        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane");
        assert_eq!(data_plane, DataPerms::empty(), "entity tombstone should block older data plane upsert");
    }

    #[tokio::test]
    async fn upsert_event_after_higher_version_subject_delete_is_blocked() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Subject delete with high version
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteSubject,
            org_uuid: org,
            entity_kind: None,
            entity_uuid: None,
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: None,
            data_perms: None,
            version: Some((300, 0)),
        };
        db.delete_rbac_subject_event(&delete).await.expect("subject delete v300");

        // Upsert with lower version
        let upsert = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCA".to_string()),
            data_perms: Some("rw".to_string()),
            version: Some((200, 0)),
        };
        db.upsert_rbac_pg_event(&upsert).await.expect("upsert v200");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane");
        assert_eq!(control_plane, ControlPerms::empty(), "subject tombstone should block older upsert");
    }

    // -----------------------------------------------------------------------
    // Individual permission bit tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_verify_each_individual_bit() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant all individual bits
        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant all");

        // Verify each individual bit
        for (bit, name) in [
            (ControlPerms::READ, "READ"),
            (ControlPerms::CONFIGURE, "CONFIGURE"),
            (ControlPerms::PROMOTE, "PROMOTE"),
            (ControlPerms::GRANT, "GRANT"),
            (ControlPerms::DESTROY, "DESTROY"),
            (ControlPerms::AUDIT, "AUDIT"),
        ] {
            let ok = db
                .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, bit)
                .await
                .unwrap_or_else(|_| panic!("verify {name}"));
            assert!(ok, "{name} should be present");
        }

        let stored = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get all");
        assert_eq!(stored, ControlPerms::all());
    }

    #[tokio::test]
    async fn control_plane_grant_single_bits_and_verify_isolation() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant only PROMOTE | DESTROY
        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::PROMOTE | ControlPerms::DESTROY,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant PD");

        let stored = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(stored, ControlPerms::PROMOTE | ControlPerms::DESTROY);

        // Bits NOT granted should fail verification
        for (bit, name) in [
            (ControlPerms::READ, "READ"),
            (ControlPerms::CONFIGURE, "CONFIGURE"),
            (ControlPerms::GRANT, "GRANT"),
            (ControlPerms::AUDIT, "AUDIT"),
        ] {
            let ok = db
                .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, bit)
                .await
                .unwrap_or_else(|_| panic!("verify {name}"));
            assert!(!ok, "{name} should NOT be present");
        }

        // Granted bits should pass
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, ControlPerms::PROMOTE)
            .await
            .expect("verify PROMOTE");
        assert!(ok);
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, ControlPerms::DESTROY)
            .await
            .expect("verify DESTROY");
        assert!(ok);
    }

    #[tokio::test]
    async fn data_plane_verify_each_individual_bit() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant all");

        for (bit, name) in [
            (DataPerms::READ, "READ"),
            (DataPerms::WRITE, "WRITE"),
            (DataPerms::EXECUTE, "EXECUTE"),
        ] {
            let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, bit).await.unwrap_or_else(|_| panic!("verify {name}"));
            assert!(ok, "{name} should be present");
        }
    }

    #[tokio::test]
    async fn data_plane_grant_single_bits_and_verify_isolation() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant WRITE only
        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::WRITE,
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant WRITE");

        let stored = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(stored, DataPerms::WRITE);

        let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::READ).await.expect("verify READ");
        assert!(!ok, "READ should NOT be present");
        let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::EXECUTE).await.expect("verify EXECUTE");
        assert!(!ok, "EXECUTE should NOT be present");
        let ok = db.data_plane_verify(org, endpoint, IdKind::User, subject, DataPerms::WRITE).await.expect("verify WRITE");
        assert!(ok, "WRITE should be present");
    }

    // -----------------------------------------------------------------------
    // Cross-org isolation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_perms_are_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        let data = ControlPlaneRbacData {
            org_uuid: org1,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant org1");

        // Same entity+subject but different org should have no perms
        let perms = db.control_plane_get(org2, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org2");
        assert_eq!(perms, ControlPerms::empty(), "perms should not leak across orgs");
    }

    #[tokio::test]
    async fn data_plane_perms_are_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        let data = DataPlaneRbacData {
            org_uuid: org1,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant org1");

        let perms = db.data_plane_get(org2, endpoint, IdKind::User, subject).await.expect("get org2");
        assert_eq!(perms, DataPerms::empty(), "perms should not leak across orgs");
    }

    // -----------------------------------------------------------------------
    // All entity kinds
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_supports_all_entity_kinds() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let entity_kinds = [
            IdKind::Organization,
            IdKind::Endpoint,
            IdKind::Workflow,
            IdKind::Template,
            IdKind::Api,
            IdKind::Robot,
            IdKind::EdenNode,
        ];
        for (i, kind) in entity_kinds.iter().enumerate() {
            let entity = Uuid::new_v4();
            let data = ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: kind.as_str().to_string(),
                entity_uuid: entity,
                subject_kind: "user".to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            };
            db.control_plane_grant(&data, 100, i as i64).await.unwrap_or_else(|_| panic!("grant {kind}"));

            let perms = db.control_plane_get(org, *kind, entity, IdKind::User, subject).await.unwrap_or_else(|_| panic!("get {kind}"));
            assert_eq!(perms, ControlPerms::READ, "should work for entity kind '{kind}'");
        }

        // Verify all 7 show up in list_by_subject
        let all = db.control_plane_list_by_subject(org, IdKind::User, subject).await.expect("list all kinds");
        assert_eq!(all.len(), 7, "all entity kinds should be listed");
    }

    // -----------------------------------------------------------------------
    // Empty permission-set conversion
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn upsert_event_with_empty_permission_sets_grants_empty_perms() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some(String::new()),
            data_perms: Some(String::new()),
            version: Some((100, 0)),
        };

        db.upsert_rbac_pg_event(&event).await.expect("upsert none");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert_eq!(control_plane, ControlPerms::empty(), "none should map to empty control plane perms");

        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(data_plane, DataPerms::empty(), "none should map to empty data plane perms");
    }

    // -----------------------------------------------------------------------
    // Representative permission bitsets produce correct control/data plane mappings
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn upsert_event_read_perms_produce_correct_bits() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("R".to_string()),
            data_perms: Some("r".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&event).await.expect("upsert read");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert_eq!(control_plane, ControlPerms::READ);

        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(data_plane, DataPerms::READ);
    }

    #[tokio::test]
    async fn upsert_event_write_perms_produce_correct_bits() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCA".to_string()),
            data_perms: Some("rw".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&event).await.expect("upsert write");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert_eq!(control_plane, ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::AUDIT);

        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(data_plane, DataPerms::READ | DataPerms::WRITE);
    }

    #[tokio::test]
    async fn upsert_event_admin_perms_produce_correct_bits() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&event).await.expect("upsert admin");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert_eq!(
            control_plane,
            ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::PROMOTE | ControlPerms::GRANT | ControlPerms::AUDIT
        );

        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(data_plane, DataPerms::all());
    }

    #[tokio::test]
    async fn upsert_event_full_perms_produce_correct_bits() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGDA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&event).await.expect("upsert super_admin");

        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane_get");
        assert_eq!(control_plane, ControlPerms::all());

        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane_get");
        assert_eq!(data_plane, DataPerms::all());
    }

    // -----------------------------------------------------------------------
    // Error handling: invalid inputs
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_grant_invalid_entity_kind_returns_error() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "invalid_kind".to_string(),
            entity_uuid: Uuid::new_v4(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: ControlPerms::READ,
        };

        let result = db.control_plane_grant(&data, 100, 0).await;
        assert!(result.is_err(), "invalid entity_kind should fail CHECK constraint");
    }

    #[tokio::test]
    async fn control_plane_grant_invalid_subject_kind_returns_error() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: Uuid::new_v4(),
            subject_kind: "service_account".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: ControlPerms::READ,
        };

        let result = db.control_plane_grant(&data, 100, 0).await;
        assert!(result.is_err(), "invalid subject_kind should fail CHECK constraint");
    }

    #[tokio::test]
    async fn control_plane_grant_nonexistent_org_returns_error() {
        let db = test_db().await;
        // Deliberately NOT inserting org
        let org = Uuid::new_v4();

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: Uuid::new_v4(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: ControlPerms::READ,
        };

        let result = db.control_plane_grant(&data, 100, 0).await;
        assert!(result.is_err(), "nonexistent org should fail FK constraint");
    }

    #[tokio::test]
    async fn data_plane_grant_nonexistent_org_returns_error() {
        let db = test_db().await;
        let org = Uuid::new_v4();

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: Uuid::new_v4(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: DataPerms::READ,
        };

        let result = db.data_plane_grant(&data, 100, 0).await;
        assert!(result.is_err(), "nonexistent org should fail FK constraint");
    }

    #[tokio::test]
    async fn upsert_event_invalid_control_perms_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: Some("manager".to_string()),
            data_perms: Some("manager".to_string()),
            version: Some((100, 0)),
        };

        let result = db.upsert_rbac_pg_event(&event).await;
        assert!(result.is_err(), "unknown control_perms should fail parsing");
    }

    #[tokio::test]
    async fn upsert_event_missing_entity_kind_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: Uuid::new_v4(),
            entity_kind: None,
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: Some("R".to_string()),
            data_perms: Some("r".to_string()),
            version: Some((100, 0)),
        };

        let result = db.upsert_rbac_pg_event(&event).await;
        assert!(result.is_err(), "missing entity_kind should fail");
    }

    #[tokio::test]
    async fn upsert_event_missing_subject_kind_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: Uuid::new_v4(),
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: None,
            subject_uuid: None,
            control_perms: Some("R".to_string()),
            data_perms: Some("r".to_string()),
            version: Some((100, 0)),
        };

        let result = db.upsert_rbac_pg_event(&event).await;
        assert!(result.is_err(), "missing subject_kind should fail");
    }

    #[tokio::test]
    async fn upsert_event_missing_version_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: Uuid::new_v4(),
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: Some("R".to_string()),
            data_perms: Some("r".to_string()),
            version: None,
        };

        let result = db.upsert_rbac_pg_event(&event).await;
        assert!(result.is_err(), "missing version should fail");
    }

    #[tokio::test]
    async fn upsert_event_missing_control_perms_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: Uuid::new_v4(),
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        let result = db.upsert_rbac_pg_event(&event).await;
        assert!(result.is_err(), "missing control_perms should fail for upsert");
    }

    #[tokio::test]
    async fn delete_row_event_missing_entity_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteRow,
            org_uuid: Uuid::new_v4(),
            entity_kind: None,
            entity_uuid: None,
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        let result = db.delete_rbac_row_event(&event).await;
        assert!(result.is_err(), "missing entity should fail for row delete");
    }

    #[tokio::test]
    async fn delete_subject_event_missing_subject_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteSubject,
            org_uuid: Uuid::new_v4(),
            entity_kind: None,
            entity_uuid: None,
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        let result = db.delete_rbac_subject_event(&event).await;
        assert!(result.is_err(), "missing subject should fail for subject delete");
    }

    #[tokio::test]
    async fn delete_entity_event_missing_entity_returns_error() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: Uuid::new_v4(),
            entity_kind: None,
            entity_uuid: None,
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        let result = db.delete_rbac_entity_event(&event).await;
        assert!(result.is_err(), "missing entity should fail for entity delete");
    }

    // -----------------------------------------------------------------------
    // Edge cases: empty permissions
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_verify_empty_required_always_passes() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        // Subject with NO permissions at all (not even in the table)
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, Uuid::new_v4(), IdKind::User, Uuid::new_v4(), ControlPerms::empty())
            .await
            .expect("verify empty");
        // bitflags: empty.contains(empty) == true
        assert!(ok, "empty required perms should always pass (contains semantics)");
    }

    #[tokio::test]
    async fn data_plane_verify_empty_required_always_passes() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ok = db.data_plane_verify(org, Uuid::new_v4(), IdKind::User, Uuid::new_v4(), DataPerms::empty()).await.expect("verify empty");
        assert!(ok, "empty required perms should always pass (contains semantics)");
    }

    #[tokio::test]
    async fn control_plane_grant_empty_perms_and_verify() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::empty(),
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant empty");

        // Should have a row but with empty perms
        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::empty());

        // Verify with READ should fail — the row exists but has no bits
        let ok = db
            .control_plane_verify(org, IdKind::Endpoint, entity, IdKind::User, subject, ControlPerms::READ)
            .await
            .expect("verify READ");
        assert!(!ok, "empty perms row should not satisfy READ requirement");
    }

    // -----------------------------------------------------------------------
    // Idempotency and missing-data scenarios
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_revoke_nonexistent_row_succeeds_silently() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        // Revoking a row that was never granted should not error
        db.control_plane_revoke(org, IdKind::Endpoint, Uuid::new_v4(), IdKind::User, Uuid::new_v4(), 100, 0)
            .await
            .expect("revoke nonexistent should succeed");
    }

    #[tokio::test]
    async fn data_plane_revoke_nonexistent_row_succeeds_silently() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        db.data_plane_revoke(org, Uuid::new_v4(), IdKind::User, Uuid::new_v4(), 100, 0)
            .await
            .expect("revoke nonexistent should succeed");
    }

    #[tokio::test]
    async fn control_plane_remove_subject_with_no_perms_succeeds_silently() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        db.control_plane_remove_subject(org, IdKind::User, Uuid::new_v4(), 100, 0)
            .await
            .expect("remove subject with no perms should succeed");
    }

    #[tokio::test]
    async fn control_plane_remove_entity_with_no_subjects_succeeds_silently() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        db.control_plane_remove_entity(org, IdKind::Endpoint, Uuid::new_v4(), 100, 0)
            .await
            .expect("remove entity with no subjects should succeed");
    }

    #[tokio::test]
    async fn data_plane_remove_endpoint_with_no_subjects_succeeds_silently() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        db.data_plane_remove_endpoint(org, Uuid::new_v4(), 100, 0)
            .await
            .expect("remove endpoint with no subjects should succeed");
    }

    #[tokio::test]
    async fn data_plane_remove_subject_with_no_endpoints_succeeds_silently() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        db.data_plane_remove_subject(org, IdKind::User, Uuid::new_v4(), 100, 0)
            .await
            .expect("remove subject with no endpoints should succeed");
    }

    #[tokio::test]
    async fn control_plane_grant_duplicate_same_version_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ | ControlPerms::CONFIGURE,
        };

        // Grant twice with same version — should not error
        db.control_plane_grant(&data, 100, 0).await.expect("first grant");
        db.control_plane_grant(&data, 100, 0).await.expect("duplicate grant");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::READ | ControlPerms::CONFIGURE);
    }

    #[tokio::test]
    async fn data_plane_grant_duplicate_same_version_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ | DataPerms::WRITE,
        };

        db.data_plane_grant(&data, 100, 0).await.expect("first grant");
        db.data_plane_grant(&data, 100, 0).await.expect("duplicate grant");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::READ | DataPerms::WRITE);
    }

    #[tokio::test]
    async fn control_plane_double_revoke_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant");

        // Revoke twice with same version
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, subject, 200, 0).await.expect("first revoke");
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, subject, 200, 0)
            .await
            .expect("second revoke should not error");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::empty());
    }

    #[tokio::test]
    async fn control_plane_double_remove_subject_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: Uuid::new_v4(),
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant");

        // Remove subject twice with same version — tombstone upsert should be idempotent
        db.control_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("first remove");
        db.control_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("second remove should not error");
    }

    #[tokio::test]
    async fn control_plane_double_remove_entity_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant");

        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 200, 0).await.expect("first remove");
        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 200, 0).await.expect("second remove should not error");
    }

    #[tokio::test]
    async fn delete_row_event_on_nonexistent_row_succeeds_silently() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteRow,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        db.delete_rbac_row_event(&event).await.expect("delete nonexistent row should succeed");
    }

    #[tokio::test]
    async fn delete_entity_event_on_nonexistent_entity_succeeds_silently() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(Uuid::new_v4()),
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        db.delete_rbac_entity_event(&event).await.expect("delete nonexistent entity should succeed");
    }

    #[tokio::test]
    async fn delete_subject_event_on_nonexistent_subject_succeeds_silently() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let event = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteSubject,
            org_uuid: org,
            entity_kind: None,
            entity_uuid: None,
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(Uuid::new_v4()),
            control_perms: None,
            data_perms: None,
            version: Some((100, 0)),
        };

        db.delete_rbac_subject_event(&event).await.expect("delete nonexistent subject should succeed");
    }

    // -----------------------------------------------------------------------
    // Tombstone allows newer-version re-provisioning
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_grant_after_entity_tombstone_succeeds_with_higher_version() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Create entity tombstone at v200
        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 200, 0).await.expect("remove entity");

        // Grant with HIGHER version (v300) should succeed despite tombstone
        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 300, 0).await.expect("grant after tombstone with higher version");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::READ, "newer grant should succeed after tombstone");
    }

    #[tokio::test]
    async fn control_plane_grant_after_subject_tombstone_succeeds_with_higher_version() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        db.control_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("remove subject");

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::CONFIGURE,
        };
        db.control_plane_grant(&data, 300, 0).await.expect("grant after subject tombstone");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::CONFIGURE, "newer grant should succeed after subject tombstone");
    }

    #[tokio::test]
    async fn data_plane_grant_after_entity_tombstone_succeeds_with_higher_version() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        db.data_plane_remove_endpoint(org, endpoint, 200, 0).await.expect("remove endpoint");

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ,
        };
        db.data_plane_grant(&data, 300, 0).await.expect("grant after endpoint tombstone");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::READ, "newer grant should succeed after endpoint tombstone");
    }

    #[tokio::test]
    async fn data_plane_grant_after_subject_tombstone_succeeds_with_higher_version() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        db.data_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("remove subject");

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::WRITE,
        };
        db.data_plane_grant(&data, 300, 0).await.expect("grant after subject tombstone");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::WRITE, "newer grant should succeed after subject tombstone");
    }

    // -----------------------------------------------------------------------
    // Upsert replaces perms (not merges)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_upsert_replaces_perms_not_merges() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Grant READ | CONFIGURE
        let mut data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ | ControlPerms::CONFIGURE,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant RC");

        // Upsert with AUDIT only (higher version) — should REPLACE, not merge
        data.perms = ControlPerms::AUDIT;
        db.control_plane_grant(&data, 200, 0).await.expect("grant A");

        let perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, ControlPerms::AUDIT, "upsert should replace, not merge — READ and CONFIGURE should be gone");
        assert!(!perms.contains(ControlPerms::READ));
        assert!(!perms.contains(ControlPerms::CONFIGURE));
    }

    #[tokio::test]
    async fn data_plane_upsert_replaces_perms_not_merges() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let mut data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ | DataPerms::WRITE,
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant rw");

        // Upsert with EXECUTE only
        data.perms = DataPerms::EXECUTE;
        db.data_plane_grant(&data, 200, 0).await.expect("grant x");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::EXECUTE, "upsert should replace — READ and WRITE should be gone");
        assert!(!perms.contains(DataPerms::READ));
        assert!(!perms.contains(DataPerms::WRITE));
    }

    // -----------------------------------------------------------------------
    // Subject kind isolation (same UUID, different kinds = distinct rows)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_same_uuid_different_subject_kind_are_distinct() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let shared_uuid = Uuid::new_v4(); // same UUID used as both user and robot
        insert_org(&db, org).await;

        let user_data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: shared_uuid,
            perms: ControlPerms::READ,
        };
        let robot_data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "robot".to_string(),
            subject_uuid: shared_uuid,
            perms: ControlPerms::all(),
        };

        db.control_plane_grant(&user_data, 100, 0).await.expect("grant user");
        db.control_plane_grant(&robot_data, 100, 1).await.expect("grant robot");

        let user_perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, shared_uuid).await.expect("get user");
        let robot_perms = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::Robot, shared_uuid).await.expect("get robot");

        assert_eq!(user_perms, ControlPerms::READ, "user should have READ only");
        assert_eq!(robot_perms, ControlPerms::all(), "robot should have all");

        // Revoking user should not affect robot
        db.control_plane_revoke(org, IdKind::Endpoint, entity, IdKind::User, shared_uuid, 200, 0).await.expect("revoke user");
        let robot_perms = db
            .control_plane_get(org, IdKind::Endpoint, entity, IdKind::Robot, shared_uuid)
            .await
            .expect("get robot after user revoke");
        assert_eq!(robot_perms, ControlPerms::all(), "robot should be unaffected by user revoke");
    }

    // -----------------------------------------------------------------------
    // Entity kind isolation (same UUID, different kinds = distinct rows)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_same_uuid_different_entity_kind_are_distinct() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let shared_uuid = Uuid::new_v4(); // same UUID used as both endpoint and workflow
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let endpoint_data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: shared_uuid,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        let workflow_data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "workflow".to_string(),
            entity_uuid: shared_uuid,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::all(),
        };

        db.control_plane_grant(&endpoint_data, 100, 0).await.expect("grant endpoint");
        db.control_plane_grant(&workflow_data, 100, 1).await.expect("grant workflow");

        let ep_perms = db.control_plane_get(org, IdKind::Endpoint, shared_uuid, IdKind::User, subject).await.expect("get endpoint");
        let wf_perms = db.control_plane_get(org, IdKind::Workflow, shared_uuid, IdKind::User, subject).await.expect("get workflow");

        assert_eq!(ep_perms, ControlPerms::READ);
        assert_eq!(wf_perms, ControlPerms::all());

        // Removing entity kind "endpoint" should not affect "workflow"
        db.control_plane_remove_entity(org, IdKind::Endpoint, shared_uuid, 200, 0).await.expect("remove endpoint entity");
        let wf_perms = db
            .control_plane_get(org, IdKind::Workflow, shared_uuid, IdKind::User, subject)
            .await
            .expect("get workflow after endpoint remove");
        assert_eq!(wf_perms, ControlPerms::all(), "workflow should be unaffected by endpoint entity remove");
    }

    // -----------------------------------------------------------------------
    // Cross-org isolation in bulk operations
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_remove_subject_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        // Grant same subject in both orgs
        for (org, seq) in [(org1, 0i64), (org2, 1)] {
            let data = ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: "endpoint".to_string(),
                entity_uuid: entity,
                subject_kind: "user".to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            };
            db.control_plane_grant(&data, 100, seq).await.expect("grant");
        }

        // Remove subject in org1 only
        db.control_plane_remove_subject(org1, IdKind::User, subject, 300, 0).await.expect("remove subject in org1");

        // org1 should be cleared
        let p1 = db.control_plane_get(org1, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org1");
        assert_eq!(p1, ControlPerms::empty());

        // org2 should be untouched
        let p2 = db.control_plane_get(org2, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org2");
        assert_eq!(p2, ControlPerms::READ, "remove_subject in org1 should not affect org2");
    }

    #[tokio::test]
    async fn control_plane_subject_tombstone_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        // Create subject tombstone in org1 at v200
        db.control_plane_remove_subject(org1, IdKind::User, subject, 200, 0).await.expect("remove subject in org1");

        // Grant in org2 with v100 should succeed — org1's tombstone shouldn't block it
        let data = ControlPlaneRbacData {
            org_uuid: org2,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant in org2");

        let perms = db.control_plane_get(org2, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org2");
        assert_eq!(perms, ControlPerms::READ, "org1 tombstone should not block org2 grant");
    }

    #[tokio::test]
    async fn data_plane_remove_subject_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        for (org, seq) in [(org1, 0i64), (org2, 1)] {
            let data = DataPlaneRbacData {
                org_uuid: org,
                endpoint_uuid: endpoint,
                subject_kind: "user".to_string(),
                subject_uuid: subject,
                perms: DataPerms::all(),
            };
            db.data_plane_grant(&data, 100, seq).await.expect("grant");
        }

        db.data_plane_remove_subject(org1, IdKind::User, subject, 300, 0).await.expect("remove subject in org1");

        let p1 = db.data_plane_get(org1, endpoint, IdKind::User, subject).await.expect("get org1");
        assert_eq!(p1, DataPerms::empty());

        let p2 = db.data_plane_get(org2, endpoint, IdKind::User, subject).await.expect("get org2");
        assert_eq!(p2, DataPerms::all(), "remove_subject in org1 should not affect org2");
    }

    // -----------------------------------------------------------------------
    // remove_entity version ordering (higher-version rows survive)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_remove_entity_respects_version_ordering() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        insert_org(&db, org).await;

        // s1 granted at low version
        let d1 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: s1,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&d1, 100, 0).await.expect("grant s1");

        // s2 granted at HIGH version (higher than the remove)
        let d2 = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: s2,
            perms: ControlPerms::all(),
        };
        db.control_plane_grant(&d2, 400, 0).await.expect("grant s2");

        // Remove entity at version between the two
        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 200, 0).await.expect("remove entity");

        let p1 = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, s1).await.expect("get s1");
        assert_eq!(p1, ControlPerms::empty(), "lower-version row should be deactivated");

        let p2 = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, s2).await.expect("get s2");
        assert_eq!(p2, ControlPerms::all(), "higher-version row should survive entity remove");
    }

    #[tokio::test]
    async fn data_plane_remove_endpoint_respects_version_ordering() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        insert_org(&db, org).await;

        let d1 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: s1,
            perms: DataPerms::READ,
        };
        db.data_plane_grant(&d1, 100, 0).await.expect("grant s1");

        let d2 = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: s2,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&d2, 400, 0).await.expect("grant s2");

        db.data_plane_remove_endpoint(org, endpoint, 200, 0).await.expect("remove endpoint");

        let p1 = db.data_plane_get(org, endpoint, IdKind::User, s1).await.expect("get s1");
        assert_eq!(p1, DataPerms::empty(), "lower-version row should be deactivated");

        let p2 = db.data_plane_get(org, endpoint, IdKind::User, s2).await.expect("get s2");
        assert_eq!(p2, DataPerms::all(), "higher-version row should survive endpoint remove");
    }

    // -----------------------------------------------------------------------
    // Data plane revoke and remove idempotency
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_double_revoke_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ,
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant");

        db.data_plane_revoke(org, endpoint, IdKind::User, subject, 200, 0).await.expect("first revoke");
        db.data_plane_revoke(org, endpoint, IdKind::User, subject, 200, 0).await.expect("second revoke should not error");

        let perms = db.data_plane_get(org, endpoint, IdKind::User, subject).await.expect("get");
        assert_eq!(perms, DataPerms::empty());
    }

    #[tokio::test]
    async fn data_plane_double_remove_endpoint_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: DataPerms::READ,
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant");

        db.data_plane_remove_endpoint(org, endpoint, 200, 0).await.expect("first remove");
        db.data_plane_remove_endpoint(org, endpoint, 200, 0).await.expect("second remove should not error");
    }

    #[tokio::test]
    async fn data_plane_double_remove_subject_is_idempotent() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: Uuid::new_v4(),
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::all(),
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant");

        db.data_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("first remove");
        db.data_plane_remove_subject(org, IdKind::User, subject, 200, 0).await.expect("second remove should not error");
    }

    // -----------------------------------------------------------------------
    // Re-grant after revoke shows up in lists
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_re_grant_after_remove_entity_with_higher_version_shows_in_list() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = ControlPlaneRbacData {
            org_uuid: org,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant");
        db.control_plane_remove_entity(org, IdKind::Endpoint, entity, 200, 0).await.expect("remove entity");

        // Verify gone from list
        let list = db.control_plane_list_by_entity(org, IdKind::Endpoint, entity).await.expect("list after remove");
        assert!(list.is_empty());

        // Re-grant with higher version
        let redata = ControlPlaneRbacData {
            perms: ControlPerms::CONFIGURE | ControlPerms::PROMOTE,
            ..data
        };
        db.control_plane_grant(&redata, 300, 0).await.expect("re-grant");

        let list = db.control_plane_list_by_entity(org, IdKind::Endpoint, entity).await.expect("list after re-grant");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].perms, ControlPerms::CONFIGURE | ControlPerms::PROMOTE);
    }

    // -----------------------------------------------------------------------
    // Cross-org isolation for entity operations
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_remove_entity_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        for (org, seq) in [(org1, 0i64), (org2, 1)] {
            let data = ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: "endpoint".to_string(),
                entity_uuid: entity,
                subject_kind: "user".to_string(),
                subject_uuid: subject,
                perms: ControlPerms::READ,
            };
            db.control_plane_grant(&data, 100, seq).await.expect("grant");
        }

        db.control_plane_remove_entity(org1, IdKind::Endpoint, entity, 300, 0).await.expect("remove entity in org1");

        let p1 = db.control_plane_get(org1, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org1");
        assert_eq!(p1, ControlPerms::empty());

        let p2 = db.control_plane_get(org2, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org2");
        assert_eq!(p2, ControlPerms::READ, "remove_entity in org1 should not affect org2");
    }

    #[tokio::test]
    async fn data_plane_remove_endpoint_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        for (org, seq) in [(org1, 0i64), (org2, 1)] {
            let data = DataPlaneRbacData {
                org_uuid: org,
                endpoint_uuid: endpoint,
                subject_kind: "user".to_string(),
                subject_uuid: subject,
                perms: DataPerms::all(),
            };
            db.data_plane_grant(&data, 100, seq).await.expect("grant");
        }

        db.data_plane_remove_endpoint(org1, endpoint, 300, 0).await.expect("remove endpoint in org1");

        let p1 = db.data_plane_get(org1, endpoint, IdKind::User, subject).await.expect("get org1");
        assert_eq!(p1, DataPerms::empty());

        let p2 = db.data_plane_get(org2, endpoint, IdKind::User, subject).await.expect("get org2");
        assert_eq!(p2, DataPerms::all(), "remove_endpoint in org1 should not affect org2");
    }

    // -----------------------------------------------------------------------
    // Data plane invalid subject_kind
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_grant_invalid_subject_kind_returns_error() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: Uuid::new_v4(),
            subject_kind: "service_account".to_string(),
            subject_uuid: Uuid::new_v4(),
            perms: DataPerms::READ,
        };

        let result = db.data_plane_grant(&data, 100, 0).await;
        assert!(result.is_err(), "invalid subject_kind should fail CHECK constraint");
    }

    // -----------------------------------------------------------------------
    // Data plane subject kind isolation (same UUID, different kinds)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_same_uuid_different_subject_kind_are_distinct() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let shared_uuid = Uuid::new_v4();
        insert_org(&db, org).await;

        let user_data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: shared_uuid,
            perms: DataPerms::READ,
        };
        let robot_data = DataPlaneRbacData {
            org_uuid: org,
            endpoint_uuid: endpoint,
            subject_kind: "robot".to_string(),
            subject_uuid: shared_uuid,
            perms: DataPerms::all(),
        };

        db.data_plane_grant(&user_data, 100, 0).await.expect("grant user");
        db.data_plane_grant(&robot_data, 100, 1).await.expect("grant robot");

        let user_perms = db.data_plane_get(org, endpoint, IdKind::User, shared_uuid).await.expect("get user");
        let robot_perms = db.data_plane_get(org, endpoint, IdKind::Robot, shared_uuid).await.expect("get robot");

        assert_eq!(user_perms, DataPerms::READ, "user should have READ only");
        assert_eq!(robot_perms, DataPerms::all(), "robot should have all");

        // Revoking user should not affect robot
        db.data_plane_revoke(org, endpoint, IdKind::User, shared_uuid, 200, 0).await.expect("revoke user");
        let robot_perms = db.data_plane_get(org, endpoint, IdKind::Robot, shared_uuid).await.expect("get robot after user revoke");
        assert_eq!(robot_perms, DataPerms::all(), "robot should be unaffected by user revoke");
    }

    // -----------------------------------------------------------------------
    // Non-endpoint entity skips data plane in delete paths
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn delete_row_event_non_endpoint_skips_rbac_data() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Upsert a workflow (writes to rbac_control only)
        let upsert = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("workflow".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&upsert).await.expect("upsert workflow");

        // Delete the workflow row — should only touch rbac_control, not rbac_data
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteRow,
            org_uuid: org,
            entity_kind: Some("workflow".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: None,
            data_perms: None,
            version: Some((200, 0)),
        };
        db.delete_rbac_row_event(&delete).await.expect("delete workflow row");

        let control_plane = db.control_plane_get(org, IdKind::Workflow, entity, IdKind::User, subject).await.expect("control_plane");
        assert_eq!(control_plane, ControlPerms::empty(), "workflow should be deleted from control plane");
    }

    #[tokio::test]
    async fn delete_entity_event_non_endpoint_skips_rbac_data() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // Upsert a template
        let upsert = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("template".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCA".to_string()),
            data_perms: Some("rw".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&upsert).await.expect("upsert template");

        // Delete the template entity — should only touch rbac_control
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: org,
            entity_kind: Some("template".to_string()),
            entity_uuid: Some(entity),
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((200, 0)),
        };
        db.delete_rbac_entity_event(&delete).await.expect("delete template entity");

        let control_plane = db.control_plane_get(org, IdKind::Template, entity, IdKind::User, subject).await.expect("control_plane");
        assert_eq!(control_plane, ControlPerms::empty(), "template should be deleted from control plane");
    }

    // -----------------------------------------------------------------------
    // Tombstone cross-org isolation (entity tombstones)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn data_plane_entity_tombstone_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        // Create entity tombstone in org1
        db.data_plane_remove_endpoint(org1, endpoint, 200, 0).await.expect("remove endpoint in org1");

        // Grant in org2 with lower version should succeed — org1's tombstone shouldn't block it
        let data = DataPlaneRbacData {
            org_uuid: org2,
            endpoint_uuid: endpoint,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: DataPerms::READ,
        };
        db.data_plane_grant(&data, 100, 0).await.expect("grant in org2");

        let perms = db.data_plane_get(org2, endpoint, IdKind::User, subject).await.expect("get org2");
        assert_eq!(perms, DataPerms::READ, "org1 entity tombstone should not block org2 grant");
    }

    #[tokio::test]
    async fn control_plane_entity_tombstone_scoped_to_org() {
        let db = test_db().await;
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org1).await;
        insert_org(&db, org2).await;

        // Create entity tombstone in org1
        db.control_plane_remove_entity(org1, IdKind::Endpoint, entity, 200, 0).await.expect("remove entity in org1");

        // Grant in org2 with lower version should succeed
        let data = ControlPlaneRbacData {
            org_uuid: org2,
            entity_kind: "endpoint".to_string(),
            entity_uuid: entity,
            subject_kind: "user".to_string(),
            subject_uuid: subject,
            perms: ControlPerms::READ,
        };
        db.control_plane_grant(&data, 100, 0).await.expect("grant in org2");

        let perms = db.control_plane_get(org2, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("get org2");
        assert_eq!(perms, ControlPerms::READ, "org1 entity tombstone should not block org2 grant");
    }

    // -----------------------------------------------------------------------
    // PG sync event: upsert replaces perms (not merges)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn upsert_event_replaces_perms_not_merges() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let entity = Uuid::new_v4();
        let subject = Uuid::new_v4();
        insert_org(&db, org).await;

        // First upsert with admin (broad perms)
        let event1 = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("RCPGA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&event1).await.expect("upsert admin");

        // Second upsert with read (narrow perms, higher version)
        let event2 = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(entity),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(subject),
            control_perms: Some("R".to_string()),
            data_perms: Some("r".to_string()),
            version: Some((200, 0)),
        };
        db.upsert_rbac_pg_event(&event2).await.expect("upsert read");

        // Control plane should be narrowed to READ only
        let control_plane = db.control_plane_get(org, IdKind::Endpoint, entity, IdKind::User, subject).await.expect("control_plane");
        assert_eq!(control_plane, ControlPerms::READ, "upsert should replace, not merge — admin bits should be gone");
        assert!(!control_plane.contains(ControlPerms::CONFIGURE));
        assert!(!control_plane.contains(ControlPerms::GRANT));

        // Data plane should be narrowed to READ only
        let data_plane = db.data_plane_get(org, entity, IdKind::User, subject).await.expect("data_plane");
        assert_eq!(data_plane, DataPerms::READ, "data plane should replace, not merge");
        assert!(!data_plane.contains(DataPerms::WRITE));
        assert!(!data_plane.contains(DataPerms::EXECUTE));
    }

    // -----------------------------------------------------------------------
    // PG sync: delete entity event version ordering across both tables
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn delete_entity_event_respects_version_ordering_across_both_tables() {
        use crate::db::rbac_pg_sync::{RbacPgSyncEvent, RbacPgSyncOp};

        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        insert_org(&db, org).await;

        // s1 at low version
        let ev1 = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(endpoint),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(s1),
            control_perms: Some("R".to_string()),
            data_perms: Some("r".to_string()),
            version: Some((100, 0)),
        };
        db.upsert_rbac_pg_event(&ev1).await.expect("upsert s1");

        // s2 at high version
        let ev2 = RbacPgSyncEvent {
            op: RbacPgSyncOp::Upsert,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(endpoint),
            subject_kind: Some("user".to_string()),
            subject_uuid: Some(s2),
            control_perms: Some("RCPGDA".to_string()),
            data_perms: Some("rwx".to_string()),
            version: Some((400, 0)),
        };
        db.upsert_rbac_pg_event(&ev2).await.expect("upsert s2");

        // Delete entity at version between the two
        let delete = RbacPgSyncEvent {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: org,
            entity_kind: Some("endpoint".to_string()),
            entity_uuid: Some(endpoint),
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: Some((200, 0)),
        };
        db.delete_rbac_entity_event(&delete).await.expect("entity delete");

        // s1 (v100) should be deactivated in both planes
        let cp1 = db.control_plane_get(org, IdKind::Endpoint, endpoint, IdKind::User, s1).await.expect("cp s1");
        let dp1 = db.data_plane_get(org, endpoint, IdKind::User, s1).await.expect("dp s1");
        assert_eq!(cp1, ControlPerms::empty(), "s1 control plane should be deactivated");
        assert_eq!(dp1, DataPerms::empty(), "s1 data plane should be deactivated");

        // s2 (v400) should survive in both planes
        let cp2 = db.control_plane_get(org, IdKind::Endpoint, endpoint, IdKind::User, s2).await.expect("cp s2");
        let dp2 = db.data_plane_get(org, endpoint, IdKind::User, s2).await.expect("dp s2");
        assert_eq!(cp2, ControlPerms::all(), "s2 control plane should survive");
        assert_eq!(dp2, DataPerms::all(), "s2 data plane should survive");
    }

    #[tokio::test]
    async fn data_plane_grant_endpoint_users_exclusive_writes_runtime_endpoint_grants() {
        let db = test_db().await;
        let org = Uuid::new_v4();
        let endpoint = Uuid::new_v4();
        let read_user = Uuid::new_v4();
        let write_user = Uuid::new_v4();
        insert_org(&db, org).await;

        let org_cache = OrganizationCacheUuid::from_raw_uuid(None, org);
        let endpoint_cache = EndpointCacheUuid::from_raw_uuid(Some(org_cache), endpoint);
        let grants = vec![
            (UserUuid::from(read_user), DataPerms::READ),
            (UserUuid::from(write_user), DataPerms::READ | DataPerms::WRITE),
        ];

        db.data_plane_grant_endpoint_users_exclusive(&endpoint_cache, &grants, 100)
            .await
            .expect("grant endpoint data-plane users");

        let read_data = db.data_plane_get(org, endpoint, IdKind::User, read_user).await.expect("read data");
        let write_data = db.data_plane_get(org, endpoint, IdKind::User, write_user).await.expect("write data");

        assert_eq!(read_data, DataPerms::READ);
        assert_eq!(write_data, DataPerms::READ | DataPerms::WRITE);
    }
}
    }
}
