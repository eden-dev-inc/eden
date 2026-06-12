#[cfg(not(embedded_db))]
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
#[cfg(not(embedded_db))]
use crate::db::rbac_pg_sync::RbacPgSyncEvent;
#[cfg(not(embedded_db))]
use crate::sql_file;
#[cfg(not(embedded_db))]
use eden_core::error::{EpError, ResultEP};

#[cfg(not(embedded_db))]
impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// soft-delete a single row in `rbac_control` and `rbac_data`.
    #[allow(dead_code)]
    pub(crate) async fn delete_rbac_row_event(&self, event: &RbacPgSyncEvent) -> ResultEP<()> {
        let (entity_kind, entity_uuid) = event.entity_parts()?;
        let (subject_kind, subject_uuid) = event.subject_parts()?;
        let (version_ms, version_seq) = event.version()?;

        self.pg_connection()
            .await?
            .execute(
                sql_file!("delete", "rbac_row_delete"),
                &[
                    &event.org_uuid,
                    &entity_kind,
                    entity_uuid,
                    &subject_kind,
                    subject_uuid,
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to delete RBAC row state: {e}")))?;

        Ok(())
    }

    /// subject-wide delete in `rbac_control` and `rbac_data`.
    #[allow(dead_code)]
    pub(crate) async fn delete_rbac_subject_event(&self, event: &RbacPgSyncEvent) -> ResultEP<()> {
        let (subject_kind, subject_uuid) = event.subject_parts()?;
        let (version_ms, version_seq) = event.version()?;

        self.pg_connection()
            .await?
            .execute(
                sql_file!("delete", "rbac_subject_delete"),
                &[&event.org_uuid, &subject_kind, subject_uuid, &version_ms, &version_seq],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to delete RBAC subject state: {e}")))?;

        Ok(())
    }

    /// Purge stale RBAC replay-guard tombstones whose `updated_at` is older than `cutoff`.
    ///
    /// These tombstones are not the canonical mutation history for an entity.
    /// They are replay guards for versioned RBAC writes: a delete writes the
    /// highest seen `(version_ms, version_seq)` so that older replayed grants
    /// cannot resurrect revoked access.
    ///
    /// A newer grant is still allowed to re-provision access, so keeping these
    /// rows forever is unnecessary. After the replay window has safely passed
    /// (configured via `rbac_pg_sync.tombstone_retention_days`), they can be
    /// purged to prevent unbounded growth.
    pub async fn purge_rbac_tombstones(&self, cutoff: chrono::DateTime<chrono::Utc>) -> ResultEP<u64> {
        let mut conn = self.pg_connection().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| EpError::database(format!("Failed to start transaction for tombstone purge: {e}")))?;

        let mut total: u64 = 0;
        for (table, sql) in [
            ("rbac_control_row_tombstones", sql_file!("delete", "rbac_control_row_tombstones_purge")),
            ("rbac_control_entity_tombstones", sql_file!("delete", "rbac_control_entity_tombstones_purge")),
            ("rbac_control_subject_tombstones", sql_file!("delete", "rbac_control_subject_tombstones_purge")),
            ("rbac_data_row_tombstones", sql_file!("delete", "rbac_data_row_tombstones_purge")),
            ("rbac_data_entity_tombstones", sql_file!("delete", "rbac_data_entity_tombstones_purge")),
            ("rbac_data_subject_tombstones", sql_file!("delete", "rbac_data_subject_tombstones_purge")),
            ("rbac_entity_tombstones", sql_file!("delete", "rbac_entity_tombstones_purge")),
            ("rbac_subject_tombstones", sql_file!("delete", "rbac_subject_tombstones_purge")),
        ] {
            let n = tx.execute(sql, &[&cutoff]).await.map_err(|e| EpError::database(format!("Failed to purge {table}: {e}")))?;
            total += n;
        }

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit tombstone purge: {e}")))?;

        Ok(total)
    }

    /// entity-wide delete in `rbac_control` and `rbac_data`.
    #[allow(dead_code)]
    pub(crate) async fn delete_rbac_entity_event(&self, event: &RbacPgSyncEvent) -> ResultEP<()> {
        let (entity_kind, entity_uuid) = event.entity_parts()?;
        let (version_ms, version_seq) = event.version()?;

        self.pg_connection()
            .await?
            .execute(
                sql_file!("delete", "rbac_entity_delete"),
                &[&event.org_uuid, &entity_kind, entity_uuid, &version_ms, &version_seq],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to delete RBAC entity state: {e}")))?;

        Ok(())
    }
}
