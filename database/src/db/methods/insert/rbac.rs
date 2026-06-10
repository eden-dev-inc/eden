#[cfg(not(embedded_db))]
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
#[cfg(not(embedded_db))]
use crate::db::rbac_pg_sync::RbacPgSyncEvent;
#[cfg(not(embedded_db))]
use crate::sql_file;
#[cfg(not(embedded_db))]
use eden_core::error::{EpError, ResultEP};
#[cfg(not(embedded_db))]
use eden_core::format::rbac::{ControlPerms, DataPerms};

#[cfg(not(embedded_db))]
impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Upsert into `rbac_control` and (for endpoint entities)
    /// `rbac_data` using explicit permission strings from the sync event.
    #[allow(dead_code)]
    pub(crate) async fn upsert_rbac_pg_event(&self, event: &RbacPgSyncEvent) -> ResultEP<()> {
        let (entity_kind, entity_uuid) = event.entity_parts()?;
        let (subject_kind, subject_uuid) = event.subject_parts()?;
        let (version_ms, version_seq) = event.version()?;
        let control_plane_perms = ControlPerms::from_perm_str(event.control_perms()?)?.to_perm_string();

        let mut conn = self.pg_connection().await?;
        let tx = conn.transaction().await.map_err(|e| EpError::database(format!("Failed to start transaction for rbac upsert: {e}")))?;

        // Always write to rbac_control
        tx.execute(
            sql_file!("insert", "rbac_control"),
            &[
                &event.org_uuid,
                &entity_kind,
                entity_uuid,
                &subject_kind,
                subject_uuid,
                &control_plane_perms.as_str(),
                &version_ms,
                &version_seq,
            ],
        )
        .await
        .map_err(|e| EpError::database(format!("Failed to upsert rbac_control: {e}")))?;

        // For endpoint entities, also write to rbac_data
        if entity_kind == "endpoint" {
            let data_plane_perms = match event.data_perms() {
                Some(perms) => DataPerms::from_perm_str(perms)?.to_perm_string(),
                None => DataPerms::empty().to_perm_string(),
            };
            tx.execute(
                sql_file!("insert", "rbac_data"),
                &[
                    &event.org_uuid,
                    entity_uuid, // endpoint_uuid
                    &subject_kind,
                    subject_uuid,
                    &data_plane_perms.as_str(),
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to upsert rbac_data: {e}")))?;
        }

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit rbac upsert: {e}")))?;

        Ok(())
    }
}
