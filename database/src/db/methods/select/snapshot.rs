use super::decode_schema_row;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::sql_file;
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::OrganizationUuid;
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::snapshot::SnapshotSchema;
use uuid::Uuid;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    pub async fn select_snapshot_uuid(
        &self,
        snapshot_uuid: &Uuid,
        org_uuid: &OrganizationUuid,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<SnapshotSchema> {
        let conn = self.pg_connection().await?;
        decode_schema_row(
            conn.query_one(sql_file!("select", "snapshot/snapshot_uuid"), &[snapshot_uuid, org_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?,
        )
    }

    pub async fn select_snapshot_id(
        &self,
        snapshot_id: &str,
        org_uuid: &OrganizationUuid,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<SnapshotSchema> {
        let conn = self.pg_connection().await?;
        decode_schema_row(
            conn.query_one(sql_file!("select", "snapshot/snapshot_id"), &[&snapshot_id, org_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?,
        )
    }

    pub async fn select_all_snapshots(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<SnapshotSchema>> {
        let conn = self.pg_connection().await?;
        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "snapshot/snapshots"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?
        {
            schemas.push(decode_schema_row(row)?);
        }

        Ok(schemas)
    }

    pub async fn update_snapshot_status(&self, snapshot_uuid: &Uuid, status: &str, _telemetry: &mut TelemetryWrapper) -> ResultEP<u64> {
        let conn = self.pg_connection().await?;

        let rows = conn
            .execute(sql_file!("update", "snapshot_status"), &[snapshot_uuid, &status])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?;

        Ok(rows)
    }

    /// Select all snapshots with an enabled schedule whose `next_run_at` is in the past
    /// and are not currently running. Returns tuples of `(SnapshotSchema, organization_uuid)`.
    pub async fn select_due_snapshots(&self, _telemetry: &mut TelemetryWrapper) -> ResultEP<Vec<(SnapshotSchema, Uuid)>> {
        let conn = self.pg_connection().await?;
        let mut results = vec![];

        for row in conn
            .query(sql_file!("select", "snapshot/snapshots_due"), &[])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?
        {
            let org_uuid: Uuid = row.try_get("organization_uuid").map_err(EpError::database)?;
            let schema: SnapshotSchema = decode_schema_row(row)?;
            results.push((schema, org_uuid));
        }

        Ok(results)
    }

    pub async fn update_snapshot_lsn(
        &self,
        snapshot_uuid: &Uuid,
        lsn: &Option<String>,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<u64> {
        let conn = self.pg_connection().await?;

        let rows = conn
            .execute(sql_file!("update", "snapshot_lsn"), &[snapshot_uuid, lsn])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?;

        Ok(rows)
    }

    pub async fn update_snapshot_schedule(
        &self,
        snapshot_uuid: &Uuid,
        last_run_at: &Option<chrono::DateTime<chrono::Utc>>,
        next_run_at: &Option<chrono::DateTime<chrono::Utc>>,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<u64> {
        let conn = self.pg_connection().await?;

        let rows = conn
            .execute(sql_file!("update", "snapshot_schedule"), &[snapshot_uuid, last_run_at, next_run_at])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))?;

        Ok(rows)
    }
}
