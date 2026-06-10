use crate::db::lib::DatabaseManager;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::sql_file;
use eden_core::error::{EntityType, EpError};
use eden_core::format::{EdenUuid, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::snapshot::SnapshotSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertSnapshot {
    org_uuid: OrganizationUuid,
    snapshot_schema: SnapshotSchema,
}

impl InsertSnapshot {
    pub fn new(org_uuid: OrganizationUuid, snapshot_schema: SnapshotSchema) -> Self {
        Self { org_uuid, snapshot_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertSnapshot
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let schedule_json = self
            .snapshot_schema
            .schedule()
            .as_ref()
            .map(|s| serde_json::to_value(s).unwrap_or_default())
            .unwrap_or(serde_json::Value::Null);

        let cdc_config_json = self
            .snapshot_schema
            .cdc_config()
            .as_ref()
            .map(|c| serde_json::to_value(c).unwrap_or_default())
            .unwrap_or(serde_json::Value::Null);

        let data_json = self.snapshot_schema.data().clone();

        db.pg_connection()
            .await?
            .query_one(
                sql_file!("insert", "snapshot"),
                &[
                    &self.snapshot_schema.id(),                      // $1
                    &self.snapshot_schema.uuid(),                    // $2
                    &self.snapshot_schema.description(),             // $3
                    &self.snapshot_schema.status().to_string(),      // $4
                    &self.snapshot_schema.source_endpoint(),         // $5
                    &self.snapshot_schema.target_endpoint(),         // $6
                    &data_json,                                      // $7
                    &self.snapshot_schema.preserve_ttl(),            // $8
                    &schedule_json,                                  // $9
                    &self.snapshot_schema.source_mode().to_string(), // $10
                    &self.snapshot_schema.filter(),                  // $11
                    &cdc_config_json,                                // $12
                    &self.snapshot_schema.last_lsn(),                // $13
                    &self.snapshot_schema.write_template_uuid(),     // $14
                    &self.snapshot_schema.read_template_uuid(),      // $15
                    &self.snapshot_schema.created_by(),              // $16
                    &self.snapshot_schema.updated_by(),              // $17
                    &self.snapshot_schema.created_at(),              // $18
                    &self.org_uuid.uuid(),                           // $19
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Snapshot))
    }

    async fn insert_cache(&self, _db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        // Snapshots don't use the generic cache layer.
        // They are fetched directly from Postgres.
        Ok(())
    }
}
