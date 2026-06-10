use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
#[cfg(not(embedded_db))]
use crate::sql_file;
use eden_core::error::EpError;
use eden_core::format::cache_id::RobotCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::{RobotId, RobotUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::Table;
use ep_core::database::schema::robot::RobotSchema;
#[cfg(not(embedded_db))]
use postgres_types::Json;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct InsertRobot {
    pub robot_schema: RobotSchema,
}

impl InsertRobot {
    pub fn new(robot_schema: RobotSchema) -> Self {
        Self { robot_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertRobot
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: robot UUID (UUID)
    /// $2: username (VARCHAR)
    /// $3: organization_uuid (UUID)
    /// $4: api_key (JSONB)
    /// $5: description (TEXT)
    /// $6: ttl (BIGINT)
    /// $7: expires_at (TIMESTAMP)
    /// $8: created_by (UUID)
    /// $9: updated_by (UUID)
    /// $10: created_at (TIMESTAMP)
    /// $11: updated_at (TIMESTAMP)
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let conn = db.pg_connection().await?;

        #[cfg(not(embedded_db))]
        {
            conn.execute(
                sql_file!("insert", "robot"),
                &[
                    &self.robot_schema.uuid(),
                    &self.robot_schema.username(),
                    &self.robot_schema.organization_uuid(),
                    &Json(self.robot_schema.api_key()),
                    &self.robot_schema.description(),
                    &self.robot_schema.ttl(),
                    &self.robot_schema.expires_at(),
                    &self.robot_schema.created_by(),
                    &self.robot_schema.updated_by(),
                    &self.robot_schema.created_at(),
                    &self.robot_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)?;
        }

        #[cfg(embedded_db)]
        {
            let api_key_json = serde_json::to_string(self.robot_schema.api_key())
                .map_err(|e| EpError::parse(format!("Failed to serialize api_key: {e}")))?;

            conn.execute(
                "INSERT INTO robots (uuid, username, organization_uuid, api_key, description, ttl, expires_at, created_by, updated_by, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                &[
                    &self.robot_schema.uuid(),
                    &self.robot_schema.username(),
                    &self.robot_schema.organization_uuid(),
                    &api_key_json,
                    &self.robot_schema.description(),
                    &self.robot_schema.ttl(),
                    &self.robot_schema.expires_at(),
                    &self.robot_schema.created_by(),
                    &self.robot_schema.updated_by(),
                    &self.robot_schema.created_at(),
                    &self.robot_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)?;

            conn.execute(
                "INSERT INTO organization_robots (organization_uuid, robot_uuid) VALUES (?1, ?2)",
                &[&self.robot_schema.organization_uuid(), &self.robot_schema.uuid()],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)?;
        }

        Ok(())
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        <DatabaseManager<R, P, C> as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::set_ex_cache(
            db,
            Some(OrganizationCacheUuid::new(None, self.robot_schema.organization_uuid().clone())),
            self.robot_schema.to_owned(),
            telemetry_wrapper,
        )
        .await
    }
}
