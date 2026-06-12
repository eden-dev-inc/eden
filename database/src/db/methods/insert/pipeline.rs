use crate::db::lib::DatabaseManager;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::sql_file;
use eden_core::error::{EntityType, EpError};
use eden_core::format::{EdenUuid, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::pipeline::PipelineSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertPipeline {
    org_uuid: OrganizationUuid,
    pipeline_schema: PipelineSchema,
}

impl InsertPipeline {
    pub fn new(org_uuid: OrganizationUuid, pipeline_schema: PipelineSchema) -> Self {
        Self { org_uuid, pipeline_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertPipeline
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let cdc_config_json = serde_json::to_value(self.pipeline_schema.cdc_config()).unwrap_or_default();

        db.pg_connection()
            .await?
            .query_one(
                sql_file!("insert", "pipeline"),
                &[
                    &self.pipeline_schema.id(),                  // $1
                    &self.pipeline_schema.uuid(),                // $2
                    &self.pipeline_schema.description(),         // $3
                    &self.pipeline_schema.status().to_string(),  // $4
                    &self.pipeline_schema.source_endpoint(),     // $5
                    &self.pipeline_schema.target_endpoint(),     // $6
                    &self.pipeline_schema.filter(),              // $7
                    &cdc_config_json,                            // $8
                    &self.pipeline_schema.last_lsn(),            // $9
                    &self.pipeline_schema.write_template_uuid(), // $10
                    &self.pipeline_schema.read_template_uuid(),  // $11
                    &self.pipeline_schema.created_by(),          // $12
                    &self.org_uuid.uuid(),                       // $13
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Pipeline))
    }

    async fn insert_cache(&self, _db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        Ok(())
    }
}
