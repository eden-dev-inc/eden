use super::decode_schema_row;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::sql_file;
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::OrganizationUuid;
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::pipeline::PipelineSchema;
use uuid::Uuid;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    pub async fn select_pipeline_uuid(
        &self,
        pipeline_uuid: &Uuid,
        org_uuid: &OrganizationUuid,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<PipelineSchema> {
        let conn = self.pg_connection().await?;
        decode_schema_row(
            conn.query_one(sql_file!("select", "pipeline/pipeline_uuid"), &[pipeline_uuid, org_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Pipeline))?,
        )
    }

    pub async fn select_pipeline_id(
        &self,
        pipeline_id: &str,
        org_uuid: &OrganizationUuid,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<PipelineSchema> {
        let conn = self.pg_connection().await?;
        decode_schema_row(
            conn.query_one(sql_file!("select", "pipeline/pipeline_id"), &[&pipeline_id, org_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Pipeline))?,
        )
    }

    pub async fn select_all_pipelines(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<PipelineSchema>> {
        let conn = self.pg_connection().await?;
        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "pipeline/pipelines"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Pipeline))?
        {
            schemas.push(decode_schema_row(row)?);
        }

        Ok(schemas)
    }

    pub async fn update_pipeline_status(&self, pipeline_uuid: &Uuid, status: &str, _telemetry: &mut TelemetryWrapper) -> ResultEP<u64> {
        let conn = self.pg_connection().await?;

        let rows = conn
            .execute(sql_file!("update", "pipeline_status"), &[pipeline_uuid, &status])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Pipeline))?;

        Ok(rows)
    }

    pub async fn update_pipeline_lsn(
        &self,
        pipeline_uuid: &Uuid,
        lsn: &Option<String>,
        _telemetry: &mut TelemetryWrapper,
    ) -> ResultEP<u64> {
        let conn = self.pg_connection().await?;

        let rows = conn
            .execute(sql_file!("update", "pipeline_lsn"), &[pipeline_uuid, lsn])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Pipeline))?;

        Ok(rows)
    }
}
