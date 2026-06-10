use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::{WorkflowId, WorkflowUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::FromRow;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select workflow
    pub async fn select_workflow_uuid<T>(&self, workflow_uuid: &WorkflowUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "workflow_uuid"), &[&workflow_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?,
        )
    }

    /// Select workflow
    pub async fn select_workflow_id<T>(&self, workflow_id: &WorkflowId, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "workflow_id"), &[&workflow_id])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?,
        )
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub(crate) mod select_workflow {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use eden_core::error::ResultEP;
    use eden_core::format::CacheUuid;
    use eden_core::format::cache_id::{CacheId, WorkflowCacheId};
    use eden_core::format::cache_uuid::WorkflowCacheUuid;
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::workflow::WorkflowSchema;

    /// test module for testing workflow selection
    pub async fn select_workflow_id(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        workflow_cache_id: &WorkflowCacheId,
    ) -> ResultEP<WorkflowSchema> {
        let cache_schema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<WorkflowSchema, WorkflowCacheId>>::get_from_database(
                db_manager,
                workflow_cache_id,
                test_telemetry,
            )
            .await?;

        let db_schema = db_manager.select_workflow_id(&workflow_cache_id.eden_id(), test_telemetry).await?;

        assert_eq!(db_schema, cache_schema);

        Ok(db_schema)
    }

    /// test module for testing workflow selection
    #[allow(dead_code)]
    pub async fn select_workflow_uuid(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        workflow_cache_uuid: &WorkflowCacheUuid,
    ) -> ResultEP<WorkflowSchema> {
        let cache_schema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                WorkflowSchema,
                WorkflowCacheUuid,
            >>::get_from_database(db_manager, workflow_cache_uuid, test_telemetry)
            .await?;

        let db_schema: WorkflowSchema = db_manager.select_workflow_uuid(&workflow_cache_uuid.eden_uuid(), test_telemetry).await?;

        assert_eq!(db_schema, cache_schema);

        Ok(db_schema)
    }
}
