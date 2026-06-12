use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{OrganizationUuid, TemplateId, TemplateUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::{ctx_with_trace, log_trace};
use ep_core::database::schema::FromRow;
use ep_core::database::schema::template::{TemplateSchema, TemplateSchemaIds};
use function_name::named;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select template
    pub async fn select_template_uuid<T>(&self, template_uuid: &TemplateUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "template/template_uuid"), &[&template_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Template))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Template))
    }

    /// Select template
    #[named]
    pub async fn select_template_id<T>(&self, template_id: &TemplateId, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let _ctx = ctx_with_trace!().with_feature("database").with_additional("template_id", template_id.to_string());

        log_trace!(
            _ctx,
            "select_template_id",
            audience = eden_logger_internal::LogAudience::Internal,
            template_id = template_id.to_string()
        );
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "template/template_id"), &[&template_id])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Template))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Template))
    }

    pub async fn select_all_templates_ids(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<TemplateSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "template/templates_ids"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Template))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Template))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_templates(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<TemplateSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "template/templates"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Template))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Template))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_templates_ids_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<TemplateSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "template/templates_ids_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Template))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Template))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_templates_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<TemplateSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "template/templates_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Template))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Template))?);
        }

        Ok(schemas)
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub(crate) mod select_template {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use eden_core::error::ResultEP;
    use eden_core::format::CacheUuid;
    use eden_core::format::cache_id::{CacheId, TemplateCacheId};
    use eden_core::format::cache_uuid::TemplateCacheUuid;
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::template::TemplateSchema;

    /// test module for testing template selection
    pub async fn select_template_id(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        template_cache_id: &TemplateCacheId,
    ) -> ResultEP<TemplateSchema> {
        let cache_schema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<TemplateSchema, TemplateCacheId>>::get_from_database(
                db_manager,
                template_cache_id,
                test_telemetry,
            )
            .await?;

        let db_schema = db_manager.select_template_id(&template_cache_id.eden_id(), test_telemetry).await?;

        assert_eq!(db_schema, cache_schema);

        Ok(db_schema)
    }

    /// test module for testing endpoint selection
    #[allow(dead_code)]
    pub async fn select_template_uuid(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        template_cache_uuid: &TemplateCacheUuid,
    ) -> ResultEP<TemplateSchema> {
        let cache_schema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                TemplateSchema,
                TemplateCacheUuid,
            >>::get_from_database(db_manager, template_cache_uuid, test_telemetry)
            .await?;

        let db_schema = db_manager.select_template_uuid(&template_cache_uuid.eden_uuid(), test_telemetry).await?;

        assert_eq!(db_schema, cache_schema);

        Ok(db_schema)
    }
}
