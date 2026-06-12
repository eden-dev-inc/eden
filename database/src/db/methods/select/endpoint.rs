use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{EdenUuid, EndpointId, EndpointUuid, OrganizationUuid};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_schema::endpoint::{EndpointSchema, EndpointSchemaIds};
use ep_core::database::schema::FromRow;
use function_name::named;
use std::borrow::Cow;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select endpoint
    #[named]
    pub async fn select_endpoint_uuid<T>(&self, endpoint_uuid: &EndpointUuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "endpoint/endpoint_uuid"), &[&endpoint_uuid.uuid()])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))
    }

    /// Select endpoint
    #[named]
    pub async fn select_endpoint_id<T>(&self, endpoint_id: &EndpointId, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self
            .pg_connection()
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

        let row = conn.query_one(sql_file!("select", "endpoint/endpoint_id"), &[&endpoint_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

            EpError::database_query_error(e, EntityType::Endpoint)
        })?;

        decode_schema_row(row).map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

            EpError::database_query_error(e, EntityType::Endpoint)
        })
    }

    pub async fn select_all_endpoints_ids(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "endpoint/endpoints_ids"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_endpoints(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "endpoint/endpoints"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_endpoints_ids_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "endpoint/endpoints_ids_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_endpoints_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "endpoint/endpoints_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?);
        }

        Ok(schemas)
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub(crate) mod select_endpoint {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use eden_core::error::ResultEP;
    use eden_core::format::CacheUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::EndpointCacheUuid;
    use eden_core::telemetry::TelemetryWrapper;
    use endpoint_schema::endpoint::EndpointSchema;

    /// test module for testing endpoint selection
    pub async fn select_endpoint_id(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        endpoint_cache_id: &EndpointCacheId,
    ) -> ResultEP<EndpointSchema> {
        let cache_schema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::get_from_database(
                db_manager,
                endpoint_cache_id,
                test_telemetry,
            )
            .await?;

        let db_schema = db_manager.select_endpoint_id(&endpoint_cache_id.eden_id(), test_telemetry).await?;

        assert_eq!(db_schema, cache_schema);

        Ok(db_schema)
    }

    /// test module for testing endpoint selection
    #[allow(dead_code)]
    pub async fn select_endpoint_uuid(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        endpoint_cache_uuid: &EndpointCacheUuid,
    ) -> ResultEP<EndpointSchema> {
        let cache_schema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                EndpointSchema,
                EndpointCacheUuid,
            >>::get_from_database(db_manager, endpoint_cache_uuid, test_telemetry)
            .await?;

        let db_schema = db_manager.select_endpoint_uuid(&endpoint_cache_uuid.eden_uuid(), test_telemetry).await?;

        assert_eq!(db_schema, cache_schema);

        Ok(db_schema)
    }
}
