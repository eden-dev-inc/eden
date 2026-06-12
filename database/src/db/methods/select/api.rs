use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{ApiId, ApiUuid, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::FromRow;
use ep_core::database::schema::api::{ApiSchema, ApiSchemaIds};

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select api
    pub async fn select_api_uuid<T>(&self, api_uuid: &ApiUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "api/api_uuid"), &[api_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Api))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Api))
    }

    /// Select api
    pub async fn select_api_id<T>(&self, api_id: &ApiId, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "api/api_id"), &[api_id])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Api))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Api))
    }

    pub async fn select_all_apis_ids(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<ApiSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "api/apis"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Api))?
        {
            schemas.push(ApiSchemaIds::from(
                decode_schema_row::<ApiSchema>(row).map_err(|e| EpError::database_query_error(e, EntityType::Api))?,
            ));
        }

        Ok(schemas)
    }

    pub async fn select_all_apis(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<ApiSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "api/apis"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Api))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Api))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_apis_ids_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<ApiSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "api/apis_ids_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Api))?
        {
            schemas.push(ApiSchemaIds::from(
                decode_schema_row::<ApiSchema>(row).map_err(|e| EpError::database_query_error(e, EntityType::Api))?,
            ));
        }

        Ok(schemas)
    }

    pub async fn select_all_apis_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<ApiSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "api/apis_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Api))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Api))?);
        }

        Ok(schemas)
    }
}
