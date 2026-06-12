use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::{OrganizationId, OrganizationUuid};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error};
use ep_core::database::schema::FromRow;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;
use std::borrow::Cow;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select organization
    #[named]
    pub async fn select_organization_uuid<T>(
        &self,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<T>
    where
        T: FromRow,
    {
        let mut span = telemetry_wrapper.client_tracer(format!("cache.{}", function_name!()));

        let _ctx = ctx_with_trace!().with_feature("database").with_additional("organization_uuid", organization_uuid.to_string());

        log_debug!(
            _ctx.clone(),
            "Select org UUID",
            audience = LogAudience::Internal,
            organization_uuid = organization_uuid.to_string()
        );
        let conn = self.pg_connection().await.map_err(|e| {
            log_error!(_ctx, "Select org ID error", audience = LogAudience::Internal, error = e.to_string());
            e
        })?;

        span.add_simple_event("connected to postgres");

        decode_schema_row(conn.query_one(sql_file!("select", "organization_uuid"), &[&organization_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

            EpError::database_query_error(e, EntityType::Organization)
        })?)
        .map_err(|e| EpError::database_query_error(e, EntityType::Organization))
    }

    /// Select organization
    #[named]
    pub async fn select_organization_id<T>(&self, organization_id: &OrganizationId, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let mut span = telemetry_wrapper.client_tracer(format!("cache.{}", function_name!()));

        let _ctx = ctx_with_trace!().with_feature("database").with_additional("organization_id", organization_id.to_string());

        log_debug!(
            _ctx.clone(),
            "Select org ID",
            audience = LogAudience::Internal,
            organization_id = organization_id.to_string()
        );
        let conn = self.pg_connection().await.map_err(|e| {
            log_error!(_ctx, "Select org ID error", audience = LogAudience::Internal, error = e.to_string());
            e
        })?;

        span.add_simple_event("connected to postgres");

        decode_schema_row(conn.query_one(sql_file!("select", "organization_id"), &[&organization_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

            EpError::database_query_error(e, EntityType::Organization)
        })?)
        .map_err(|e| EpError::database_query_error(e, EntityType::Organization))
    }

    pub async fn select_all_organizations(&self) -> ResultEP<Vec<OrganizationSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "organization/organizations"), &[])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Organization))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Organization))?);
        }

        Ok(schemas)
    }
}
