use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::{OrganizationUuid, RobotId, RobotUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus};
use ep_core::database::schema::FromRow;
use function_name::named;
use std::borrow::Cow;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select robot by UUID
    pub async fn select_robot_uuid<T>(&self, robot_uuid: &RobotUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "robot_uuid"), &[&robot_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Robot))?,
        )
    }

    /// Select robot by ID (username) scoped by organization UUID
    #[named]
    pub async fn select_robot_id<T>(
        &self,
        robot_id: &RobotId,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<T>
    where
        T: FromRow,
    {
        let mut span = telemetry_wrapper.client_tracer(format!("cache.{}", function_name!()));

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        span.add_simple_event("connected to postgres");

        let row = conn
            .query_one(sql_file!("select", "robot_id"), &[&robot_id, &organization_uuid])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

                EpError::database_query_error(e, EntityType::Robot)
            })
            .inspect(|row| span.add_event("collected row from postgres", vec![FastSpanAttribute::new("row_len", row.len().to_string())]))?;

        decode_schema_row(row).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })
    }
}
