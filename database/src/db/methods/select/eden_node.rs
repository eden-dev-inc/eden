use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::{EdenNodeId, EdenNodeUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::FromRow;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select eden_node
    pub async fn select_eden_node_uuid<T>(&self, eden_node_uuid: &EdenNodeUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "eden_node_uuid"), &[&eden_node_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::EdenNode))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::EdenNode))
    }

    /// Select eden_node
    pub async fn select_eden_node_id<T>(&self, eden_node_id: &EdenNodeId, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "eden_node_id"), &[&eden_node_id])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::EdenNode))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::EdenNode))
    }
}
