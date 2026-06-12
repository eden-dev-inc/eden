use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::{AuthId, AuthUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::FromRow;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select auth
    pub async fn select_auth_uuid<T>(&self, auth_uuid: &AuthUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(conn.query_one(sql_file!("select", "auth_uuid"), &[&auth_uuid]).await.map_err(EpError::database)?)
            .map_err(EpError::database)
    }

    /// Select auth
    pub async fn select_auth_id<T>(&self, auth_id: &AuthId, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(conn.query_one(sql_file!("select", "auth_id"), &[&auth_id]).await.map_err(EpError::database)?)
            .map_err(EpError::database)
    }
}
