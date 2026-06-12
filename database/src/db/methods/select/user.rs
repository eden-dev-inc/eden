use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use chrono::{DateTime, Utc};
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::{OrganizationUuid, UserId, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus};
use ep_core::database::schema::FromRow;
use function_name::named;
use std::borrow::Cow;

/// Query parameters for paginated user listing with optional filters.
pub struct UsersPaginatedQuery {
    pub cursor_created_at: DateTime<Utc>,
    pub cursor_uuid: UserUuid,
    pub status_filter: Option<String>,
    pub perms_filter: Option<String>,
    pub limit: i64,
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select user
    pub async fn select_user_uuid<T>(&self, user_uuid: &UserUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "user_uuid"), &[&user_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::User))?,
        )
    }

    /// Select user
    #[named]
    pub async fn select_user_id<T>(
        &self,
        user_id: &UserId,
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
            .query_one(sql_file!("select", "user_id"), &[&user_id, &organization_uuid])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

                EpError::database_query_error(e, EntityType::User)
            })
            .inspect(|row| span.add_event("collected row from postgres", vec![FastSpanAttribute::new("row_len", row.len().to_string())]))?;

        decode_schema_row(row).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })
    }

    /// Select organization users with cursor pagination and optional RBAC-backed filters.
    pub async fn select_users_paginated_filtered<T>(&self, org_uuid: &OrganizationUuid, query: UsersPaginatedQuery) -> ResultEP<Vec<T>>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;
        let rows = conn
            .query(
                sql_file!("select", "users_paginated_filtered"),
                &[
                    org_uuid,
                    &query.cursor_created_at,
                    &query.cursor_uuid,
                    &query.status_filter,
                    &query.perms_filter,
                    &query.limit,
                ],
            )
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::User))?;

        let mut users = Vec::with_capacity(rows.len());
        for row in rows {
            users.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::User))?);
        }

        Ok(users)
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod tests {
    use super::*;
    use crate::cache::CacheIdFunctions;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::eden_node::insert_eden_node::insert_eden_node;
    use crate::methods::insert::organization::tests::insert_organization;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::auth::Password;
    use eden_core::format::CacheUuid;
    use eden_core::format::cache_id::{CacheId, UserCacheId};
    use eden_core::format::cache_uuid::OrganizationCacheUuid;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::user::UserSchema;

    #[tokio::test]
    async fn cache_id_user_lookup_is_scoped_by_organization() {
        let db_manager = create_database_manager().await;
        let test_telemetry = &mut test_telemetry();
        let eden_node = insert_eden_node(&db_manager, test_telemetry, "test_node", vec![], serde_json::Value::default()).await;

        let admin_credentials = [(UserId::from("admin"), Password::new("password".to_string()))];
        let (org_a, admin_users_a) =
            insert_organization(&db_manager, test_telemetry, "org_a", &admin_credentials, vec![eden_node.uuid()], None).await;
        let (org_b, admin_users_b) =
            insert_organization(&db_manager, test_telemetry, "org_b", &admin_credentials, vec![eden_node.uuid()], None).await;

        let lookup_a = UserCacheId::new(Some(OrganizationCacheUuid::new(None, org_a.uuid())), UserId::from("admin"));
        let lookup_b = UserCacheId::new(Some(OrganizationCacheUuid::new(None, org_b.uuid())), UserId::from("admin"));

        let resolved_a: UserSchema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<UserSchema, UserCacheId>>::get_from_database(
                &db_manager,
                &lookup_a,
                test_telemetry,
            )
            .await
            .expect("org_a user lookup should succeed");
        let resolved_b: UserSchema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<UserSchema, UserCacheId>>::get_from_database(
                &db_manager,
                &lookup_b,
                test_telemetry,
            )
            .await
            .expect("org_b user lookup should succeed");

        assert_eq!(resolved_a.uuid(), admin_users_a[0].uuid());
        assert_eq!(resolved_b.uuid(), admin_users_b[0].uuid());
        assert_ne!(resolved_a.organization_uuid(), resolved_b.organization_uuid());
    }
}
