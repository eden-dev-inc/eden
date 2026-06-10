use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::{
    db::{cache::CacheFunctions, lib::DatabaseManager},
    sql_file,
};
use eden_core::format::{AuthId, AuthUuid, EdenUuid, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::EpError,
    format::{
        cache_id::AuthCacheId,
        cache_uuid::{AuthCacheUuid, CacheUuid, OrganizationCacheUuid},
    },
};
use ep_core::database::schema::Table;
use ep_core::database::schema::auth::AuthSchema;
use function_name::named;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertAuth {
    org_uuid: OrganizationUuid,
    auth_schema: AuthSchema,
}

impl<R, P, C> Insert<R, P, C> for InsertAuth
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: auth id (VARCHAR)
    /// $2: auth uuid (UUID)
    /// $3: auth authtype (TEXT)
    /// $4: endpoint uuid (UUID)
    /// $5: created_at (TIMESTAMP)
    /// $6: updated_at (TIMESTAMP)
    #[named]
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        span.add_simple_event("inserting auth schema into database");
        db.pg_connection()
            .await?
            .execute(
                sql_file!("insert", "auth"),
                &[
                    &self.auth_schema.id().to_string(),
                    &self.auth_schema.uuid().uuid(),
                    &self.auth_schema.auth(),
                    &self.auth_schema.endpoint_uuid(),
                    &self.auth_schema.created_at(),
                    &self.auth_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)
    }
    #[named]
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <DatabaseManager<R, P, C> as CacheFunctions<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId>>::set_ex_cache(
            db,
            Some(OrganizationCacheUuid::new(None, self.org_uuid.to_owned())),
            self.auth_schema.to_owned(),
            telemetry_wrapper,
        )
        .await
    }
}
