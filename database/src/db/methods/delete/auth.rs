use crate::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::AuthCacheId;
use eden_core::format::cache_uuid::{AuthCacheUuid, OrganizationCacheUuid};
use eden_core::format::{AuthId, AuthUuid, CacheObjectType};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::auth::AuthSchema;
use function_name::named;

pub struct DeleteAuth {
    object: CacheObjectType<AuthCacheUuid, AuthCacheId>,
}

impl<R, P, C> DeleteMethod<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId, R, P, C> for DeleteAuth
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<AuthCacheUuid, AuthCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<AuthCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId>>::get_cache_uuid(
            db,
            <Self as DeleteMethod<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId, R, P, C>>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<AuthCacheUuid, AuthCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        _db: &DatabaseManager<R, P, C>,
        _deleted_cache_uuid: AuthCacheUuid,
        _uuids_to_update: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let _org_uuid = org_key.clone();

        // Auth entities don't have dependent relations that need cache updates
        Ok(())
    }
}
