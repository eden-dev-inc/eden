use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{ApiCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::{ApiCacheUuid, CacheUuid, OrganizationCacheUuid};
use eden_core::format::{ApiId, ApiUuid, CacheObjectType, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::api::ApiSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;

pub struct DeleteApi {
    object: CacheObjectType<ApiCacheUuid, ApiCacheId>,
}

impl<R, P, C> DeleteMethod<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId, R, P, C> for DeleteApi
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<ApiCacheUuid, ApiCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<ApiCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::get_cache_uuid(
            db,
            <Self as DeleteMethod<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId, R, P, C>>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<ApiCacheUuid, ApiCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_cache_uuid: ApiCacheUuid,
        uuids: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        // remove template from organization
        for org_uuid in uuids.organization() {
            let org_key = Some(OrganizationCacheUuid::new(org_key.clone(), org_uuid));
            let org_cache_object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> = CacheObjectType::new(org_key.clone(), None);

            // get mutable role object
            let mut org_schema = <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::get_from_cache(db, &org_cache_object, telemetry_wrapper)
            .await?;

            // remove endpoint from eden_node object
            org_schema.remove_api_by_uuid(&deleted_cache_uuid.eden_uuid::<ApiUuid>());

            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::set_ex_cache(db, org_key.clone(), org_schema, telemetry_wrapper)
            .await?
        }

        Ok(())
    }
}
