use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{EndpointGroupCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EndpointGroupCacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EndpointGroupId, EndpointGroupUuid, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::endpoint_group::EndpointGroupSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;

pub struct DeleteEndpointGroup {
    object: CacheObjectType<EndpointGroupCacheUuid, EndpointGroupCacheId>,
}

impl<R, P, C> DeleteMethod<EndpointGroupSchema, EndpointGroupCacheUuid, EndpointGroupUuid, EndpointGroupCacheId, EndpointGroupId, R, P, C>
    for DeleteEndpointGroup
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<EndpointGroupCacheUuid, EndpointGroupCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(
        &self,
        db: &DatabaseManager<R, P, C>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<EndpointGroupCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            EndpointGroupSchema,
            EndpointGroupCacheUuid,
            EndpointGroupUuid,
            EndpointGroupCacheId,
            EndpointGroupId,
        >>::get_cache_uuid(
            db,
            <Self as DeleteMethod<
                EndpointGroupSchema,
                EndpointGroupCacheUuid,
                EndpointGroupUuid,
                EndpointGroupCacheId,
                EndpointGroupId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<EndpointGroupCacheUuid, EndpointGroupCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_cache_uuid: EndpointGroupCacheUuid,
        uuids: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        // Remove endpoint group from organization cache
        for org_uuid in uuids.organization() {
            let org_key = Some(OrganizationCacheUuid::new(org_key.clone(), org_uuid));
            let org_cache_object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> = CacheObjectType::new(org_key.clone(), None);

            let mut org_schema = <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::get_from_cache(db, &org_cache_object, telemetry_wrapper)
            .await?;

            org_schema.remove_endpoint_group_by_uuid(&deleted_cache_uuid.eden_uuid::<EndpointGroupUuid>());

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
