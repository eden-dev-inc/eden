use crate::db::cache::{CacheFunctions, CacheUuidFunctions};
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{AuthCacheId, EdenNodeCacheId, EndpointCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::{AuthCacheUuid, CacheUuid, EdenNodeCacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::{
    AuthId, AuthUuid, CacheObjectType, EdenNodeId, EdenNodeUuid, EndpointId, EndpointUuid, OrganizationId, OrganizationUuid,
};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_schema::endpoint::EndpointSchema;
use ep_core::database::schema::auth::AuthSchema;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;

pub struct DeleteEndpoint {
    object: CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
}

impl<R, P, C> DeleteMethod<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId, R, P, C> for DeleteEndpoint
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<EndpointCacheUuid, EndpointCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<EndpointCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::get_cache_uuid(
            db,
            <DeleteEndpoint as DeleteMethod<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<EndpointCacheUuid, EndpointCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_uuid: EndpointCacheUuid,
        uuids: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        for org_uuid in uuids.organization() {
            let org_uuid = OrganizationCacheUuid::new(None, org_uuid);
            // get mutable object
            let mut org_schema: OrganizationSchema = <DatabaseManager<R, P, C> as CacheUuidFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
            >>::get_from_cache(db, &org_uuid, telemetry_wrapper)
            .await?;

            org_schema.remove_endpoint_by_uuid(&deleted_uuid.eden_uuid::<EndpointUuid>());

            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::set_ex_cache(db, org_key.clone(), org_schema, telemetry_wrapper)
            .await?;
        }

        // invalidate auths
        for auth_uuid in uuids.auth() {
            let auth_cache_object = CacheObjectType::new(Some(AuthCacheUuid::new(org_key.clone(), auth_uuid)), None);

            <DatabaseManager<R, P, C> as CacheFunctions<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId>>::invalidate(
                db,
                &auth_cache_object,
                telemetry_wrapper,
            )
            .await?;
        }

        for eden_node_uuid in uuids.eden_node() {
            let eden_node_cache_object = CacheObjectType::new(Some(EdenNodeCacheUuid::new(org_key.clone(), eden_node_uuid)), None);

            // get mutable role object
            let mut eden_node_schema = <DatabaseManager<R, P, C> as CacheFunctions<
                EdenNodeSchema,
                EdenNodeCacheUuid,
                EdenNodeUuid,
                EdenNodeCacheId,
                EdenNodeId,
            >>::get_from_cache(db, &eden_node_cache_object, telemetry_wrapper)
            .await?;

            // remove endpoint from eden_node object
            eden_node_schema.remove_endpoint_uuid(deleted_uuid.eden_uuid::<EndpointUuid>().clone());

            <DatabaseManager<R, P, C> as CacheFunctions<
                EdenNodeSchema,
                EdenNodeCacheUuid,
                EdenNodeUuid,
                EdenNodeCacheId,
                EdenNodeId,
            >>::set_ex_cache(db, org_key.clone(), eden_node_schema, telemetry_wrapper)
            .await?
        }

        Ok::<_, EpError>(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::delete::DeleteMethod;
    use crate::methods::delete::endpoint::DeleteEndpoint;
    use crate::methods::insert::endpoint::InsertEndpoint;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::EndpointCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, EndpointId, EndpointUuid, OrganizationCacheUuid};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    #[tokio::test]
    async fn delete() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());
        insert_endpoint.insert_database(&db_manager, test_telemetry).await.unwrap_or_default();

        let org_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            endpoint_schema.uuid(),
        ));
        let endpoint_cache_uuid = Some(EndpointCacheUuid::new(org_cache_uuid.clone(), endpoint_schema.uuid()));
        let endpoint_cache_id = Some(EndpointCacheId::new(org_cache_uuid, endpoint_schema.id()));
        let object: CacheObjectType<EndpointCacheUuid, EndpointCacheId> =
            CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::new(endpoint_cache_uuid, endpoint_cache_id);
        let delete_endpoint = <DeleteEndpoint as DeleteMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >>::new(object);
        let removed_uuids = DeleteMethod::<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >::delete_database(&delete_endpoint, &db_manager, test_telemetry)
        .await
        .expect("removed endpoint");
        assert!(removed_uuids.organization().contains(&organization_schema.uuid()));
        assert!(removed_uuids.eden_node().contains(&eden_node_schema.uuid()));

        //manually teardown containers
    }
}
