use crate::db::cache::{CacheFunctions, CacheUuidFunctions};
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{EdenNodeCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EdenNodeCacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EdenNodeId, EdenNodeUuid, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;

pub struct DeleteEdenNode {
    object: CacheObjectType<EdenNodeCacheUuid, EdenNodeCacheId>,
}

impl<R, P, C> DeleteMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeUuid, EdenNodeCacheId, EdenNodeId, R, P, C> for DeleteEdenNode
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<EdenNodeCacheUuid, EdenNodeCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<EdenNodeCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeUuid,
            EdenNodeCacheId,
            EdenNodeId,
        >>::get_cache_uuid(
            db,
            <Self as DeleteMethod<
                EdenNodeSchema,
                EdenNodeCacheUuid,
                EdenNodeUuid,
                EdenNodeCacheId,
                EdenNodeId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<EdenNodeCacheUuid, EdenNodeCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        _deleted_cache_uuid: EdenNodeCacheUuid,
        uuids: &UuidsToUpdate,
        _org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let eden_node_uuid: EdenNodeUuid = <DatabaseManager<R, P, C> as CacheFunctions<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeUuid,
            EdenNodeCacheId,
            EdenNodeId,
        >>::get_cache_uuid(
            db,
            <DeleteEdenNode as DeleteMethod<
                EdenNodeSchema,
                EdenNodeCacheUuid,
                EdenNodeUuid,
                EdenNodeCacheId,
                EdenNodeId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await?
        .eden_uuid();

        for org_uuid in uuids.organization() {
            let org_uuid = OrganizationCacheUuid::new(None, org_uuid);

            let mut org_schema =
                <DatabaseManager<R, P, C> as CacheUuidFunctions<OrganizationSchema, OrganizationCacheUuid>>::get_from_cache(
                    db,
                    &org_uuid,
                    telemetry_wrapper,
                )
                .await?;

            // remove auth from role object
            org_schema.remove_eden_node_by_uuid(&eden_node_uuid);

            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::set_ex_cache(db, Some(org_uuid), org_schema, telemetry_wrapper)
            .await?
        }

        Ok::<_, EpError>(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::delete::DeleteMethod;
    use crate::methods::delete::eden_node::DeleteEdenNode;
    use crate::methods::insert::Insert;
    use crate::methods::insert::endpoint::InsertEndpoint;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EdenNodeCacheId};
    use eden_core::format::cache_uuid::EdenNodeCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, EdenNodeId, EdenNodeUuid, EndpointId, OrganizationCacheUuid};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::eden_node::EdenNodeSchema;
    use ep_core::ep::EpConfig;
    use mongo_core::config::MongoConfig;

    #[tokio::test]
    async fn delete() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        // add an endpoint, they need to get deleted when eden_node is deleted
        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            MongoConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());
        insert_endpoint.insert_database(&db_manager, test_telemetry).await.unwrap_or_default();

        // Delete eden_node and check if UUIDs of references are returned
        let eden_node_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            eden_node_schema.uuid(),
        ));
        let eden_node_cache_id = Some(CacheId::new(Some(OrganizationCacheUuid::from(organization_schema.uuid())), eden_node_schema.id()));
        let org_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            eden_node_schema.uuid(),
        ));

        let object: CacheObjectType<EdenNodeCacheUuid, EdenNodeCacheId> =
            CacheObjectType::<EdenNodeCacheUuid, EdenNodeCacheId>::new(eden_node_cache_uuid.clone(), eden_node_cache_id);

        let delete_eden_node = <DeleteEdenNode as DeleteMethod<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeUuid,
            EdenNodeCacheId,
            EdenNodeId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >>::new(object);

        let eden_node_cache_uuid = Some(EdenNodeCacheUuid::new(org_cache_uuid.clone(), eden_node_schema.uuid()));
        let eden_node_cache_id = Some(EdenNodeCacheId::new(org_cache_uuid, eden_node_schema.id()));
        let _object: CacheObjectType<EdenNodeCacheUuid, EdenNodeCacheId> =
            CacheObjectType::<EdenNodeCacheUuid, EdenNodeCacheId>::new(eden_node_cache_uuid, eden_node_cache_id);
        let removed_uuids = DeleteMethod::<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeUuid,
            EdenNodeCacheId,
            EdenNodeId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >::delete_database(&delete_eden_node, &db_manager, test_telemetry)
        .await
        .unwrap_or_default();
        assert!(removed_uuids.endpoint().contains(&endpoint_schema.uuid()));
        assert!(removed_uuids.organization().contains(&organization_schema.uuid()));

        //manually teardown containers
    }
}
