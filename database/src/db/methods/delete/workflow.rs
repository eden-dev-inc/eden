use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{OrganizationCacheId, WorkflowCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, WorkflowCacheUuid};
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid, WorkflowId, WorkflowUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::{organization::OrganizationSchema, workflow::WorkflowSchema};
use function_name::named;

pub struct DeleteWorkflow {
    object: CacheObjectType<WorkflowCacheUuid, WorkflowCacheId>,
}

impl<R, P, C> DeleteMethod<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId, R, P, C> for DeleteWorkflow
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<WorkflowCacheUuid, WorkflowCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<WorkflowCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            WorkflowSchema,
            WorkflowCacheUuid,
            WorkflowUuid,
            WorkflowCacheId,
            WorkflowId,
        >>::get_cache_uuid(
            db,
            <Self as DeleteMethod<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<WorkflowCacheUuid, WorkflowCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_cache_uuid: WorkflowCacheUuid,
        uuids: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        // remove workflow from organization
        for org_uuid in uuids.organization() {
            let org_key = Some(OrganizationCacheUuid::new(org_key.clone(), org_uuid));
            let org_cache_object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> = CacheObjectType::new(org_key.clone(), None);

            // get mutable role object
            let mut org_schema: OrganizationSchema = <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::get_from_cache(db, &org_cache_object, telemetry_wrapper)
            .await?;

            org_schema.remove_workflow_by_uuid(&deleted_cache_uuid.eden_uuid::<WorkflowUuid>());

            // update org cache
            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::set_ex_cache(db, org_key, org_schema, telemetry_wrapper)
            .await?
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::delete::DeleteMethod;
    use crate::methods::delete::workflow::DeleteWorkflow;
    use crate::methods::insert::endpoint::tests::insert_endpoint;
    use crate::methods::insert::workflow::insert_workflow::insert_workflow;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::cache_id::{CacheId, WorkflowCacheId};
    use eden_core::format::cache_uuid::WorkflowCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, WorkflowId, WorkflowUuid};
    use ep_core::database::schema::Table;
    use ep_core::database::schema::workflow::WorkflowSchema;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    #[tokio::test]
    async fn delete() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        // template needs an endpoint
        let endpoint_schema = insert_endpoint(
            &db_manager,
            test_telemetry,
            "test_endpoint",
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            Some("Test Redis endpoint".to_string()),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        let org_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            workflow_schema.uuid(),
        ));
        let workflow_cache_uuid = Some(WorkflowCacheUuid::new(org_cache_uuid.clone(), workflow_schema.uuid()));
        let workflow_cache_id = Some(WorkflowCacheId::new(org_cache_uuid, workflow_schema.id()));
        let object: CacheObjectType<WorkflowCacheUuid, WorkflowCacheId> =
            CacheObjectType::<WorkflowCacheUuid, WorkflowCacheId>::new(workflow_cache_uuid, workflow_cache_id);
        let delete_workflow = <DeleteWorkflow as DeleteMethod<
            WorkflowSchema,
            WorkflowCacheUuid,
            WorkflowUuid,
            WorkflowCacheId,
            WorkflowId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >>::new(object);
        let removed_uuids = DeleteMethod::<
            WorkflowSchema,
            WorkflowCacheUuid,
            WorkflowUuid,
            WorkflowCacheId,
            WorkflowId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >::delete_database(&delete_workflow, &db_manager, test_telemetry)
        .await
        .expect("Failed to delete workflow");
        assert!(removed_uuids.organization().contains(&organization_schema.uuid()));

        //manually teardown containers
    }
}
