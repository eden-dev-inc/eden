use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{OrganizationCacheId, TemplateCacheId, WorkflowCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid, WorkflowCacheUuid};
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid, TemplateId, TemplateUuid, WorkflowId, WorkflowUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::template::TemplateSchema;
use ep_core::database::schema::workflow::WorkflowSchema;
use function_name::named;

pub struct DeleteTemplate {
    object: CacheObjectType<TemplateCacheUuid, TemplateCacheId>,
}

impl<R, P, C> DeleteMethod<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId, R, P, C> for DeleteTemplate
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<TemplateCacheUuid, TemplateCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<TemplateCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
        >>::get_cache_uuid(
            db,
            <Self as DeleteMethod<
                TemplateSchema,
                TemplateCacheUuid,
                TemplateUuid,
                TemplateCacheId,
                TemplateId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<TemplateCacheUuid, TemplateCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_cache_uuid: TemplateCacheUuid,
        uuids: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        // remove template from workflow
        for workflow_uuid in uuids.workflow() {
            let workflow_cache_object: CacheObjectType<WorkflowCacheUuid, WorkflowCacheId> =
                CacheObjectType::new(Some(WorkflowCacheUuid::new(org_key.clone(), workflow_uuid)), None);

            // get mutable role object
            let mut workflow_schema: WorkflowSchema = <DatabaseManager<R, P, C> as CacheFunctions<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
            >>::get_from_cache(db, &workflow_cache_object, telemetry_wrapper)
            .await?;

            // remove endpoint from eden_node object
            workflow_schema.remove_template_uuid(deleted_cache_uuid.eden_uuid::<TemplateUuid>().clone());

            <DatabaseManager<R, P, C> as CacheFunctions<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
            >>::set_ex_cache(db, org_key.clone(), workflow_schema, telemetry_wrapper)
            .await?
        }

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
            org_schema.remove_template_by_uuid(&deleted_cache_uuid.eden_uuid::<TemplateUuid>());

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

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use super::*;
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::insert::endpoint::tests::insert_endpoint;
    use crate::methods::insert::template::InsertTemplate;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, TemplateCacheId};
    use eden_core::format::cache_uuid::TemplateCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, TemplateId};
    use ep_core::database::schema::Table;
    use ep_core::database::schema::template::TemplateSchema;
    use ep_core::database::template::JsonTemplate;
    use ep_core::database::template::wrapper::TemplateValue;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    #[tokio::test]
    async fn delete() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        // endpoint is needed to create a template
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
        // template
        let template_schema = TemplateSchema::new(
            TemplateId::from("test_template"),
            JsonTemplate::new(
                endpoint_schema.uuid(),
                crate::template::TemplateKind::Read,
                TemplateValue::new(serde_json::Value::Null),
                Vec::new(),
                endpoint_schema.kind(),
                None,
            )
            .expect("Failed to create template"),
            Some("Test template".to_string()),
            None,
            UserUuid::new_uuid(),
        );

        let insert_template = InsertTemplate::new(organization_schema.uuid(), template_schema.clone());
        insert_template.insert_database(&db_manager, test_telemetry).await.expect("Insert failed");

        let org_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            template_schema.uuid(),
        ));
        let template_cache_uuid = Some(TemplateCacheUuid::new(org_cache_uuid.clone(), template_schema.uuid()));
        let template_cache_id = Some(TemplateCacheId::new(org_cache_uuid, template_schema.id()));
        let object: CacheObjectType<TemplateCacheUuid, TemplateCacheId> =
            CacheObjectType::<TemplateCacheUuid, TemplateCacheId>::new(template_cache_uuid, template_cache_id);
        let delete_template = <DeleteTemplate as DeleteMethod<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >>::new(object);
        let removed_uuids = DeleteMethod::<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >::delete_database(&delete_template, &db_manager, test_telemetry)
        .await
        .expect("Delete failed");
        assert!(removed_uuids.organization().contains(&organization_schema.uuid()));

        //manually teardown containers
    }
}
