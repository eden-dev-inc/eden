use crate::EdenDb;
use database::cache::CacheFunctions;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::TemplateCacheUuid;
use eden_core::format::{CacheObjectType, TemplateId, TemplateUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use std::future::Future;

pub mod delete;
pub mod get;
pub mod patch;
pub mod post;
pub mod recommendation;
pub mod render;
pub mod run;

// TODO: Convert to `async fn` when caller ergonomics are updated.
#[allow(clippy::manual_async_fn)]
pub(crate) fn get_template_schema<'a>(
    database_manager: &'a EdenDb,
    template_cache_object: &'a CacheObjectType<TemplateCacheUuid, TemplateCacheId>,
    telemetry_wrapper: &'a mut TelemetryWrapper,
) -> impl Future<Output = ResultEP<TemplateSchema>> + Send + 'a {
    async move {
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_from_cache(
            database_manager,
            template_cache_object,
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod eden_template_tests {
    use crate::comm::templates::delete::delete_template;
    use crate::comm::templates::patch::update_template;
    use crate::comm::templates::post::post_template;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::eden_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use database::methods::insert::template::InsertTemplate;
    use database::methods::update::UpdateActor;
    use eden_core::format::cache_uuid::TemplateCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheObjectType, CacheUuid, EdenId, EndpointUuid, OrganizationCacheUuid, TemplateId, UserUuid};
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_core::ep_core::database::schema::template::TemplateSchema;
    use endpoint_core::ep_core::database::template::UpdateTemplateSchema;
    use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
    use endpoint_core::ep_core::database::template::wrapper::TemplateValue;
    use endpoint_core::ep_core::database::template::{JsonTemplate, TemplateFields, TemplateKind};
    use serial_test::serial;
    use std::sync::Arc;

    fn template_registry() -> Arc<TemplateRegistry> {
        Arc::new(TemplateRegistry::new())
    }

    #[tokio::test]
    #[serial]
    async fn template_crud_test() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_, _, org_schema) = initialize_organization(&db_manager, test_telemetry).await;

        let org_uuid = org_schema.uuid();

        let template_schema = TemplateSchema::new(
            TemplateId::new("test_template".to_string()),
            JsonTemplate::new(
                EndpointUuid::new_uuid(),
                TemplateKind::Read,
                TemplateValue::from(serde_json::Value::default()),
                Vec::new(),
                EpKind::default(),
                None,
            )
            .expect("Expected to build a template schema"),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let template_registry = template_registry();

        // Post template
        assert!(
            post_template(
                &db_manager,
                InsertTemplate::new(org_uuid.clone(), template_schema.clone()),
                test_telemetry,
                template_schema.clone(),
                template_registry.clone()
            )
            .await
            .is_ok()
        );

        // Get template
        assert_eq!(
            template_registry
                .get(&template_schema.uuid(), test_telemetry)
                .await
                .expect("Failed to get template")
                .expect("Failed to unwrap template"),
            template_schema.template().to_owned()
        );

        let template_cache = CacheObjectType::new(
            Some(TemplateCacheUuid::new(
                Some(OrganizationCacheUuid::new(None, org_uuid.clone())),
                template_schema.uuid(),
            )),
            None,
        );

        // Update template
        assert!(
            update_template(
                &db_manager,
                &template_schema,
                &template_cache,
                UpdateActor::System("eden-service-test"),
                test_telemetry,
                template_registry.clone(),
                UpdateTemplateSchema::new(Some(TemplateId::new("new_test_template".to_string())), None, None, None,),
            )
            .await
            .is_ok()
        );

        // Render template
        let template = db_manager
            .render_template(&template_registry, &template_schema.uuid(), &org_uuid, &TemplateFields::default(), test_telemetry)
            .await
            .expect("Failed to render template");

        println!("Template: {:?}", template);

        //TODO DELETE
        assert!(delete_template(&db_manager, template_cache, test_telemetry,).await.is_ok());
    }
}
