pub mod delete;
pub mod get;
pub mod patch;
pub mod post;

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod eden_workflow_tests {
    use crate::comm::workflows::delete::delete_workflow;
    use crate::comm::workflows::get::get_workflow;
    use crate::comm::workflows::patch::update_workflow;
    use crate::comm::workflows::post::post_workflow;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::eden_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use database::methods::insert::workflow::InsertWorkflow;
    use database::methods::update::UpdateActor;
    use eden_core::format::cache_uuid::WorkflowCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, EdenId, OrganizationCacheUuid, UserUuid, WorkflowId};
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_core::ep_core::database::schema::workflow::{UpdateWorkflowSchema, WorkflowSchema};
    use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
    use endpoint_core::ep_core::database::workflow::{Dag, Workflow};
    use std::sync::Arc;

    #[allow(dead_code)]
    fn template_registry() -> Arc<TemplateRegistry> {
        Arc::new(TemplateRegistry::new())
    }

    #[tokio::test]
    async fn workflow_crud_test() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_, _, org_schema) = initialize_organization(&db_manager, test_telemetry).await;

        let org_uuid = org_schema.uuid();

        let workflow_schema =
            WorkflowSchema::new(Workflow::new(WorkflowId::new("test_workflow".to_string()), Dag::new(), None), UserUuid::new_uuid());

        let workflow_cache = CacheObjectType::new(
            Some(WorkflowCacheUuid::new(
                Some(OrganizationCacheUuid::new(None, org_uuid.clone())),
                workflow_schema.uuid(),
            )),
            None,
        );

        // Post template
        post_workflow(&db_manager, InsertWorkflow::new(org_uuid.clone(), workflow_schema), test_telemetry)
            .await
            .expect("Failed to post workflow");

        // Get workflow
        assert!(get_workflow(&db_manager, &workflow_cache, test_telemetry).await.is_ok());

        assert!(
            update_workflow(
                &db_manager,
                &workflow_cache,
                UpdateActor::System("eden-service-test"),
                test_telemetry,
                UpdateWorkflowSchema::new(Some(WorkflowId::new("new_test_workflow".to_string())), None, None,)
            )
            .await
            .is_ok()
        );

        assert!(delete_workflow(&db_manager, workflow_cache, test_telemetry).await.is_ok());
    }
}
