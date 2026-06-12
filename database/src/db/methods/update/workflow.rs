#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod method_update {
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::workflow::insert_workflow::insert_workflow;
    use crate::methods::select::workflow::select_workflow::select_workflow_id;
    use crate::methods::update::method_update::setup;
    use crate::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, WorkflowCacheId};
    use eden_core::format::cache_uuid::WorkflowCacheUuid;
    use eden_core::format::{CacheObjectType, EdenId, EndpointId, WorkflowUuid};
    use eden_core::format::{TemplateUuid, WorkflowId};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::workflow::WorkflowSchema;
    use ep_core::database::workflow::Dag;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    #[tokio::test]
    async fn update_id() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert workflow
        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );
        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid.clone()), workflow_schema.id());

        assert_eq!(
            workflow_schema,
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id).await.expect("failed to select workflow")
        );

        // update workflow id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
            >>::update_id(
                &db_manager,
                &CacheObjectType::new(None, Some(workflow_cache_id.clone())),
                SqlQueries::UpdateWorkflowId,
                "new_test_workflow".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid), WorkflowId::new("new_test_workflow".to_string()));

        assert_eq!(
            WorkflowId::new("new_test_workflow".to_string()),
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id).await.expect("failed to select workflow").id()
        );
    }

    #[tokio::test]
    async fn update_description() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert workflow
        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );
        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid), workflow_schema.id());

        assert_eq!(
            Some("".to_string()),
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id)
                .await
                .expect("failed to select workflow")
                .description()
        );

        let workflow_cache_object = &CacheObjectType::new(None, Some(workflow_cache_id.clone()));

        // update workflow id
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            WorkflowSchema,
            WorkflowCacheUuid,
            WorkflowUuid,
            WorkflowCacheId,
            WorkflowId,
        >>::update_description(
            &db_manager,
            workflow_cache_object,
            SqlQueries::UpdateWorkflowDescription,
            "new sample description".to_string(),
            UpdateActor::System("infra-test"),
            test_telemetry,
        )
        .await
        .unwrap_or_default();

        assert_eq!(
            Some("new sample description".to_string()),
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id)
                .await
                .expect("failed to select workflow")
                .description()
        );
    }

    #[tokio::test]
    async fn update_workflow_id() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert workflow
        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );
        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid.clone()), workflow_schema.id());

        assert_eq!(
            workflow_schema,
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id).await.expect("failed to select workflow")
        );

        // update workflow id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
            >>::update_workflow_id(
                &db_manager,
                &CacheObjectType::new(None, Some(workflow_cache_id.clone())),
                "new_test_workflow".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid), WorkflowId::new("new_test_workflow".to_string()));

        assert_eq!(
            WorkflowId::new("new_test_workflow".to_string()),
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id).await.expect("failed to select workflow").id()
        );
    }

    #[tokio::test]
    async fn update_workflow_description() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert workflow
        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );
        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid), workflow_schema.id());

        assert_eq!(
            Some("".to_string()),
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id)
                .await
                .expect("failed to select workflow")
                .description()
        );

        let workflow_cache_object = &CacheObjectType::new(None, Some(workflow_cache_id.clone()));

        // update workflow id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
            >>::update_workflow_description(
                &db_manager,
                workflow_cache_object,
                "new sample description".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        assert_eq!(
            Some("new sample description".to_string()),
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id)
                .await
                .expect("failed to select workflow")
                .description()
        );
    }

    #[tokio::test]
    async fn update_workflow_dag() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert workflow
        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("Test Redis endpoint".to_string()),
            UserUuid::new_uuid(),
        );
        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        let workflow_cache_id = WorkflowCacheId::new(Some(org_cache_uuid), workflow_schema.id());

        assert!(select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id).await.is_ok());

        let workflow_cache_object = &CacheObjectType::new(None, Some(workflow_cache_id.clone()));

        let test_template_uuid = TemplateUuid::new_uuid();

        let mut dag = Dag::new();
        dag.add_node("test_node".to_string(), test_template_uuid.clone(), None, None, "simple description".to_string());

        // update workflow id
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            WorkflowSchema,
            WorkflowCacheUuid,
            WorkflowUuid,
            WorkflowCacheId,
            WorkflowId,
        >>::update_workflow_dag(&db_manager, workflow_cache_object, dag, UpdateActor::System("infra-test"), test_telemetry)
        .await
        .unwrap_or_default();

        assert_eq!(
            vec![test_template_uuid],
            select_workflow_id(&db_manager, test_telemetry, &workflow_cache_id)
                .await
                .expect("failed to select workflow")
                .dag()
                .get_templates()
        );
    }
}
