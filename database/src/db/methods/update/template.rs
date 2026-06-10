#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod method_update {
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::template::insert_template::insert_template;
    use crate::methods::select::template::select_template::select_template_id;
    use crate::methods::update::method_update::setup;
    use crate::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
    use crate::template::TemplateKind;
    use eden_core::format::cache_id::{CacheId, TemplateCacheId};
    use eden_core::format::cache_uuid::TemplateCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheObjectType, EdenId, EndpointUuid};
    use eden_core::format::{TemplateId, TemplateUuid};
    use ep_core::database::schema::Table;
    use ep_core::database::schema::template::TemplateSchema;
    use ep_core::database::template::JsonTemplate;
    use ep_core::database::template::wrapper::TemplateValue;

    #[tokio::test]
    async fn update_id() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;
        let test_telemetry = &mut test_telemetry;

        // insert template
        let template_schema =
            insert_template(&db_manager, test_telemetry, EndpointUuid::new_uuid(), organization_schema.uuid(), "test_template")
                .await
                .expect("Insert failed");

        let template_cache_id = &TemplateCacheId::new(Some(org_cache_uuid.clone()), template_schema.id());

        assert_eq!(
            template_schema,
            select_template_id(&db_manager, test_telemetry, template_cache_id).await.expect("failed to select template")
        );

        // update template id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                TemplateSchema,
                TemplateCacheUuid,
                TemplateUuid,
                TemplateCacheId,
                TemplateId,
            >>::update_id(
                &db_manager,
                &CacheObjectType::new(None, Some(template_cache_id.clone())),
                SqlQueries::UpdateTemplateId,
                "new_test_template".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let template_cache_id = &TemplateCacheId::new(Some(org_cache_uuid), TemplateId::new("new_test_template".to_string()));

        assert_eq!(
            TemplateId::new("new_test_template".to_string()),
            select_template_id(&db_manager, test_telemetry, template_cache_id).await.expect("failed to select template").id()
        );
    }

    #[tokio::test]
    async fn update_description() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert template
        let template_schema =
            insert_template(&db_manager, test_telemetry, EndpointUuid::new_uuid(), organization_schema.uuid(), "test_template")
                .await
                .expect("Insert failed");

        let template_cache_id = TemplateCacheId::new(Some(org_cache_uuid), template_schema.id());

        assert_eq!(
            Some("sample description".to_string()),
            select_template_id(&db_manager, test_telemetry, &template_cache_id)
                .await
                .expect("failed to select template")
                .description()
        );

        let template_cache_object = &CacheObjectType::new(None, Some(template_cache_id.clone()));

        // update template id
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
        >>::update_description(
            &db_manager,
            template_cache_object,
            SqlQueries::UpdateTemplateDescription,
            "new sample description".to_string(),
            UpdateActor::System("infra-test"),
            test_telemetry,
        )
        .await
        .expect("Update failed");

        assert_eq!(
            Some("new sample description".to_string()),
            select_template_id(&db_manager, test_telemetry, &template_cache_id)
                .await
                .expect("failed to select template")
                .description()
        );
    }

    #[tokio::test]
    async fn update_template_id() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert template
        let template_schema =
            insert_template(&db_manager, test_telemetry, EndpointUuid::new_uuid(), organization_schema.uuid(), "test_template")
                .await
                .expect("Insert failed");

        let template_cache_id = &TemplateCacheId::new(Some(org_cache_uuid.clone()), template_schema.id());

        assert_eq!(
            template_schema,
            select_template_id(&db_manager, test_telemetry, template_cache_id).await.expect("failed to select template")
        );

        // update template id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                TemplateSchema,
                TemplateCacheUuid,
                TemplateUuid,
                TemplateCacheId,
                TemplateId,
            >>::update_template_id(
                &db_manager,
                &CacheObjectType::new(None, Some(template_cache_id.clone())),
                "new_test_template".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let template_cache_id = TemplateCacheId::new(Some(org_cache_uuid), TemplateId::new("new_test_template".to_string()));

        assert_eq!(
            TemplateId::new("new_test_template".to_string()),
            select_template_id(&db_manager, test_telemetry, &template_cache_id).await.expect("failed to select template").id()
        );
    }

    #[tokio::test]
    async fn update_template_description() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert template
        let template_schema =
            insert_template(&db_manager, test_telemetry, EndpointUuid::new_uuid(), organization_schema.uuid(), "test_template")
                .await
                .expect("Insert failed");

        let template_cache_id = &TemplateCacheId::new(Some(org_cache_uuid), template_schema.id());

        assert_eq!(
            Some("sample description".to_string()),
            select_template_id(&db_manager, test_telemetry, template_cache_id)
                .await
                .expect("failed to select template")
                .description()
        );

        let template_cache_object = &CacheObjectType::new(None, Some(template_cache_id.clone()));

        // update template id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                TemplateSchema,
                TemplateCacheUuid,
                TemplateUuid,
                TemplateCacheId,
                TemplateId,
            >>::update_template_description(
                &db_manager,
                template_cache_object,
                "new sample description".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        assert_eq!(
            Some("new sample description".to_string()),
            select_template_id(&db_manager, test_telemetry, template_cache_id)
                .await
                .expect("failed to select template")
                .description()
        );
    }

    #[tokio::test]
    async fn update_template_template() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert template
        let template_schema =
            insert_template(&db_manager, test_telemetry, EndpointUuid::new_uuid(), organization_schema.uuid(), "test_template")
                .await
                .expect("Insert failed");

        let template_cache_id = &TemplateCacheId::new(Some(org_cache_uuid), template_schema.id());

        assert_eq!(
            &TemplateKind::Read,
            select_template_id(&db_manager, test_telemetry, template_cache_id)
                .await
                .expect("failed to select template")
                .template()
                .kind()
        );

        let template_cache_object = &CacheObjectType::new(None, Some(template_cache_id.clone()));

        let new_template = JsonTemplate::new(
            EndpointUuid::new_uuid(),
            TemplateKind::Read,
            TemplateValue::new(serde_json::Value::default()),
            Vec::new(),
            EpKind::default(),
            None,
        )
        .expect("Failed to create template");

        // update template id
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
        >>::update_template_template(
            &db_manager, template_cache_object, new_template, UpdateActor::System("infra-test"), test_telemetry
        )
        .await
        .expect("Failed to update template");

        assert_eq!(
            &TemplateKind::Read,
            select_template_id(&db_manager, test_telemetry, template_cache_id)
                .await
                .expect("failed to select template")
                .template()
                .kind()
        );
    }
}
