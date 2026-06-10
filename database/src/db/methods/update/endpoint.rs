#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod method_update {
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::methods::insert::endpoint::InsertEndpoint;
    use crate::methods::insert::endpoint::tests::insert_endpoint;
    use crate::methods::select::endpoint::select_endpoint::select_endpoint_id;
    use crate::methods::update::method_update::setup;
    use crate::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
    use eden_core::format::EndpointId;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::EndpointCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheObjectType, EdenId, EndpointUuid};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::ep::EpConfig;
    use postgres_core::config::PostgresConfig;
    use redis_core::config::RedisConfig;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn update_id() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert endpoint
        let endpoint_schema = insert_endpoint(
            &db_manager,
            test_telemetry,
            "test_endpoint",
            EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid.clone()), endpoint_schema.id());

        assert_eq!(
            endpoint_schema,
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id).await.expect("failed to select endpoint")
        );

        // update endpoint id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::update_id(
                &db_manager,
                &CacheObjectType::new(None, Some(endpoint_cache_id.clone())),
                SqlQueries::UpdateEndpointId,
                "new_test_endpoint".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid), EndpointId::new("new_test_endpoint".to_string()));

        assert_eq!(
            EndpointId::new("new_test_endpoint".to_string()),
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id).await.expect("failed to select endpoint").id()
        );
    }

    #[tokio::test]
    #[serial]
    async fn update_description() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert endpoint
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_endpoint".to_string()),
            EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("sample description".to_string()),
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointCacheId,
            InsertEndpoint,
        >>::insert(&db_manager, insert_endpoint, test_telemetry)
        .await
        .expect("Failed to insert");

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid), endpoint_schema.id());

        assert_eq!(
            Some("sample description".to_string()),
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id)
                .await
                .expect("failed to select endpoint")
                .description()
        );

        let endpoint_cache_object = &CacheObjectType::new(None, Some(endpoint_cache_id.clone()));

        // update endpoint id
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::update_description(
            &db_manager,
            endpoint_cache_object,
            SqlQueries::UpdateEndpointDescription,
            "new sample description".to_string(),
            UpdateActor::System("infra-test"),
            test_telemetry,
        )
        .await
        .unwrap_or_default();

        assert_eq!(
            Some("new sample description".to_string()),
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id)
                .await
                .expect("failed to select endpoint")
                .description()
        );
    }

    #[tokio::test]
    #[serial]
    async fn update_endpoint_id() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert endpoint
        let endpoint_schema = insert_endpoint(
            &db_manager,
            test_telemetry,
            "test_endpoint",
            EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid.clone()), endpoint_schema.id());

        assert_eq!(
            endpoint_schema,
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id).await.expect("failed to select endpoint")
        );

        // update endpoint id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::update_endpoint_id(
                &db_manager,
                &CacheObjectType::new(None, Some(endpoint_cache_id.clone())),
                "new_test_endpoint".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid), EndpointId::new("new_test_endpoint".to_string()));

        assert_eq!(
            EndpointId::new("new_test_endpoint".to_string()),
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id).await.expect("failed to select endpoint").id()
        );
    }

    #[tokio::test]
    #[serial]
    async fn update_endpoint_description() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert endpoint
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_endpoint".to_string()),
            EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            Some("sample description".to_string()),
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointCacheId,
            InsertEndpoint,
        >>::insert(&db_manager, insert_endpoint, test_telemetry)
        .await
        .expect("Failed to insert");

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid), endpoint_schema.id());

        assert_eq!(
            Some("sample description".to_string()),
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id)
                .await
                .expect("failed to select endpoint")
                .description()
        );

        let endpoint_cache_object = &CacheObjectType::new(None, Some(endpoint_cache_id.clone()));

        // update endpoint id
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::update_endpoint_description(
                &db_manager,
                endpoint_cache_object,
                "new sample description".to_string(),
                UpdateActor::System("infra-test"),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        assert_eq!(
            Some("new sample description".to_string()),
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id)
                .await
                .expect("failed to select endpoint")
                .description()
        );
    }

    #[tokio::test]
    #[serial]
    async fn update_endpoint_config() {
        // start containers
        let (db_manager, mut test_telemetry, _user_schema, eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert endpoint
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_endpoint".to_string()),
            EpKind::Postgres,
            PostgresConfig::default().as_config(),
            None,
            Some("sample description".to_string()),
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointCacheId,
            InsertEndpoint,
        >>::insert(&db_manager, insert_endpoint, test_telemetry)
        .await
        .expect("Failed to insert");

        let endpoint_cache_id = EndpointCacheId::new(Some(org_cache_uuid), endpoint_schema.id());

        select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id).await.expect("failed to select endpoint");

        let endpoint_cache_object = &CacheObjectType::new(None, Some(endpoint_cache_id.clone()));

        // update endpoint id
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::update_endpoint_config(
            &db_manager,
            endpoint_cache_object,
            PostgresConfig::default().as_config(),
            UpdateActor::System("infra-test"),
            test_telemetry,
        )
        .await
        .unwrap_or_default();

        assert_eq!(
            EpKind::Postgres,
            select_endpoint_id(&db_manager, test_telemetry, &endpoint_cache_id)
                .await
                .expect("failed to select endpoint")
                .config()
                .kind()
        );
    }
}
