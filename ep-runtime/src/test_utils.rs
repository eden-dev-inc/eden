// Re-export test utilities from database crate
pub use database::test_utils::{clickhouse_test_utils, redis_test_utils, telemetry_test_utils};

// Communication-specific test utilities
#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod database_test_utils {
    use crate::comp::MyEngineService;
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use database::methods::insert::InsertMethod;
    use database::methods::insert::eden_node::InsertEdenNode;
    use database::methods::insert::endpoint::InsertEndpoint;
    use database::methods::insert::organization::InsertOrganization;
    use database::methods::insert::user::InsertUser;
    use eden_core::auth::Password;
    use eden_core::format::cache_id::{EdenNodeCacheId, EndpointCacheId, OrganizationCacheId, UserCacheId};
    use eden_core::format::cache_uuid::{EdenNodeCacheUuid, EndpointCacheUuid, UserCacheUuid};
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{
        EdenId, EdenNodeId, EdenNodeUuid, EndpointId, EndpointUuid, OrganizationCacheUuid, OrganizationId, OrganizationUuid, UserId,
        UserUuid,
    };
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::eden_node::EdenNodeSchema;
    use ep_core::database::schema::organization::OrganizationSchema;
    use ep_core::database::schema::user::UserSchema;
    use ep_core::ep::EpConfig;
    use testcontainers_modules::clickhouse::ClickHouse;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::redis::Redis;
    use testcontainers_modules::testcontainers::ContainerAsync;

    pub enum TestContainer {
        Local,
        Redis(ContainerAsync<Redis>),
        Postgres(ContainerAsync<Postgres>),
        Clickhouse(ContainerAsync<ClickHouse>),
    }

    impl TestContainer {
        pub async fn stop(self) -> std::io::Result<()> {
            match self {
                Self::Local => Ok(()),
                Self::Redis(container) => container.stop().await.map_err(|e| std::io::Error::other(e.to_string())),
                Self::Postgres(container) => container.stop().await.map_err(|e| std::io::Error::other(e.to_string())),
                Self::Clickhouse(container) => container.stop().await.map_err(|e| std::io::Error::other(e.to_string())),
            }
        }
    }

    trait IntoTestContainer {
        fn into_test_container(self) -> TestContainer;
    }

    impl IntoTestContainer for () {
        fn into_test_container(self) -> TestContainer {
            TestContainer::Local
        }
    }

    impl IntoTestContainer for ContainerAsync<Redis> {
        fn into_test_container(self) -> TestContainer {
            TestContainer::Redis(self)
        }
    }

    impl IntoTestContainer for ContainerAsync<Postgres> {
        fn into_test_container(self) -> TestContainer {
            TestContainer::Postgres(self)
        }
    }

    impl IntoTestContainer for ContainerAsync<ClickHouse> {
        fn into_test_container(self) -> TestContainer {
            TestContainer::Clickhouse(self)
        }
    }

    pub async fn initialize_engine_service() -> (
        TestContainer,
        TestContainer,
        TestContainer,
        DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        MyEngineService,
    ) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (redis_container, pg_container, clickhouse_container, db_manager) =
            database::test_utils::database_test_utils::create_database_manager_dedicated().await;

        (
            redis_container.into_test_container(),
            pg_container.into_test_container(),
            clickhouse_container.into_test_container(),
            db_manager,
            MyEngineService::default(),
        )
    }

    pub async fn insert_eden_node(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        eden_node_id: &str,
        endpoint_uuids: Vec<EndpointUuid>,
        info: serde_json::Value,
    ) -> EdenNodeSchema {
        let eden_node_uuid = EdenNodeUuid::new_uuid();
        let eden_node_schema = EdenNodeSchema::new(eden_node_id.to_string(), eden_node_uuid, endpoint_uuids, info);

        let insert_eden_node = InsertEdenNode::new(eden_node_schema.clone());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeCacheId,
            InsertEdenNode,
        >>::insert(db_manager, insert_eden_node, test_telemetry)
        .await
        .expect("Failed to insert eden node");

        eden_node_schema
    }

    /// test insert for organizations
    pub async fn insert_organization(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        organization_id: &str,
        super_admins: &[(UserId, Password)],
        eden_node_uuids: Vec<EdenNodeUuid>,
        description: Option<String>,
    ) -> OrganizationSchema {
        let organization_schema = OrganizationSchema::new(
            organization_id.to_string(),
            None,
            eden_node_uuids.iter().map(|u| (format!("eden_node_{}", u.to_string().split_at(4).0).into(), u.clone())).collect(),
            description,
        );

        let insert_organization = InsertOrganization::new(organization_schema.clone());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationCacheId,
            InsertOrganization,
        >>::insert(db_manager, insert_organization, test_telemetry)
        .await
        .expect("Failed to insert organization");

        for (user_id, password) in super_admins {
            let user_schema = UserSchema::new(user_id.clone(), password.clone(), organization_schema.uuid(), None, None, None);
            let insert_user = InsertUser::new(user_schema);
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
                UserSchema,
                UserCacheUuid,
                UserCacheId,
                InsertUser,
            >>::insert(db_manager, insert_user, test_telemetry)
            .await
            .unwrap_or_else(|_| panic!("Failed to insert user {user_id}"));
        }

        organization_schema
    }

    /// test insert for endpoints
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub(crate) async fn insert_endpoint(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        endpoint_id: &str,
        ep_kind: EpKind,
        config: Box<dyn EpConfig>,
        description: Option<String>,
        organization_uuid: OrganizationUuid,
        eden_node_uuid: EdenNodeUuid,
    ) -> EndpointSchema {
        let endpoint_schema =
            EndpointSchema::new(EndpointId::new(endpoint_id.to_string()), ep_kind, config, None, description, UserUuid::new_uuid());

        let insert_endpoint = InsertEndpoint::new(organization_uuid, endpoint_schema.clone(), eden_node_uuid);

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointCacheId,
            InsertEndpoint,
        >>::insert(db_manager, insert_endpoint, test_telemetry)
        .await
        .expect("Failed to insert endpoint");

        endpoint_schema
    }

    pub(crate) async fn initialize_database(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema) {
        // create organization so we can test creating new endpoints
        let eden_node_schema = match db_manager.select_eden_node_id(&EdenNodeId::from("eden_node_test"), test_telemetry).await {
            Ok(en) => en,
            Err(_) => insert_eden_node(db_manager, test_telemetry, "eden_node_test", vec![], serde_json::Value::default()).await,
        };

        let organization_id: OrganizationId = "test_organization".into();

        let user_names_and_passwords = (UserId::from("username"), Password::new("password".to_string()));

        let organization_schema = insert_organization(
            db_manager,
            test_telemetry,
            &organization_id,
            std::slice::from_ref(&user_names_and_passwords),
            vec![eden_node_schema.uuid()],
            None,
        )
        .await;

        let admin_user_schema =
            UserSchema::new(user_names_and_passwords.0, user_names_and_passwords.1, organization_schema.uuid(), None, None, None);

        (admin_user_schema.clone(), eden_node_schema.clone(), organization_schema)
    }
}

// #[cfg(all(test, feature = "infra-tests"))]
// pub(crate) mod telemetry_test_utils {
//     use actix_web::web::Data;
//     use eden_core::format::EdenNodeUuid;
//     use eden_core::telemetry::labels::TelemetryLabels;
//     use eden_core::telemetry::{AllMetrics, TelemetryDurations, TelemetryWrapper, setup_metrics};
//     use std::sync::Arc;
//
//     pub fn test_telemetry() -> TelemetryWrapper {
//         TelemetryWrapper::new(
//             Arc::new(setup_metrics("http://localhost:4317", "").expect("Failed to setup metrics")),
//             TelemetryLabels::new(&EdenNodeUuid::new_uuid()),
//             TelemetryDurations::default(),
//         )
//     }
//
//     pub fn test_metrics() -> Data<AllMetrics> {
//         Data::new(setup_metrics("http://localhost:4317", "").expect("Failed to setup metrics"))
//     }
// }
