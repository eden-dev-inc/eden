// Re-export test utilities from database crate
pub use database::test_utils::clickhouse_test_utils;
pub use database::test_utils::database_test_utils;
pub use database::test_utils::redis_test_utils;
pub use database::test_utils::telemetry_test_utils;

// Eden-service specific test utilities (PG/Redis path only)
#[cfg(external_db)]
pub mod eden_test_utils {
    use crate::EdenDb;
    use crate::comm;
    use actix_web::web::Data;
    use actix_web::{App, HttpResponse, test, web};
    use database::db::lib::{
        CacheTtl, ClickhouseConn, ClickhouseDbConfig, DEFAULT_CLICKHOUSE_POOL_SIZE, DatabaseManager, PgConn, RedisConn,
    };
    use database::methods::insert::InsertMethod;
    use database::methods::insert::eden_node::InsertEdenNode;
    use database::methods::insert::organization::InsertOrganization;
    use function_name::named;

    use eden_core::auth::Password;
    use eden_core::format::cache_id::{EdenNodeCacheId, OrganizationCacheId};
    use eden_core::format::cache_uuid::EdenNodeCacheUuid;
    use eden_core::format::{EdenNodeId, EdenNodeUuid, EndpointUuid, OrganizationCacheUuid, OrganizationId, UserId};
    use eden_core::request::ServerData;
    use eden_core::telemetry::TelemetryWrapper;
    use eden_logger_internal::{ctx_with_trace, log_debug};
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_core::ep_core::database::schema::eden_node::EdenNodeSchema;
    use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
    use endpoint_core::ep_core::database::schema::user::UserSchema;
    use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
    use std::sync::Arc;

    #[allow(dead_code)]
    #[derive(Clone, Debug)]
    struct EdenClient {
        address: String,
        auth_token: String,
        url: String,
    }

    impl EdenClient {
        fn new(address: String, auth_token: String, url: String) -> Self {
            Self { address, auth_token, url }
        }
    }

    #[allow(dead_code)]
    #[named]
    async fn setup_test_app(
        new_org_token: Option<String>,
    ) -> impl actix_web::dev::Service<actix_http::Request, Response = actix_web::dev::ServiceResponse, Error = actix_web::Error> {
        let _ctx = ctx_with_trace!().with_feature("test");

        log_debug!(_ctx.clone(), "Init client", audience = eden_logger_internal::LogAudience::Internal);
        let engine_client = Arc::new(EdenClient::new("test_url".to_string(), "auth_token".to_string(), "example.com".to_string()));
        log_debug!(_ctx.clone(), "Starting engine pool", audience = eden_logger_internal::LogAudience::Internal);
        // let engine_pool = engine_client.client_pool().await.unwrap_or_default();
        // let engine_pool = Data::new(Pool::from(
        //     (0..64)
        //         .map(|_| Mutex::new(Wrapper(engine_pool.clone())))
        //         .collect::<Vec<Mutex<Wrapper>>>(),
        // ));
        log_debug!(_ctx.clone(), "Init databases", audience = eden_logger_internal::LogAudience::Internal);
        let clickhouse_config =
            ClickhouseDbConfig::new("http://localhost:8123".to_string(), None, None, None, DEFAULT_CLICKHOUSE_POOL_SIZE)
                .expect("Failed to build Clickhouse config");
        let database_manager = Data::new(
            DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(
                "redis://localhost",
                "postgres://postgres:password@localhost:5432/postgres",
                clickhouse_config,
                CacheTtl::from_secs(3600),
                None,
            )
            .await
            .expect("Failed to create test database manager"),
        );

        let templates_data = Data::new(TemplateRegistry::new());
        let server_data = Data::new(ServerData {
            engine_url: "test_url".to_string(),
            public_key: EdenNodeUuid::new_uuid(),
            new_org_token,
            tools_service_timeout_secs: None,
            internal_llm: None,
        });

        log_debug!(_ctx, "Init server", audience = eden_logger_internal::LogAudience::Internal);

        test::init_service(
            App::new()
                .app_data(server_data)
                .app_data(Data::new(engine_client))
                // .app_data(engine_pool)
                .app_data(templates_data)
                .app_data(database_manager)
                .route("/", web::get().to(HttpResponse::Ok))
                .service(
                    web::scope("/api/v1")
                        .route("/new", web::post().to(comm::organization::post::post))
                        .route("/org", web::post().to(comm::organization::post::post))
                        .route("/auth/login", web::post().to(comm::auth::login::login)),
                ),
        )
        .await
    }
    //
    // #[actix_web::test]
    // async fn test_health_check() {
    //     let app = setup_test_app().await;
    //     let req = test::TestRequest::get().uri("/").to_request();
    //     let resp = test::call_service(&app, req).await;
    //     assert!(resp.status().is_success());
    // }
    //
    // #[actix_web::test]
    // async fn test_create_org() {
    //     let app = setup_test_app().await;
    //     let payload = json!({
    //         "name": "test_org",
    //         "description": "Test Organization"
    //     });
    //
    //     let req = test::TestRequest::post()
    //         .uri("/api/v1/org")
    //         .set_json(&payload)
    //         .to_request();
    //
    //     let resp = test::call_service(&app, req).await;
    //     assert!(resp.status().is_success());
    // }
    //
    // #[actix_web::test]
    // async fn test_login() {
    //     let app = setup_test_app().await;
    //     let payload = json!({
    //         "username": "test_user",
    //         "password": "test_password"
    //     });
    //
    //     let req = test::TestRequest::post()
    //         .uri("/api/v1/auth/login")
    //         .set_json(&payload)
    //         .to_request();
    //
    //     let resp = test::call_service(&app, req).await;
    //     assert!(resp.status().is_success());
    // }

    // Add similar tests for other endpoints:
    // - test_get_org
    // - test_update_org
    // - test_delete_org
    // - test_create_user
    // - test_create_endpoint
    // etc.

    pub async fn insert_eden_node(
        db_manager: &EdenDb,
        test_telemetry: &mut TelemetryWrapper,
        eden_node_id: &str,
        endpoint_uuids: Vec<EndpointUuid>,
        info: serde_json::Value,
    ) -> EdenNodeSchema {
        let eden_node_uuid = EdenNodeUuid::new_uuid();
        let eden_node_schema = EdenNodeSchema::new(eden_node_id.to_string(), eden_node_uuid, endpoint_uuids, info);

        let insert_eden_node = InsertEdenNode::new(eden_node_schema.clone());

        <EdenDb as InsertMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeCacheId, InsertEdenNode>>::insert(
            db_manager,
            insert_eden_node,
            test_telemetry,
        )
        .await
        .expect("Failed to insert eden node");

        eden_node_schema
    }

    pub async fn insert_organization(
        db_manager: &EdenDb,
        test_telemetry: &mut TelemetryWrapper,
        organization_id: &str,
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

        <EdenDb as InsertMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationCacheId, InsertOrganization>>::insert(
            db_manager,
            insert_organization,
            test_telemetry,
        )
        .await
        .expect("Failed to insert organization");

        organization_schema
    }

    pub async fn initialize_organization(
        db_manager: &EdenDb,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema) {
        // create organization so we can test creating new endpoints
        let eden_node_schema = match db_manager.select_eden_node_id(&EdenNodeId::from("eden_node_test"), test_telemetry).await {
            Ok(en) => en,
            Err(_) => insert_eden_node(db_manager, test_telemetry, "eden_node_test", vec![], serde_json::Value::default()).await,
        };

        let organization_id: OrganizationId = "test_organization".into();

        let organization_schema =
            insert_organization(db_manager, test_telemetry, &organization_id, vec![eden_node_schema.uuid()], None).await;

        let admin_user_schema = UserSchema::new(
            UserId::from("username"),
            Password::new("password".to_string()),
            organization_schema.uuid(),
            None,
            None,
            None,
        );

        (admin_user_schema.clone(), eden_node_schema.clone(), organization_schema)
    }
}

#[cfg(embedded_db)]
pub mod eden_test_utils {
    use crate::EdenDb;
    use database::methods::insert::InsertMethod;
    use database::methods::insert::eden_node::InsertEdenNode;
    use database::methods::insert::organization::InsertOrganization;

    use eden_core::auth::Password;
    use eden_core::format::cache_id::{EdenNodeCacheId, OrganizationCacheId};
    use eden_core::format::cache_uuid::EdenNodeCacheUuid;
    use eden_core::format::{EdenNodeId, EdenNodeUuid, EndpointUuid, OrganizationCacheUuid, OrganizationId, UserId};
    use eden_core::telemetry::TelemetryWrapper;
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_core::ep_core::database::schema::eden_node::EdenNodeSchema;
    use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
    use endpoint_core::ep_core::database::schema::user::UserSchema;

    pub async fn insert_eden_node(
        db_manager: &EdenDb,
        test_telemetry: &mut TelemetryWrapper,
        eden_node_id: &str,
        endpoint_uuids: Vec<EndpointUuid>,
        info: serde_json::Value,
    ) -> EdenNodeSchema {
        let eden_node_uuid = EdenNodeUuid::new_uuid();
        let eden_node_schema = EdenNodeSchema::new(eden_node_id.to_string(), eden_node_uuid, endpoint_uuids, info);

        let insert_eden_node = InsertEdenNode::new(eden_node_schema.clone());

        <EdenDb as InsertMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeCacheId, InsertEdenNode>>::insert(
            db_manager,
            insert_eden_node,
            test_telemetry,
        )
        .await
        .expect("Failed to insert eden node");

        eden_node_schema
    }

    pub async fn insert_organization(
        db_manager: &EdenDb,
        test_telemetry: &mut TelemetryWrapper,
        organization_id: &str,
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

        <EdenDb as InsertMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationCacheId, InsertOrganization>>::insert(
            db_manager,
            insert_organization,
            test_telemetry,
        )
        .await
        .expect("Failed to insert organization");

        organization_schema
    }

    pub async fn initialize_organization(
        db_manager: &EdenDb,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema) {
        let eden_node_schema = match db_manager.select_eden_node_id(&EdenNodeId::from("eden_node_test"), test_telemetry).await {
            Ok(en) => en,
            Err(_) => insert_eden_node(db_manager, test_telemetry, "eden_node_test", vec![], serde_json::Value::default()).await,
        };

        let organization_id: OrganizationId = "test_organization".into();

        let organization_schema =
            insert_organization(db_manager, test_telemetry, &organization_id, vec![eden_node_schema.uuid()], None).await;

        let admin_user_schema = UserSchema::new(
            UserId::from("username"),
            Password::new("password".to_string()),
            organization_schema.uuid(),
            None,
            None,
            None,
        );

        (admin_user_schema.clone(), eden_node_schema.clone(), organization_schema)
    }
}

// Test utilities for Redis migration tests shared across crates.
cfg_if::cfg_if! {
    if #[cfg(any())] {
        pub mod redis_migrate_test_utils {}
    } else if #[cfg(any())] {
        pub mod redis_migrate_test_utils {
            use crate::EdenDb;
            use std::sync::Arc;

            use ep_runtime::comp::MyEngineService;
            use dashmap::DashMap;

            use database::methods::insert::InsertMethod;
            use database::methods::insert::endpoint::InsertEndpoint;
            use database::methods::insert::interlay::InsertInterlay;
            use database::test_utils::database_test_utils::create_database_manager;
            use database::test_utils::redis_test_utils::wait_for_redis_ready;
            use database::test_utils::telemetry_test_utils::test_telemetry;
            use eden_core::format::cache_id::InterlayCacheId;
            use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
            use eden_core::format::endpoint::EpKind;
            use eden_core::format::{CacheUuid, OrganizationCacheUuid, OrganizationUuid, UserUuid};
            use eden_core::telemetry::TelemetryWrapper;
            use eden_logger_internal::{ctx_with_trace, log_debug, log_error};
            use endpoint_schema::endpoint::EndpointSchema;
            use endpoint_core::ep_core::database::schema::Table;
            use endpoint_core::ep_core::database::schema::interlay::{InterlayMigration, InterlaySchema, InterlayState};
            use endpoint_core::ep_core::database::schema::organization::{MigrationUuid, OrganizationSchema};
            use endpoint_core::ep_core::settings::EdenSettings;
            use endpoint_core::redis_core::{RedisConfig, RedisConnection};
            use endpoints::endpoint::ep_redis::api::{IncrInputBuilder, RedisCommandInput};
            use endpoints::endpoint::ep_redis::ep::RedisEp;
            use endpoints::endpoint::ep_redis::protocol::decoder::DecoderRespFrame;
            use endpoints::endpoint::ep_redis::protocol::{RedisBytes, RedisProtocol};
            use endpoints::endpoint::{EP, protocol::EpProtocol};
            use function_name::named;
            use testcontainers_modules::redis::Redis;
            use testcontainers_modules::testcontainers::runners::AsyncRunner;
            use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
            use tokio::sync::{RwLock, broadcast};

            use super::eden_test_utils::initialize_organization;

    async fn initialize_redis() -> (ContainerAsync<Redis>, String, u16) {
        let container = Redis::default().with_tag("7-alpine").start().await.expect("Failed to start redis");

        wait_for_redis_ready(&container).await;

        let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");

        (container, "127.0.0.1".to_string(), host_port)
    }

    pub async fn connect_to_multi_redis(
        n: usize,
    ) -> (
        Vec<(ContainerAsync<Redis>, EndpointCacheUuid, EndpointSchema)>,
        Arc<MyEngineService>,
        EdenDb,
        OrganizationSchema,
        TelemetryWrapper,
    ) {
        let mut test_telemetry = test_telemetry();

        let database_manager = create_database_manager().await;

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&database_manager, &mut test_telemetry).await;

        let engine_service = Arc::new(MyEngineService::default());

        let mut endpoints: Vec<(ContainerAsync<Redis>, EndpointCacheUuid, EndpointSchema)> = vec![];
        for n in 0..n {
            let (container, host, port) = initialize_redis().await;

            let connection = RedisConnection {
                host,
                port: Some(port),
                tls: None,
                insecure: None,
                db: None,
                username: None,
                password: None,
                protocol_version: None,
                connect_timeout_secs: None,
                max_retries: None,
            };

            let (target, creds) = connection.split().expect("split connection");
            let redis_config = Box::new(RedisConfig {
                target,
                read_credentials: Some(creds.clone()),
                write_credentials: Some(creds),
                ..Default::default()
            });

            let endpoint_schema =
                EndpointSchema::new(format!("redis{n}").into(), EpKind::Redis, redis_config, None, None, UserUuid::new_uuid());

            let endpoint_cache_uuid =
                EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), endpoint_schema.uuid());

            let result = engine_service
                .connect(
                    &database_manager,
                    &InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid()),
                    &mut test_telemetry,
                )
                .await;

            assert!(result.is_ok(), "{:?}", result);

            endpoints.push((container, endpoint_cache_uuid, endpoint_schema));
        }

        (endpoints, engine_service, database_manager, organization_schema, test_telemetry)
    }

    #[named]
    pub async fn run_worker(
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        engine_service: Arc<MyEngineService>,
        endpoint_cache_uuid: EndpointCacheUuid,
        mut test_telemetry: TelemetryWrapper,
    ) -> i64 {
        let _ctx = ctx_with_trace!();
        let mut counter = 0;
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    log_debug!(_ctx, "Worker stopped");
                    return counter;
                },
                _ = async {
                    let incr_cmd =
                        IncrInputBuilder::default()
                            .key("counter".into())
                            .build()
                            .expect("Failed to build INCR")
                            .command();

                    let lock = engine_service.router.read().await;
                    let redis_ep = lock
                        .get(&EpKind::Redis)
                        .expect("failed to get redis ep")
                        .as_any()
                        .downcast_ref::<RedisEp>()
                        .expect("failed to get redis ep");

                    let response = redis_ep
                        .raw_bytes(
                            &endpoint_cache_uuid,
                            RedisBytes::new(incr_cmd),
                            EdenSettings::default(),
                            &mut test_telemetry,
                        )
                        .await
                        .expect("failed to get response");

                    if let Some((frame, _)) = RedisProtocol::decode_buffer(&response) {
                        counter = match frame {
                            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                                redis_protocol::resp2::types::OwnedFrame::Integer(n) => n,
                                _ => {log_error!(_ctx, "unexpected INCR response type"); 0},
                            },
                            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                                redis_protocol::resp3::types::OwnedFrame::Number{data, attributes:_} => data,
                                _ => {log_error!(_ctx, "unexpected INCR response type"); 0},
                            },
                        };
                    } else {
                        log_error!(_ctx, "incomplete RESP frame");
                    }
                } => {},
            }
        }
    }

    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_interlay(
        port: u16,
        origin_schema: EndpointSchema,
        origin_endpoint: EndpointCacheUuid,
        organization_schema: OrganizationSchema,
        engine_service: Arc<MyEngineService>,
        database_manager: Arc<EdenDb>,
        interlay_migration: Option<InterlayMigration>,
        mut test_telemetry: TelemetryWrapper,
    ) -> (
        InterlayCacheUuid,
        Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        Arc<DashMap<MigrationUuid, Arc<dyn EpMigrationState>>>,
        Arc<DashMap<MigrationUuid, Arc<RwLock<()>>>>,
    ) {
        let interlay_schema = InterlaySchema::new(
            "test_proxy".into(),
            None,
            origin_schema.uuid().clone(),
            port,
            None,
            None,
            #[cfg(any())]
            None,
            UserUuid::new_uuid(),
        );

        let organization_cache_uuid = OrganizationCacheUuid::new(None, organization_schema.uuid().clone());

        let interlay_cache_uuid = InterlayCacheUuid::new(Some(organization_cache_uuid.clone()), interlay_schema.uuid());

        <EdenDb as InsertMethod<InterlaySchema, InterlayCacheUuid, InterlayCacheId, InsertInterlay>>::insert(
            &database_manager,
            InsertInterlay::new(organization_cache_uuid.eden_uuid::<OrganizationUuid>().clone(), interlay_schema.clone()),
            &mut test_telemetry,
        )
        .await
        .expect("Failed to insert interlay");

        log::info!("Created interlay proxy on port {}", port);
        log::info!("  Initial routing to: {}", origin_schema.uuid());

        let interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>> = Arc::new(DashMap::new());

        let migration_states: Arc<DashMap<MigrationUuid, Arc<dyn EpMigrationState>>> = Arc::new(DashMap::new());
        let migration_lock: Arc<DashMap<MigrationUuid, Arc<RwLock<()>>>> = Arc::new(DashMap::new());

        let (interlay_signal_tx, interlay_signal_rx) = broadcast::channel(16);

        let mut interlay_state = InterlayState::new(
            origin_endpoint.clone(),
            EpKind::Redis,
            origin_schema.routing(),
            #[cfg(any())]
            interlay_migration,
            None,
            None,
            Default::default(),
        );
        interlay_state.set_signal_tx(interlay_signal_tx);

        interlay_endpoints.insert(interlay_cache_uuid.clone(), interlay_state);

        let listener = crate::comm::interlays::start::bind_interlay_listener(port).expect("Failed to bind interlay listener");

        tokio::spawn(crate::comm::interlays::start::start_interlay(
            listener,
            "default".to_string(),
            port,
            engine_service.clone(),
            database_manager.clone(),
            organization_cache_uuid,
            interlay_schema,
            interlay_endpoints.clone(),
            #[cfg(any())]
            migration_states.clone(),
            #[cfg(any())]
            migration_lock.clone(),
            interlay_signal_rx,
            test_telemetry.clone(),
            None,
        ));

        (interlay_cache_uuid, interlay_endpoints, migration_states, migration_lock)
    }

    pub async fn connect_to_interlay(
        port: u16,
        organization_schema: OrganizationSchema,
        engine_service: Arc<MyEngineService>,
        database_manager: Arc<EdenDb>,
        mut test_telemetry: TelemetryWrapper,
    ) -> EndpointCacheUuid {
        let connection = RedisConnection {
            host: "127.0.0.1".into(),
            port: Some(port),
            tls: None,
            insecure: None,
            db: None,
            username: None,
            password: None,
            protocol_version: Some(2),
            connect_timeout_secs: None,
            max_retries: None,
        };

        let (target, creds) = connection.split().expect("split connection");
        let redis_config = Box::new(RedisConfig {
            target,
            read_credentials: Some(creds.clone()),
            write_credentials: Some(creds),
            ..Default::default()
        });

        let endpoint_schema = EndpointSchema::new("redis-interlay".into(), EpKind::Redis, redis_config, None, None, UserUuid::new_uuid());

        let interlay_endpoint =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), endpoint_schema.uuid());

        let result = engine_service
            .connect(
                &database_manager,
                &InsertEndpoint::new(
                    organization_schema.uuid(),
                    endpoint_schema.clone(),
                    organization_schema.eden_node_uuids()[0].to_owned(),
                ),
                &mut test_telemetry,
            )
            .await;

        assert!(result.is_ok(), "{:?}", result);

        interlay_endpoint
    }
        }
    }
}
