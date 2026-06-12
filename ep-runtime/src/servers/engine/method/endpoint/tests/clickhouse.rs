pub mod read;
pub mod write;

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod clickhouse_endpoint {
    use crate::{comp::MyEngineService, test_utils::database_test_utils::initialize_database};
    use borsh::BorshSerialize;
    #[cfg(feature = "clickhouse")]
    use clickhouse_core::connection::ClickhouseConnection;
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::{
        lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn},
        methods::insert::endpoint::InsertEndpoint,
    };
    use eden_core::{
        format::{EdenId, EdenNodeUuid, EndpointId, OrganizationUuid, UserUuid},
        telemetry::TelemetryWrapper,
    };
    use ep_core::database::schema::{Table, eden_node::EdenNodeSchema, organization::OrganizationSchema, user::UserSchema};
    use ep_core::ep::{EpConfig, EpConnection};

    pub async fn initialize_clickhouse(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        connection: Box<ClickhouseConnection>,
        engine_service: &MyEngineService,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema, EndpointSchema) {
        #[cfg(feature = "clickhouse")]
        use clickhouse_core::config::ClickhouseConfig;
        let (user_schema, eden_node_schema, organization_schema) = initialize_database(db_manager, test_telemetry).await;
        let endpoint_schema = connect::<ClickhouseConfig>(
            test_telemetry,
            db_manager,
            engine_service.clone(),
            &mut ClickhouseConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;
        (user_schema, eden_node_schema, organization_schema, endpoint_schema)
    }
    pub(crate) async fn connect<C: EpConfig + BorshSerialize>(
        test_telemetry: &mut TelemetryWrapper,
        database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        engine_service: MyEngineService,
        config: &mut C,
        connection: Box<dyn EpConnection>,
        organization_uuid: OrganizationUuid,
        eden_node_uuid: EdenNodeUuid,
    ) -> EndpointSchema {
        let kind = connection.kind();
        assert!(config.update_write_conn(connection).is_ok());
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new(format!("test_{}", kind.clone())),
            kind,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );
        let insert_endpoint = InsertEndpoint::new(organization_uuid, endpoint_schema.clone(), eden_node_uuid);
        engine_service.connect(database, &insert_endpoint, test_telemetry).await.expect("Failed to connect to database");
        endpoint_schema
    }
}
