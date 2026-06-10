pub mod read;
pub mod write;

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod postgres_endpoint {
    use crate::{comp::MyEngineService, test_utils::database_test_utils::initialize_database};
    use borsh::BorshSerialize;
    use database::{
        lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn},
        methods::insert::endpoint::InsertEndpoint,
    };

    use database::endpoint_schema::endpoint::EndpointSchema;
    use eden_core::format::OrganizationCacheUuid;
    use eden_core::{
        format::{EdenId, EdenNodeUuid, EndpointId, OrganizationUuid, UserUuid},
        telemetry::TelemetryWrapper,
    };
    use endpoint::{
        EpRequest,
        postgres::{api::lib::batch_execute::BatchExecuteInputBuilder, request::PostgresRequest},
    };
    use ep_core::database::schema::{Table, eden_node::EdenNodeSchema, organization::OrganizationSchema, user::UserSchema};
    use {
        ep_core::{
            ep::{EpConfig, EpConnection},
            settings::EdenSettings,
        },
        postgres_core::{PostgresConfig, connection::PostgresConnection},
    };

    pub(crate) async fn connect<C: EpConfig + BorshSerialize>(
        test_telemetry: &mut TelemetryWrapper,
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
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

        engine_service.connect(db_manager, &insert_endpoint, test_telemetry).await.expect("Failed to connect to database");

        endpoint_schema
    }

    pub async fn initialize_postgres(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        connection: Box<PostgresConnection>,
        engine_service: &MyEngineService,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema, EndpointSchema) {
        let (user_schema, eden_node_schema, organization_schema) = initialize_database(db_manager, test_telemetry).await;

        let endpoint_schema = connect::<PostgresConfig>(
            test_telemetry,
            db_manager,
            engine_service.clone(),
            &mut PostgresConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let mut write = Box::new(PostgresRequest(Box::new(
            BatchExecuteInputBuilder::default()
                .query(
                    " \
                        CREATE TABLE test_table (id integer PRIMARY KEY, name text, age integer); \
                        INSERT INTO test_table VALUES (1, 'Alice', 25), (2, 'Bob', 27); \
                        ",
                )
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

        engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        (user_schema, eden_node_schema, organization_schema, endpoint_schema)
    }
}
