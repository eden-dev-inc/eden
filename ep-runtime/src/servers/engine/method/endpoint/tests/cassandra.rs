pub mod read;
pub mod write;

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod cassandra_endpoint {
    use std::time::Duration;

    use crate::{comp::MyEngineService, test_utils::database_test_utils::initialize_database};
    use borsh::BorshSerialize;
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::{
        lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn},
        methods::insert::endpoint::InsertEndpoint,
    };
    use eden_core::format::OrganizationCacheUuid;
    use eden_core::{
        format::{EdenId, EdenNodeUuid, EndpointId, OrganizationUuid, UserUuid},
        telemetry::TelemetryWrapper,
    };
    use endpoint::{
        EpRequest,
        cassandra::{api::lib::QueryUnpagedInputBuilder, request::CassandraRequest},
    };
    use ep_core::database::schema::{Table, eden_node::EdenNodeSchema, organization::OrganizationSchema, user::UserSchema};
    use {
        cassandra_core::config::CassandraConfig,
        ep_core::ep::{EpConfig, EpConnection},
    };
    use {cassandra_core::connection::CassandraConnection, ep_core::settings::EdenSettings};

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

    pub async fn initialize_cassandra(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        connection: Box<CassandraConnection>,
        engine_service: &MyEngineService,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema, EndpointSchema) {
        let (user_schema, eden_node_schema, organization_schema) = initialize_database(db_manager, test_telemetry).await;

        // wait for Cassandra pool to get up
        println!("Waiting for Cassandra to become available...60 seconds");
        tokio::time::sleep(Duration::from_secs(60)).await;

        let endpoint_schema = connect::<CassandraConfig>(
            test_telemetry,
            db_manager,
            engine_service.clone(),
            &mut CassandraConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        const CMDS: [&str; 4] = [
            "CREATE KEYSPACE Eden WITH replication={'class': 'SimpleStrategy', 'replication_factor' : 1};",
            "CREATE TABLE Eden.x(n int, name text, created_at timeuuid primary key, ticks list<timestamp>);",
            "INSERT INTO Eden.x(n, name, created_at, ticks) VALUES(42, 'Answer', now(), [toTimestamp(now()), toTimestamp(now())]);",
            "INSERT INTO Eden.x(n, name, created_at, ticks) VALUES(7, 'Question', now(), [toTimestamp(now()), toTimestamp(now())]);",
        ];
        for cmd in CMDS {
            let mut write = Box::new(CassandraRequest(Box::new(
                QueryUnpagedInputBuilder::default().query(cmd).build().unwrap_or_default(),
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
        }

        (user_schema, eden_node_schema, organization_schema, endpoint_schema)
    }
}
