use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use chrono::Utc;
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{InterlayId, InterlayUuid, OrganizationUuid, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::FromRow;
use ep_core::database::schema::interlay::{InterlaySchema, InterlaySchemaIds, InterlaySettings};

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select interlay
    pub async fn select_interlay_uuid<T>(&self, interlay_uuid: &InterlayUuid, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "interlay/interlay_uuid"), &[interlay_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))
    }

    /// Select interlay
    pub async fn select_interlay_id<T>(&self, interlay_id: &InterlayId, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "interlay/interlay_id"), &[interlay_id])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))
    }

    pub async fn select_all_interlays_ids(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<InterlaySchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "interlay/interlays"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?
        {
            schemas.push(InterlaySchemaIds::from(
                decode_schema_row::<InterlaySchema>(row).map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?,
            ));
        }

        Ok(schemas)
    }

    pub async fn select_all_interlays(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<InterlaySchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "interlay/interlays"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?);
        }

        Ok(schemas)
    }

    pub async fn select_all_interlays_ids_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<InterlaySchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "interlay/interlays_ids_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?
        {
            schemas.push(InterlaySchemaIds::from(
                decode_schema_row::<InterlaySchema>(row).map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?,
            ));
        }

        Ok(schemas)
    }

    pub async fn select_all_interlays_updated(
        &self,
        org_uuid: &OrganizationUuid,
        timestamp: &DateTimeWrapper,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<InterlaySchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "interlay/interlays_updated"), &[org_uuid, timestamp])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?);
        }

        Ok(schemas)
    }

    /// Select an interlay by port (across all organizations)
    pub async fn select_interlay_by_port(&self, port: i32, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<InterlaySchema>> {
        let ports = [port];
        self.select_interlay_by_ports(&ports, telemetry_wrapper).await
    }

    /// Select an interlay by any port (across all organizations)
    pub async fn select_interlay_by_ports(
        &self,
        ports: &[i32],
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<InterlaySchema>> {
        if ports.is_empty() {
            return Ok(None);
        }

        let conn = self.pg_connection().await?;

        #[cfg(embedded_db)]
        {
            const SELECT_INTERLAY_BY_PORT_SQLITE: &str = r#"
                SELECT a.*
                FROM interlays a
                WHERE a.port = $1
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(COALESCE(a.listeners, '[]')) AS listener
                       WHERE CAST(json_extract(listener.value, '$.bind_port') AS INTEGER) = $1
                   )
                LIMIT 1
            "#;

            for port in ports {
                if let Some(row) = conn
                    .query_opt(SELECT_INTERLAY_BY_PORT_SQLITE, &[port])
                    .await
                    .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?
                {
                    return Ok(Some(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?));
                }
            }

            Ok(None)
        }

        #[cfg(not(embedded_db))]
        match conn
            .query_opt(sql_file!("select", "interlay/interlay_by_ports"), &[&ports])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?
        {
            Some(row) => Ok(Some(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?)),
            None => Ok(None),
        }
    }

    /// Update only metadata fields (description, settings) for an interlay.
    /// This avoids a full upsert that could clobber concurrent port/tls/endpoint changes.
    /// Pass `None` for a field to leave it unchanged in the database.
    pub async fn update_interlay_metadata(
        &self,
        interlay_uuid: &InterlayUuid,
        description: Option<&str>,
        settings: Option<&InterlaySettings>,
        updated_by: &UserUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let now = Utc::now();
        let settings_json = settings.map(|s| serde_json::to_value(s).unwrap_or_default());
        self.pg_connection()
            .await?
            .execute(
                sql_file!("update", "interlay_metadata"),
                &[interlay_uuid, &description, &settings_json, updated_by, &now],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod tests {
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::methods::insert::endpoint::tests::insert_endpoint;
    use crate::methods::insert::interlay::InsertInterlay;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::InterlayUuid;
    use eden_core::format::cache_id::InterlayCacheId;
    use eden_core::format::cache_uuid::InterlayCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::interlay::InterlaySchema;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;
    use serial_test::serial;

    /// Verify that concurrent port update and metadata-only update don't clobber
    /// each other: the final state must have both the new port AND new description.
    #[tokio::test]
    #[serial]
    async fn update_interlay_metadata_does_not_clobber_port() {
        let db = std::sync::Arc::new(create_database_manager().await);
        let mut test_telemetry = test_telemetry();
        let telemetry = &mut test_telemetry;

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db, telemetry).await;

        let endpoint_schema = insert_endpoint(
            &db,
            telemetry,
            "test_endpoint",
            EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let interlay_schema = InterlaySchema::new(
            "test_interlay".to_string(),
            Some("original description".to_string()),
            endpoint_schema.uuid(),
            9000,
            None,
            None,
            None,
            _user_schema.uuid(),
        );

        let insert = InsertInterlay::new(organization_schema.uuid(), interlay_schema.clone());
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            InterlaySchema,
            InterlayCacheUuid,
            InterlayCacheId,
            InsertInterlay,
        >>::insert(&*db, insert, telemetry)
        .await
        .expect("Failed to insert interlay");

        let interlay_uuid: InterlayUuid = interlay_schema.uuid();
        let user_uuid = _user_schema.uuid();

        // Race a port change against a metadata-only update
        let db_port = std::sync::Arc::clone(&db);
        let uuid_port = interlay_uuid.clone();
        let port_task = tokio::spawn(async move {
            db_port
                .pg_connection()
                .await
                .expect("pg conn")
                .execute("UPDATE interlays SET port = 9999 WHERE uuid = $1", &[&uuid_port])
                .await
                .expect("port update");
        });

        let db_meta = std::sync::Arc::clone(&db);
        let uuid_meta = interlay_uuid.clone();
        let meta_task = tokio::spawn(async move {
            let mut tw = crate::test_utils::telemetry_test_utils::test_telemetry();
            db_meta
                .update_interlay_metadata(&uuid_meta, Some("updated description"), None, &user_uuid, &mut tw)
                .await
                .expect("metadata update failed");
        });

        port_task.await.expect("port task panicked");
        meta_task.await.expect("meta task panicked");

        // Verify: both changes survived — port updated AND description updated
        let fetched: InterlaySchema = db.select_interlay_uuid(&interlay_uuid, telemetry).await.expect("select failed");
        assert_eq!(fetched.port(), 9999, "port should reflect the concurrent update");

        let row = db
            .pg_connection()
            .await
            .expect("pg conn")
            .query_one("SELECT description FROM interlays WHERE uuid = $1", &[&interlay_uuid])
            .await
            .expect("select description");
        let desc: Option<String> = row.get("description");
        assert_eq!(desc.as_deref(), Some("updated description"), "description should be updated");
    }

    /// Verify that passing `None` for both description and settings leaves them unchanged.
    #[tokio::test]
    #[serial]
    async fn update_interlay_metadata_none_leaves_fields_unchanged() {
        let db_manager = create_database_manager().await;
        let mut test_telemetry = test_telemetry();
        let telemetry = &mut test_telemetry;

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, telemetry).await;

        let endpoint_schema = insert_endpoint(
            &db_manager,
            telemetry,
            "test_endpoint",
            EpKind::Redis,
            RedisConfig::default().as_config(),
            None,
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let interlay_schema = InterlaySchema::new(
            "test_interlay_noop".to_string(),
            Some("keep me".to_string()),
            endpoint_schema.uuid(),
            8000,
            None,
            None,
            None,
            _user_schema.uuid(),
        );

        let insert = InsertInterlay::new(organization_schema.uuid(), interlay_schema.clone());
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            InterlaySchema,
            InterlayCacheUuid,
            InterlayCacheId,
            InsertInterlay,
        >>::insert(&db_manager, insert, telemetry)
        .await
        .expect("Failed to insert interlay");

        let interlay_uuid: InterlayUuid = interlay_schema.uuid();

        // Update with all None — nothing should change
        db_manager
            .update_interlay_metadata(&interlay_uuid, None, None, &_user_schema.uuid(), telemetry)
            .await
            .expect("metadata update failed");

        let fetched: InterlaySchema = db_manager.select_interlay_uuid(&interlay_uuid, telemetry).await.expect("select failed");

        assert_eq!(fetched.port(), 8000, "port should remain unchanged");
    }
}
