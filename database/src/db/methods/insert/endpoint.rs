use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::{
    db::{cache::CacheFunctions, lib::DatabaseManager},
    sql_files,
};
use borsh::{BorshDeserialize, BorshSerialize};
use eden_core::format::{EdenNodeId, EdenNodeUuid, EdenUuid, EndpointId, EndpointUuid, OrganizationId, OrganizationUuid};
use eden_core::proto::proto::EndpointConnect;
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::{EntityType, EpError},
    format::{
        CacheObjectType,
        cache_id::{EdenNodeCacheId, EndpointCacheId, OrganizationCacheId},
        cache_uuid::{CacheUuid, EdenNodeCacheUuid, EndpointCacheUuid, OrganizationCacheUuid},
    },
};
use endpoint_schema::endpoint::{BoxEpConfig, EndpointSchema};
use ep_core::database::schema::Table;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize, BorshSerialize)]
pub struct InsertEndpoint {
    org_uuid: OrganizationUuid,
    endpoint_schema: EndpointSchema,
    eden_node_uuid: EdenNodeUuid,
}

impl TryFrom<EndpointConnect> for InsertEndpoint {
    type Error = EpError;

    fn try_from(conn: EndpointConnect) -> Result<Self, Self::Error> {
        borsh::from_slice(&conn.insert_endpoint).map_err(EpError::serde)
    }
}

impl InsertEndpoint {
    pub fn new(org_uuid: OrganizationUuid, endpoint_schema: EndpointSchema, eden_node_uuid: EdenNodeUuid) -> Self {
        Self { org_uuid, endpoint_schema, eden_node_uuid }
    }
    pub fn get_organization_uuid(&self) -> &OrganizationUuid {
        &self.org_uuid
    }
    pub fn get_endpoint_schema(&self) -> &EndpointSchema {
        &self.endpoint_schema
    }
    pub fn get_eden_node_id(&self) -> &EdenNodeUuid {
        &self.eden_node_uuid
    }
}

impl<R, P, C> Insert<R, P, C> for InsertEndpoint
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: endpoint id (VARCHAR)
    /// $2: endpoint uuid (UUID)
    /// $3: endpoint config (JSONB)
    /// $4: endpoint description (TEXT)
    /// $5: created At (TIMESTAMP)
    /// $6: updated At (TIMESTAMP)
    /// $7: organization uuid (UUID)
    /// $8: eden_node uuid (UUID)
    #[named]
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        #[cfg(embedded_db)]
        let conn = db.pg_connection().await?;
        #[cfg(not(embedded_db))]
        let mut conn = db.pg_connection().await?;
        log::info!("INSERT endpoint: using eden_node_uuid {}", self.eden_node_uuid);
        let tx = conn.transaction().await.map_err(EpError::database)?;

        tx.execute(sql_files!("insert", "endpoint", "verify_org"), &[&self.org_uuid.uuid()])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Organization))?;

        // Use query_one to get the actual UUID from RETURNING clause
        // This handles ON CONFLICT case where existing UUID is returned
        let routing_json: Option<serde_json::Value> =
            self.endpoint_schema.routing_raw().as_ref().map(serde_json::to_value).transpose().map_err(EpError::serde)?;

        let row = tx
            .query_one(
                sql_files!("insert", "endpoint", "insert_endpoint"),
                &[
                    &self.endpoint_schema.id(),
                    &self.endpoint_schema.uuid(),
                    &self.endpoint_schema.kind().to_string(),
                    &BoxEpConfig::new(self.endpoint_schema.config()),
                    &routing_json,
                    &self.endpoint_schema.description(),
                    &self.endpoint_schema.created_by(),
                    &self.endpoint_schema.updated_by(),
                    &self.endpoint_schema.created_at(),
                    &self.endpoint_schema.updated_at(),
                ],
            )
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?;

        let endpoint_uuid: EndpointUuid = row.get(0);

        tx.execute(sql_files!("insert", "endpoint", "link_endpoint_org"), &[&self.org_uuid.uuid(), &endpoint_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?;

        tx.execute(
            sql_files!("insert", "endpoint", "link_endpoint_eden_node"),
            &[
                &self.eden_node_uuid,
                &endpoint_uuid,
                &self.endpoint_schema.created_at(),
                &self.endpoint_schema.updated_at(),
            ],
        )
        .await
        .map_err(|e| EpError::database_query_error(e, EntityType::Endpoint))?;

        tx.commit().await.map_err(EpError::database)
    }
    #[named]
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let org_cache_uuid = Some(OrganizationCacheUuid::new(None, self.org_uuid.clone()));

        // set endpoint schema to cache - {$EndpointCacheUuid}}: {$EndpointSchema}}
        <DatabaseManager<R, P, C> as CacheFunctions<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::set_ex_cache(
            db,
            org_cache_uuid.clone(),
            self.endpoint_schema.clone(),
            telemetry_wrapper,
        )
        .await?;

        <DatabaseManager<R, P, C> as CacheFunctions<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::set_ex_cache(
            db,
            org_cache_uuid.clone(),
            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::get_from_cache(db, &CacheObjectType::new(org_cache_uuid.clone(), None), telemetry_wrapper)
            .await
            .map(|mut schema| {
                schema.add_endpoint(self.endpoint_schema.id(), self.endpoint_schema.uuid());
                schema
            })?,
            telemetry_wrapper,
        )
        .await?;

        <DatabaseManager<R, P, C> as CacheFunctions<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeUuid,
            EdenNodeCacheId,
            EdenNodeId,
        >>::set_ex_cache(
            db,
            org_cache_uuid,
            <DatabaseManager<R, P, C> as CacheFunctions<
                EdenNodeSchema,
                EdenNodeCacheUuid,
                EdenNodeUuid,
                EdenNodeCacheId,
                EdenNodeId,
            >>::get_from_cache(
                db,
                &CacheObjectType::new(
                    Some(EdenNodeCacheUuid::new(None, self.eden_node_uuid.clone())),
                    None,
                ),
                telemetry_wrapper,
            )
            .await
            .map(|mut schema| {
                schema.add_endpoint_uuid(self.endpoint_schema.uuid());
                schema
            })?,
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::methods::insert::endpoint::InsertEndpoint;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::EndpointCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheUuid, EdenId, EdenNodeUuid, EndpointId, OrganizationCacheUuid, OrganizationUuid};
    use eden_core::telemetry::TelemetryWrapper;
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    /// test insert for endpoints
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_endpoint(
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

    #[tokio::test]
    async fn insert() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

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

        assert_eq!(
            endpoint_schema,
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::get_from_database(
                &db_manager,
                &EndpointCacheId::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), endpoint_schema.id(),),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with ID"),
        );

        assert_eq!(
            endpoint_schema,
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                EndpointSchema,
                EndpointCacheUuid,
            >>::get_from_database(
                &db_manager,
                &EndpointCacheUuid::new(
                    Some(OrganizationCacheUuid::new(None, organization_schema.uuid())),
                    endpoint_schema.uuid(),
                ),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with UUID")
        );

        //manually teardown containers
    }
}
