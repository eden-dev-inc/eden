use crate::db::cache::CacheFunctions;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::{db::lib::DatabaseManager, sql_file};
use eden_core::error::{EntityType, EpError};
use eden_core::format::cache_id::EdenNodeCacheId;
use eden_core::format::cache_uuid::EdenNodeCacheUuid;
use eden_core::format::{EdenNodeId, EdenNodeUuid, EdenUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::Table;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use function_name::named;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertEdenNode {
    eden_node_schema: EdenNodeSchema,
}

impl InsertEdenNode {
    pub fn new(eden_node_schema: EdenNodeSchema) -> Self {
        Self { eden_node_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertEdenNode
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: template id (VARCHAR)
    /// $2: template uuid (UUID)
    /// $3: template (JSONB)
    /// $4: description (TEXT)
    /// $5: created_at (TIMESTAMP)
    /// $6: updated_at (TIMESTAMP)
    // TODO: revisit when telemetry is added to this function
    #[allow(unused_macros)]
    #[named]
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        db.pg_connection()
            .await?
            .execute(
                sql_file!("insert", "eden_node"),
                &[
                    &self.eden_node_schema.id().to_string(),
                    &self.eden_node_schema.uuid().uuid(),
                    // &self.eden_node_schema.info(),
                    &self.eden_node_schema.description(),
                    &self.eden_node_schema.created_at(),
                    &self.eden_node_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::EdenNode))
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeUuid,
            EdenNodeCacheId,
            EdenNodeId,
        >>::set_ex_cache(
            db,
            None,
            self.eden_node_schema.to_owned(),
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod insert_eden_node {
    use super::*;
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::{EdenId, EndpointUuid};

    /// test insert for eden nodes
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

    #[tokio::test]
    async fn insert() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        assert_eq!(
            insert_eden_node(&db_manager, test_telemetry, "test_node", vec![], serde_json::Value::default(),).await,
            db_manager
                .select_eden_node_id(&EdenNodeId::new("test_node".to_string()), test_telemetry)
                .await
                .expect("Failed to select test_node")
        );

        //manually teardown containers
    }
}
