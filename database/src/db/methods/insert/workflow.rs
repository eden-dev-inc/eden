use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::db::{cache::CacheFunctions, lib::DatabaseManager};
#[cfg(not(embedded_db))]
use crate::sql_file;
use eden_core::format::{EdenUuid, OrganizationUuid, UserUuid, WorkflowId, WorkflowUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::{EntityType, EpError},
    format::{
        cache_id::WorkflowCacheId,
        cache_uuid::{CacheUuid, OrganizationCacheUuid, WorkflowCacheUuid},
    },
};
use ep_core::database::schema::Table;
use ep_core::database::schema::workflow::WorkflowSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct InsertWorkflow {
    org_uuid: OrganizationUuid,
    workflow_schema: WorkflowSchema,
}

impl InsertWorkflow {
    pub fn new(org_uuid: OrganizationUuid, workflow_schema: WorkflowSchema) -> Self {
        InsertWorkflow { org_uuid, workflow_schema }
    }

    pub fn org_uuid(&self) -> &OrganizationUuid {
        &self.org_uuid
    }

    pub fn set_created_by(&mut self, created_by: UserUuid) {
        self.workflow_schema.set_created_by(created_by);
    }

    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.workflow_schema.set_updated_by(updated_by);
    }
}

impl<R, P, C> Insert<R, P, C> for InsertWorkflow
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: workflow id (VARCHAR)
    /// $2: workflow uuid (UUID)
    /// $3: workflow dag (equivalent to description in teams)
    /// $4: workflow description (VARCHAR)
    /// $5: created_at (TIMESTAMP)
    /// $6: updated_at (TIMESTAMP)
    /// $7: organization uuid (UUID)
    /// $8: template uuids (UUID[])
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let conn = db.pg_connection().await?;

        #[cfg(not(embedded_db))]
        {
            conn.execute(
                sql_file!("insert", "workflow"),
                &[
                    &self.workflow_schema.id(),
                    &self.workflow_schema.uuid(),
                    &self.workflow_schema.dag(),
                    &self.workflow_schema.description(),
                    &self.workflow_schema.created_by(),
                    &self.workflow_schema.updated_by(),
                    &self.workflow_schema.created_at(),
                    &self.workflow_schema.updated_at(),
                    &self.org_uuid.uuid(),
                    &self.workflow_schema.template_uuids(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?;
        }

        #[cfg(embedded_db)]
        {
            let template_uuids = self.workflow_schema.template_uuids();

            // Verify each template exists and belongs to the organization
            for template_uuid in &template_uuids {
                let rows = conn
                    .query(
                        "SELECT t.uuid FROM templates t INNER JOIN organization_templates ot ON ot.template_uuid = t.uuid WHERE t.uuid = ?1 AND ot.organization_uuid = ?2",
                        &[template_uuid, &self.org_uuid.uuid()],
                    )
                    .await
                    .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?;

                if rows.is_empty() {
                    return Err(EpError::database_template_not_found());
                }
            }

            // Insert into workflows table
            conn.execute(
                "INSERT INTO workflows (id, uuid, dag, description, created_by, updated_by, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                &[
                    &self.workflow_schema.id(),
                    &self.workflow_schema.uuid(),
                    &self.workflow_schema.dag(),
                    &self.workflow_schema.description(),
                    &self.workflow_schema.created_by(),
                    &self.workflow_schema.updated_by(),
                    &self.workflow_schema.created_at(),
                    &self.workflow_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?;

            // Link workflow to organization
            conn.execute(
                "INSERT INTO organization_workflows (organization_uuid, workflow_uuid) VALUES (?1, ?2)",
                &[&self.org_uuid.uuid(), &self.workflow_schema.uuid()],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?;

            // Link workflow to each template
            for template_uuid in &template_uuids {
                conn.execute(
                    "INSERT INTO workflow_templates (workflow_uuid, template_uuid, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
                    &[
                        &self.workflow_schema.uuid(),
                        template_uuid,
                        &self.workflow_schema.created_at(),
                        &self.workflow_schema.updated_at(),
                    ],
                )
                .await
                .map(|_| ())
                .map_err(|e| EpError::database_query_error(e, EntityType::Workflow))?;
            }
        }

        Ok(())
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            WorkflowSchema,
            WorkflowCacheUuid,
            WorkflowUuid,
            WorkflowCacheId,
            WorkflowId,
        >>::set_ex_cache(
            db,
            Some(OrganizationCacheUuid::new(None, self.org_uuid.to_owned())),
            self.workflow_schema.to_owned(),
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod insert_workflow {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::endpoint::tests::insert_endpoint;
    use crate::methods::insert::template::insert_template::insert_template;
    use crate::methods::insert::workflow::InsertWorkflow;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::cache_id::{CacheId, WorkflowCacheId};
    use eden_core::format::cache_uuid::WorkflowCacheUuid;
    use eden_core::format::{CacheUuid, EdenId, EndpointUuid, OrganizationCacheUuid, OrganizationUuid, TemplateUuid, UserUuid, WorkflowId};
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::workflow::WorkflowSchema;
    use ep_core::database::workflow::{Dag, Workflow};
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    pub async fn insert_workflow(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        organization_uuid: OrganizationUuid,
        endpoint_uuid: EndpointUuid,
    ) -> WorkflowSchema {
        // test template - workflow needs existing template
        let template_schema =
            insert_template(db_manager, test_telemetry, endpoint_uuid, organization_uuid.clone(), "test_workflow_template")
                .await
                .expect("Failed to insert template");
        // Query actual UUID from DB to handle ON CONFLICT upsert (template may already exist with a different UUID)
        let actual_template_uuid: TemplateUuid = db_manager
            .pg_connection()
            .await
            .expect("pg connection for template lookup")
            .query_one("SELECT uuid FROM templates WHERE id = $1", &[&template_schema.id()])
            .await
            .expect("template must exist in DB after insert")
            .get(0);
        // test workflow
        let mut dag = Dag::new();
        dag.add_node("test_dag_node".to_string(), actual_template_uuid, None, None, "Test DAG node".to_string());
        let workflow_schema =
            WorkflowSchema::new(Workflow::new(WorkflowId::new("test_template".to_string()), dag, None), UserUuid::new_uuid());

        let insert_workflow = InsertWorkflow::new(organization_uuid, workflow_schema.clone());
        insert_workflow.insert_database(db_manager, test_telemetry).await.expect("Failed to insert workflow");

        workflow_schema
    }

    #[tokio::test]
    async fn insert() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        // template needs an endpoint
        let endpoint_schema = insert_endpoint(
            &db_manager,
            test_telemetry,
            "test_endpoint",
            eden_core::format::endpoint::EpKind::Redis,
            RedisConfig::default().as_config(),
            Some("Test Redis endpoint".to_string()),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        // test template
        let workflow_schema = insert_workflow(&db_manager, test_telemetry, organization_schema.uuid(), endpoint_schema.uuid()).await;

        // get from database with ID
        let from_database =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<WorkflowSchema, WorkflowCacheId>>::get_from_database(
                &db_manager,
                &WorkflowCacheId::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), workflow_schema.id()),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with ID");

        assert_eq!(from_database.id(), workflow_schema.id());

        // get from database with UUID
        let from_database =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                WorkflowSchema,
                WorkflowCacheUuid,
            >>::get_from_database(
                &db_manager,
                &WorkflowCacheUuid::new(
                    Some(OrganizationCacheUuid::new(None, organization_schema.uuid())),
                    workflow_schema.uuid(),
                ),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with UUID");

        assert_eq!(from_database.uuid(), workflow_schema.uuid());

        //manually teardown containers
    }
}
