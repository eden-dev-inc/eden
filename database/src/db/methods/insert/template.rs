use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::{
    db::{cache::CacheFunctions, lib::DatabaseManager},
    sql_file,
};
use eden_core::format::cache_id::OrganizationCacheId;
use eden_core::format::{CacheObjectType, EdenUuid, OrganizationId, OrganizationUuid, TemplateId, TemplateUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::{EntityType, EpError},
    format::{
        cache_id::TemplateCacheId,
        cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid},
    },
};
use ep_core::database::schema::Table;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::template::TemplateSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertTemplate {
    org_uuid: OrganizationUuid,
    template_schema: TemplateSchema,
}

impl InsertTemplate {
    pub fn new(org_uuid: OrganizationUuid, template_schema: TemplateSchema) -> Self {
        Self { org_uuid, template_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertTemplate
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
    /// $7: organization_uuid (UUID)
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let description = self.template_schema.description();
        let llm_recommendation = self.template_schema.llm_recommendation().cloned();

        db.pg_connection()
            .await?
            .execute(
                sql_file!("insert", "template"),
                &[
                    &self.template_schema.id(),
                    &self.template_schema.uuid(),
                    self.template_schema.template(),
                    &description,
                    &llm_recommendation,
                    &self.template_schema.created_by(),
                    &self.template_schema.updated_by(),
                    &self.template_schema.created_at(),
                    &self.template_schema.updated_at(),
                    &self.org_uuid.uuid(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Template))
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let org_cache_uuid = Some(OrganizationCacheUuid::new(None, self.org_uuid.clone()));

        <DatabaseManager<R, P, C> as CacheFunctions<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
        >>::set_ex_cache(
            db,
            org_cache_uuid.clone(),
            self.template_schema.to_owned(),
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
                schema.add_template(self.template_schema.id(), self.template_schema.uuid());
                schema
            })?,
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod insert_template {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::template::InsertTemplate;
    use crate::template::TemplateKind;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::error::ResultEP;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, TemplateCacheId};
    use eden_core::format::cache_uuid::TemplateCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheUuid, EdenId, EndpointId, EndpointUuid, OrganizationCacheUuid, OrganizationUuid, TemplateId};
    use eden_core::telemetry::TelemetryWrapper;
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::template::TemplateSchema;
    use ep_core::database::template::JsonTemplate;
    use ep_core::database::template::wrapper::TemplateValue;
    use ep_core::ep::EpConfig;
    use postgres_core::config::PostgresConfig;

    pub async fn insert_template(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        endpoint_uuid: EndpointUuid,
        organization_uuid: OrganizationUuid,
        template_id: &str,
    ) -> ResultEP<TemplateSchema> {
        // test template
        let template_schema = TemplateSchema::new(
            TemplateId::new(template_id.to_string()),
            JsonTemplate::new(
                endpoint_uuid,
                TemplateKind::Read,
                TemplateValue::new(serde_json::Value::default()),
                Vec::new(),
                EpKind::default(),
                None,
            )?,
            Some("sample description".to_string()),
            None,
            UserUuid::new_uuid(),
        );

        let insert_template = InsertTemplate::new(organization_uuid, template_schema.clone());
        insert_template.insert_database(db_manager, test_telemetry).await?;
        //
        // match template_schema.kind() {
        //     EpKind::Redis => {
        //
        //     }
        //     _ => todo!("finish impl"),
        // }

        Ok(template_schema)
    }

    #[tokio::test]
    async fn insert() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, _eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        let endpoint_schema = EndpointSchema::new(
            EndpointId::from("test_endpoint"),
            eden_core::format::endpoint::EpKind::Postgres,
            PostgresConfig::default().as_config(),
            None,
            Some("test PostgreSQL endpoint".to_string()),
            UserUuid::new_uuid(),
        );

        // test template
        let template_schema =
            insert_template(&db_manager, test_telemetry, endpoint_schema.uuid(), organization_schema.uuid(), "test_template")
                .await
                .expect("template schema");

        // get from database with ID
        let from_database =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<TemplateSchema, TemplateCacheId>>::get_from_database(
                &db_manager,
                &TemplateCacheId::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), template_schema.id()),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with ID");

        assert_eq!(from_database.id(), template_schema.id());

        // get from database with UUID
        let from_database =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                TemplateSchema,
                TemplateCacheUuid,
            >>::get_from_database(
                &db_manager,
                &TemplateCacheUuid::new(
                    Some(OrganizationCacheUuid::new(None, organization_schema.uuid())),
                    template_schema.uuid(),
                ),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with UUID");

        assert_eq!(from_database.uuid(), template_schema.uuid());

        //manually teardown containers
    }
}
