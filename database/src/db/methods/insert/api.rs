use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::{
    db::{cache::CacheFunctions, lib::DatabaseManager},
    sql_file,
};
use eden_core::format::cache_id::{ApiCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::ApiCacheUuid;
use eden_core::format::{ApiId, ApiUuid, CacheObjectType, EdenUuid, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::{EntityType, EpError},
    format::cache_uuid::{CacheUuid, OrganizationCacheUuid},
};
use ep_core::database::schema::Table;
use ep_core::database::schema::api::ApiSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertApi {
    org_uuid: OrganizationUuid,
    api_schema: ApiSchema,
}

impl InsertApi {
    pub fn new(org_uuid: OrganizationUuid, api_schema: ApiSchema) -> Self {
        Self { org_uuid, api_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertApi
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        db.pg_connection()
            .await?
            .execute(
                sql_file!("insert", "api"),
                &[
                    &self.api_schema.id(),
                    &self.api_schema.uuid(),
                    &self.api_schema.description(),
                    &serde_json::to_value(self.api_schema.fields()).unwrap_or_default(),
                    &serde_json::to_value(self.api_schema.bindings()).unwrap_or_default(),
                    &self.api_schema.response_logic(),
                    &self.api_schema.created_by(),
                    &self.api_schema.updated_by(),
                    &self.api_schema.created_at(),
                    &self.api_schema.updated_at(),
                    &self.org_uuid.uuid(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Api))
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let org_cache_uuid = Some(OrganizationCacheUuid::new(None, self.org_uuid.clone()));

        <DatabaseManager<R, P, C> as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::set_ex_cache(
            db,
            org_cache_uuid.clone(),
            self.api_schema.to_owned(),
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
                schema.add_api(self.api_schema.id(), self.api_schema.uuid());
                schema
            })?,
            telemetry_wrapper,
        )
        .await
    }
}

//
// #[cfg(all(test, feature = "infra-tests"))]
// pub mod insert_template {
//     use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
//     use crate::db::methods::insert::Insert;
//     use crate::lib::{DatabaseManager, ClickhouseConn, PgConn, RedisConn};
//     use crate::test_utils::database_test_utils::create_database_manager;
//     use crate::test_utils::organization_test_utils::initialize_organization;
//     use crate::test_utils::telemetry_test_utils::test_telemetry;
//     use eden_core::format::cache_id::{CacheId, TemplateCacheId};
//     use eden_core::format::cache_uuid::TemplateCacheUuid;
//     use eden_core::format::endpoint::EpKind;
//     use eden_core::format::{
//         CacheUuid, EdenId, EndpointId, EndpointUuid, OrganizationCacheUuid, OrganizationUuid,
//         TemplateId,
//     };
//     use eden_core::telemetry::TelemetryWrapper;
//     use endpoint_schema::endpoint::EndpointSchema;
//     use ep_core::database::schema::api::ApiSchema;
//     use ep_core::database::schema::Table;
//     use ep_core::ep::EpConfig;
//     use postgres_core::config::PostgresConfig;
//
//     pub async fn insert_template(
//         db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
//         test_telemetry: &mut TelemetryWrapper,
//         endpoint_uuid: EndpointUuid,
//         organization_uuid: OrganizationUuid,
//     ) -> ApiSchema {
//         // test api
//         let template_schema = ApiSchema::new(
//             TemplateId::new("test_template".to_string()),
//             JsonTemplate::new(
//                 endpoint_uuid,
//                 TemplateKind::Read,
//                 TemplateValue::new(serde_json::Value::default()),
//                 EpKind::default(),
//             ),
//             Some("sample description".to_string()),
//         );
//
//         let insert_template = InsertTemplate::new(organization_uuid, template_schema.clone());
//         insert_template
//             .insert_database(&db_manager, test_telemetry)
//             .await
//             .unwrap_or_default();
//         //
//         // match template_schema.kind() {
//         //     EpKind::Redis => {
//         //
//         //     }
//         //     _ => todo!("finish impl"),
//         // }
//
//         template_schema
//     }
//
//     #[tokio::test]
//     async fn insert() {
//         // start containers
//         let (redis_container, pg_container, db_manager) = create_database_manager().await;
//
//         let test_telemetry = &mut test_telemetry();
//
//         let (_user_schema, _eden_node_schema, organization_schema) =
//             initialize_organization(&db_manager, test_telemetry).await;
//
//         let endpoint_schema = EndpointSchema::new(
//             EndpointId::from("test_endpoint"),
//             eden_core::format::endpoint::EpKind::Postgres,
//             Box::new(PostgresConfig::default()).as_config(),
//             Some("test PostgreSQL endpoint".to_string()),
//         );
//
//         // test api
//         let template_schema = insert_template(
//             &db_manager,
//             test_telemetry,
//             endpoint_schema.uuid(),
//             organization_schema.uuid(),
//         )
//             .await;
//
//         // get from database with ID
//         let from_database = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<
//             TemplateSchema,
//             TemplateCacheId,
//         >>::get_from_database(
//             &db_manager,
//             &TemplateCacheId::new(
//                 Some(OrganizationCacheUuid::new(None, organization_schema.uuid())),
//                 template_schema.id(),
//             ),
//             test_telemetry,
//         )
//             .await
//             .expect("Failed to get schema with ID");
//
//         assert_eq!(from_database.id(), template_schema.id());
//
//         // get from database with UUID
//         let from_database = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
//             TemplateSchema,
//             TemplateCacheUuid,
//         >>::get_from_database(
//             &db_manager,
//             &TemplateCacheUuid::new(
//                 Some(OrganizationCacheUuid::new(None, organization_schema.uuid())),
//                 template_schema.uuid(),
//             ),
//             test_telemetry,
//         )
//             .await
//             .expect("Failed to get schema with UUID");
//
//         assert_eq!(from_database.uuid(), template_schema.uuid());
//
//         //manually teardown containers
//         redis_container.stop().await.expect("Redis stop failed");
//         pg_container.stop().await.expect("Postgres stop failed");
//     }
// }
