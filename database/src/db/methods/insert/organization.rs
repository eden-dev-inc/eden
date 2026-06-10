use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
#[cfg(not(embedded_db))]
use crate::sql_file;
use eden_core::format::{EdenNodeId, EdenNodeUuid, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus};
use eden_core::{
    error::{EntityType, EpError, TransactionError},
    format::{
        cache_id::OrganizationCacheId,
        cache_uuid::{CacheUuid, OrganizationCacheUuid},
    },
};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::database::schema::Table;
use ep_core::database::schema::organization::{OrganizationInput, OrganizationSchema};
use function_name::named;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertOrganization {
    organization_schema: OrganizationSchema,
}

impl TryFrom<OrganizationInput> for InsertOrganization {
    type Error = eden_core::error::EpError;

    fn try_from(input: OrganizationInput) -> Result<Self, Self::Error> {
        let org_schema = OrganizationSchema::try_from(input)?;
        Ok(Self::new(org_schema))
    }
}

impl InsertOrganization {
    pub fn new(organization_schema: OrganizationSchema) -> Self {
        Self { organization_schema }
    }

    pub fn organization_schema(&self) -> &OrganizationSchema {
        &self.organization_schema
    }

    pub fn add_eden_node(&mut self, eden_node_id: EdenNodeId, eden_node_uuid: &EdenNodeUuid) -> Result<(), EpError> {
        self.organization_schema.add_eden_node(eden_node_id, eden_node_uuid.to_owned());
        Ok(())
    }
}

impl<R, P, C> Insert<R, P, C> for InsertOrganization
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: organization id (VARCHAR)
    /// $2: organization uuid (UUID)
    /// $3: organization description (TEXT)
    /// $4: created_at (TIMESTAMP)
    /// $5: updated_at (TIMESTAMP)
    #[named]
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("database.{}", function_name!()));

        let _ctx = ctx_with_trace!().with_feature("database").with_organization_uuid(self.organization_schema.uuid().to_string());

        log_debug!(_ctx, "Inserting organization", audience = eden_logger_internal::LogAudience::Internal);

        #[cfg(embedded_db)]
        let conn = db
            .pg_connection()
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;
        #[cfg(not(embedded_db))]
        let mut conn = db
            .pg_connection()
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

        span.add_simple_event("connected to database");

        let transaction = conn.transaction().await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::Transaction(TransactionError::BeginFailed)
        })?;

        span.add_simple_event("started transaction");

        let org_uuid = self.organization_schema.uuid();

        #[cfg(not(embedded_db))]
        {
            transaction
                .execute(
                    sql_file!("insert", "organization"),
                    &[
                        &self.organization_schema.id(),
                        &self.organization_schema.uuid(),
                        &self.organization_schema.description(),
                        &self.organization_schema.created_at(),
                        &self.organization_schema.updated_at(),
                        &self.organization_schema.eden_node_uuids(),
                    ],
                )
                .await
                .map(|_| ())
                .map_err(|e| {
                    let error_msg = format!("Failed to insert organization with eden nodes: {}", e);
                    log::error!("{}", error_msg);
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(error_msg.clone()) });
                    EpError::database_query_error(e, EntityType::Organization)
                })?;
        }

        #[cfg(embedded_db)]
        {
            transaction
                .execute(
                    "INSERT INTO organizations (id, uuid, description, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                    &[
                        &self.organization_schema.id(),
                        &self.organization_schema.uuid(),
                        &self.organization_schema.description(),
                        &self.organization_schema.created_at(),
                        &self.organization_schema.updated_at(),
                    ],
                )
                .await
                .map(|_| ())
                .map_err(|e| {
                    let error_msg = format!("Failed to insert organization: {}", e);
                    log::error!("{}", error_msg);
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(error_msg.clone()) });
                    EpError::database_query_error(e, EntityType::Organization)
                })?;

            for eden_node_uuid in self.organization_schema.eden_node_uuids() {
                transaction
                    .execute(
                        "INSERT INTO organization_eden_nodes (organization_uuid, eden_node_uuid) VALUES (?1, ?2)",
                        &[&org_uuid, &eden_node_uuid],
                    )
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        let error_msg = format!("Failed to insert organization eden node: {}", e);
                        log::error!("{}", error_msg);
                        span.set_status(FastSpanStatus::Error { message: Cow::Owned(error_msg.clone()) });
                        EpError::database_query_error(e, EntityType::Organization)
                    })?;
            }
        }

        span.add_event(
            "prepared insert organization for transaction",
            vec![
                FastSpanAttribute::new("org_uuid", org_uuid.to_string()),
                FastSpanAttribute::new("org_id", self.organization_schema().id().to_string()),
            ],
        );

        // no error during prepare, so commit
        transaction.commit().await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::Transaction(TransactionError::CommitFailed)
        })?;

        span.add_simple_event("successfully commit transaction");

        Ok(())
    }
    #[named]
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let _ctx = ctx_with_trace!().with_feature("database").with_organization_uuid(self.organization_schema.uuid().to_string());

        log_debug!(_ctx, "Collecting cache information", audience = eden_logger_internal::LogAudience::Internal);

        let org_uuid = self.organization_schema.uuid();

        let org_cache_key = OrganizationCacheUuid::new(None, org_uuid.clone());

        <DatabaseManager<R, P, C> as CacheFunctions<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::set_ex_cache(db, Some(org_cache_key.clone()), self.organization_schema().clone(), telemetry_wrapper)
        .await?;

        Ok::<_, EpError>(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::db::methods::insert::organization::InsertOrganization;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::methods::insert::eden_node::insert_eden_node::insert_eden_node;
    use crate::methods::insert::user::InsertUser;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::auth::Password;
    use eden_core::format::cache_id::{CacheId, OrganizationCacheId, UserCacheId};
    use eden_core::format::cache_uuid::UserCacheUuid;
    use eden_core::format::{CacheUuid, EdenNodeId, EdenNodeUuid, OrganizationCacheUuid, OrganizationId, UserId};
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::eden_node::EdenNodeSchema;
    use ep_core::database::schema::organization::OrganizationSchema;
    use ep_core::database::schema::user::UserSchema;

    /// test insert for organizations
    pub async fn insert_organization(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        organization_id: &str,
        admin_usernames_and_passwords: &[(UserId, Password)],
        eden_node_uuids: Vec<EdenNodeUuid>,
        description: Option<String>,
    ) -> (OrganizationSchema, Vec<UserSchema>) {
        let mut eden_node_pairs = Vec::with_capacity(eden_node_uuids.len());
        for eden_node_uuid in eden_node_uuids.iter() {
            let eden_node_schema: EdenNodeSchema =
                db_manager.select_eden_node_uuid(eden_node_uuid, test_telemetry).await.expect("Failed to fetch eden node by UUID");
            eden_node_pairs.push((eden_node_schema.id(), eden_node_uuid.clone()));
        }

        let mut organization_schema = OrganizationSchema::new(organization_id.to_string(), None, eden_node_pairs, description);
        let mut admin_users = Vec::with_capacity(admin_usernames_and_passwords.len());

        for (user_id, password) in admin_usernames_and_passwords {
            let user_schema = UserSchema::new(user_id.clone(), password.clone(), organization_schema.uuid(), None, None, None);
            organization_schema.add_user(user_schema.id(), user_schema.uuid());
            organization_schema.add_super_admin(user_schema.id(), user_schema.uuid());
            admin_users.push(user_schema);
        }

        let insert_organization = InsertOrganization::new(organization_schema.clone());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationCacheId,
            InsertOrganization,
        >>::insert(db_manager, insert_organization, test_telemetry)
        .await
        .expect("Failed to insert organization");

        for user_schema in admin_users.iter() {
            let insert_user = InsertUser::new(user_schema.clone());
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
                UserSchema,
                UserCacheUuid,
                UserCacheId,
                InsertUser,
            >>::insert(db_manager, insert_user, test_telemetry)
            .await
            .expect("Failed to insert admin user {user_id:?}");

            // Register the user as an admin in organization_admins
            db_manager
                .pg_connection()
                .await
                .expect("pg connection for admin insert")
                .execute(
                    "INSERT INTO organization_admins (organization_uuid, user_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&organization_schema.uuid(), &user_schema.uuid()],
                )
                .await
                .expect("Failed to insert organization_admins");
        }

        (organization_schema, admin_users)
    }

    #[tokio::test]
    async fn insert() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let organization_id: OrganizationId = "test_organization".into();

        let eden_node_schema: EdenNodeSchema =
            match db_manager.select_eden_node_id(&EdenNodeId::from("eden_node_test"), test_telemetry).await {
                Ok(en) => en,
                Err(_) => insert_eden_node(&db_manager, test_telemetry, "eden_node_test", vec![], serde_json::Value::default()).await,
            };

        let user_name_and_password = (UserId::from("username"), Password::new("password".to_string()));

        let (organization_schema, _admin_users) = insert_organization(
            &db_manager,
            test_telemetry,
            organization_id.as_str(),
            &[user_name_and_password],
            vec![eden_node_schema.uuid()],
            None,
        )
        .await;

        // get from database with ID
        assert_eq!(
            organization_schema,
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<
                OrganizationSchema,
                OrganizationCacheId,
            >>::get_from_database(
                &db_manager,
                &OrganizationCacheId::new(None, organization_schema.id()),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with ID")
        );

        // get from database with UUID
        assert_eq!(
            organization_schema,
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
            >>::get_from_database(
                &db_manager,
                &OrganizationCacheUuid::new(None, organization_schema.uuid()),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with UUID")
        );

        //manually teardown containers
    }
}
