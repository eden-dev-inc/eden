use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus};
use std::borrow::Cow;

use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
#[cfg(not(embedded_db))]
use crate::sql_file;
use eden_core::error::{EntityType, EpError, TransactionError};
use eden_core::format::cache_id::UserCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::{self, EdenUuid, UserId, UserUuid};
use eden_core::telemetry::{self, TelemetryWrapper};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::database::schema::Table;
use ep_core::database::schema::user::UserSchema;
use function_name::named;
#[cfg(not(embedded_db))]
use postgres_types::Json;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct InsertUser {
    user_schema: UserSchema,
}

impl InsertUser {
    pub fn new(user_schema: UserSchema) -> Self {
        Self { user_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertUser
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: user UUID (UUID)
    /// $2: username (VARCHAR)
    /// $3: organization uuid (UUID)
    /// $4: password (JSONB)
    /// $5: user description (TEXT)
    /// $6: email (VARCHAR)
    /// $7: display_name (VARCHAR)
    /// $8: bio (TEXT)
    /// $9: created_by (UUID)
    /// $10: updated_by (UUID)
    /// $11: created_at (TIMESTAMP)
    /// $12: updated_at (TIMESTAMP)
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let conn = db.pg_connection().await?;

        #[cfg(not(embedded_db))]
        {
            conn.execute(
                sql_file!("insert", "user"),
                &[
                    &self.user_schema.uuid(),
                    &self.user_schema.username(),
                    &self.user_schema.organization_uuid(),
                    &Json(self.user_schema.password()),
                    &self.user_schema.description(),
                    &self.user_schema.email(),
                    &self.user_schema.display_name(),
                    &self.user_schema.bio(),
                    &self.user_schema.created_by(),
                    &self.user_schema.updated_by(),
                    &self.user_schema.created_at(),
                    &self.user_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)?;
        }

        #[cfg(embedded_db)]
        {
            let password_json = serde_json::to_string(self.user_schema.password())
                .map_err(|e| EpError::parse(format!("Failed to serialize password: {e}")))?;
            conn.execute(
                "INSERT INTO users (uuid, username, organization_uuid, password, description, email, display_name, bio, created_by, updated_by, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                &[
                    &self.user_schema.uuid(),
                    &self.user_schema.username(),
                    &self.user_schema.organization_uuid(),
                    &password_json,
                    &self.user_schema.description(),
                    &self.user_schema.email(),
                    &self.user_schema.display_name(),
                    &self.user_schema.bio(),
                    &self.user_schema.created_by(),
                    &self.user_schema.updated_by(),
                    &self.user_schema.created_at(),
                    &self.user_schema.updated_at(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)?;

            conn.execute(
                "INSERT INTO organization_users (organization_uuid, user_uuid) VALUES (?1, ?2)",
                &[&self.user_schema.organization_uuid(), &self.user_schema.uuid()],
            )
            .await
            .map(|_| ())
            .map_err(EpError::database)?;
        }

        Ok(())
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::set_ex_cache(
            db,
            Some(OrganizationCacheUuid::new(None, self.user_schema.organization_uuid().clone())),
            self.user_schema.to_owned(),
            telemetry_wrapper,
        )
        .await
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InsertAdminUser {
    user_schema: UserSchema,
}

impl InsertAdminUser {
    pub fn new(user_schema: UserSchema) -> Self {
        Self { user_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertAdminUser
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// $1: user UUID (UUID)
    /// $2: username (VARCHAR)
    /// $3: organization uuid (UUID)
    /// $4: password (JSONB)
    /// $5: user description (TEXT)
    /// $6: email (VARCHAR)
    /// $7: display_name (VARCHAR)
    /// $8: bio (TEXT)
    /// $9: created_by (UUID)
    /// $10: updated_by (UUID)
    /// $11: created_at (TIMESTAMP)
    /// $12: updated_at (TIMESTAMP)
    #[named]
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let created_at = self.user_schema.created_at();
        let updated_at = self.user_schema.updated_at();
        let mut span = telemetry_wrapper.client_tracer(format!("database.{}", function_name!()));

        let _ctx = ctx_with_trace!()
            .with_feature("database")
            .with_organization_uuid(self.user_schema.organization_uuid().to_string())
            .with_user_uuid(self.user_schema.uuid().to_string());

        log_debug!(_ctx, "Updating organization in postgres", audience = eden_logger_internal::LogAudience::Internal);

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

        span.add_simple_event("connected to postgres");

        let transaction = conn.transaction().await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::Transaction(TransactionError::BeginFailed)
        })?;

        span.add_simple_event("started transaction");

        let user_uuid = self.user_schema.uuid();

        #[cfg(not(embedded_db))]
        let admin_count = {
            // Current count of admin
            let count = transaction
                .execute(sql_file!("select", "organization_admin_count"), &[&self.user_schema.organization_uuid()])
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;

            transaction
                .execute(
                    sql_file!("insert", "user"),
                    &[
                        &user_uuid.uuid(),
                        &self.user_schema.username().to_string(),
                        &self.user_schema.organization_uuid().clone(),
                        &Json(self.user_schema.password()),
                        &self.user_schema.description(),
                        &self.user_schema.email(),
                        &self.user_schema.display_name(),
                        &self.user_schema.bio(),
                        &self.user_schema.created_by(),
                        &self.user_schema.updated_by(),
                        &created_at,
                        &updated_at,
                    ],
                )
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;

            transaction
                .execute(sql_file!("insert", "organization_admin"), &[&self.user_schema.organization_uuid(), &user_uuid])
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;

            count
        };

        #[cfg(embedded_db)]
        let admin_count = {
            let password_json = serde_json::to_string(self.user_schema.password())
                .map_err(|e| EpError::parse(format!("Failed to serialize password: {e}")))?;

            // Insert user
            transaction
                .execute(
                    "INSERT INTO users (uuid, username, organization_uuid, password, description, email, display_name, bio, created_by, updated_by, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    &[
                        &user_uuid.uuid(),
                        &self.user_schema.username().to_string(),
                        &self.user_schema.organization_uuid().clone(),
                        &password_json,
                        &self.user_schema.description(),
                        &self.user_schema.email(),
                        &self.user_schema.display_name(),
                        &self.user_schema.bio(),
                        &self.user_schema.created_by(),
                        &self.user_schema.updated_by(),
                        &created_at,
                        &updated_at,
                    ],
                )
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;

            // Link user to organization
            transaction
                .execute(
                    "INSERT INTO organization_users (organization_uuid, user_uuid) VALUES (?1, ?2)",
                    &[&self.user_schema.organization_uuid(), &user_uuid],
                )
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;

            // Add as org admin
            transaction
                .execute(
                    "INSERT INTO organization_admins (organization_uuid, user_uuid) VALUES (?1, ?2)",
                    &[&self.user_schema.organization_uuid(), &user_uuid],
                )
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;

            // Count admins
            let rows = transaction
                .query(
                    "SELECT count(*) as cnt FROM organization_admins WHERE organization_uuid = ?1",
                    &[&self.user_schema.organization_uuid()],
                )
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database_query_error(e, EntityType::User)
                })?;
            let count = if rows.is_empty() {
                0u64
            } else {
                rows[0].get::<_, i64>("cnt") as u64
            };

            transaction.commit().await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::Transaction(TransactionError::CommitFailed)
            })?;

            count
        };

        span.add_event(
            "prepared insert user for transaction",
            vec![
                FastSpanAttribute::new("user_uuid", user_uuid.to_string()),
                FastSpanAttribute::new("user_id", self.user_schema.username().to_string()),
            ],
        );

        span.add_event(
            "prepared associated admin with organization for transaction",
            vec![
                FastSpanAttribute::new("org_uuid", self.user_schema.organization_uuid().to_string()),
                FastSpanAttribute::new("user_uuid", user_uuid.to_string()),
            ],
        );

        // update iam metrics with super_admin number
        telemetry_wrapper.record_event(telemetry::MetricEvent::RolesGrantedBatch {
            org_uuid: &self.user_schema.organization_uuid().to_string(),
            perms: format::rbac::ControlPerms::all(),
            count: admin_count as i64,
        });

        span.add_simple_event("updated metrics");

        Ok(())
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::set_ex_cache(
            db,
            Some(OrganizationCacheUuid::new(None, self.user_schema.organization_uuid().clone())),
            self.user_schema.to_owned(),
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::methods::insert::user::InsertUser;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::auth::Password;
    use eden_core::format::cache_id::{CacheId, UserCacheId};
    use eden_core::format::cache_uuid::UserCacheUuid;
    use eden_core::format::{CacheUuid, OrganizationCacheUuid, OrganizationUuid, UserId};
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::user::UserSchema;

    /// test insert for users
    pub async fn insert_user(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        username: UserId,
        password: Password,
        description: Option<String>,
        organization_uuid: OrganizationUuid,
    ) -> UserSchema {
        let user_schema = UserSchema::new(username, password, organization_uuid, description, None, None);

        let insert_user = InsertUser::new(user_schema.clone());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertUser>>::insert(
            db_manager,
            insert_user,
            test_telemetry,
        )
        .await
        .expect("Failed to insert user");

        user_schema
    }

    #[tokio::test]
    async fn insert() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, _eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        // test user
        let test_user_schema = UserSchema::new(
            UserId::from("test_user"),
            Password::new("password".to_string()),
            organization_schema.uuid(),
            None,
            None,
            None,
        );

        let insert_user = InsertUser::new(test_user_schema);
        insert_user.insert_database(&db_manager, test_telemetry).await.unwrap_or_default();

        // get from database with ID
        let from_database: UserSchema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<UserSchema, UserCacheId>>::get_from_database(
                &db_manager,
                &UserCacheId::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), insert_user.user_schema.id()),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with ID");

        assert_eq!(from_database.id(), insert_user.user_schema.id());

        // get from database with UUID
        let from_database: UserSchema =
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<UserSchema, UserCacheUuid>>::get_from_database(
                &db_manager,
                &UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, organization_schema.uuid())), insert_user.user_schema.uuid()),
                test_telemetry,
            )
            .await
            .expect("Failed to get schema with UUID");

        assert_eq!(from_database.uuid(), insert_user.user_schema.uuid());

        //manually teardown containers
    }
}
