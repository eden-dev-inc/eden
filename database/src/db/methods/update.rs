mod auth;
mod endpoint;
mod organization;
mod template;
mod user;
mod workflow;

use crate::db::cache::CacheIdFunctions;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{
    db::{cache::CacheFunctions, lib::DatabaseManager},
    sql_file,
};
use chrono::Utc;
use eden_core::auth::{ApiKey, Password};
use eden_core::error::{EntityType, ResultEP};
use eden_core::format::cache_id::{
    AuthCacheId, EndpointCacheId, OrganizationCacheId, RobotCacheId, TemplateCacheId, UserCacheId, WorkflowCacheId,
};
use eden_core::format::cache_uuid::{
    AuthCacheUuid, EndpointCacheUuid, OrganizationCacheUuid, RobotCacheUuid, TemplateCacheUuid, UserCacheUuid, WorkflowCacheUuid,
};
use eden_core::format::{
    AuthId, AuthUuid, EdenId, EdenUuid, EndpointId, EndpointUuid, OrganizationId, OrganizationUuid, RobotId, RobotUuid, TemplateId,
    TemplateUuid, UserId, UserUuid, WorkflowId, WorkflowUuid,
};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::EpError,
    format::{CacheObjectType, cache_id::CacheId, cache_uuid::CacheUuid, endpoint::EpKind},
};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_warn};
use endpoint_schema::endpoint::{BoxEpConfig, EndpointSchema};
use ep_core::database::schema::auth::AuthSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::robot::RobotSchema;
use ep_core::database::schema::template::TemplateSchema;
use ep_core::database::schema::user::UserSchema;
use ep_core::database::schema::workflow::WorkflowSchema;
use ep_core::database::schema::{AuthType, FromRow, Table};
use ep_core::database::template::JsonTemplate;
use ep_core::database::workflow::Dag;
use ep_core::ep::{EpConfig, EpConnection};
use function_name::named;
use postgres_types::ToSql;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

#[derive(Debug)]
pub struct UpdateEndpoint {
    pub read_conn: Option<Box<dyn EpConnection>>,
    pub write_conn: Option<Box<dyn EpConnection>>,
}

impl UpdateEndpoint {
    pub fn read_conn(&self) -> &Option<Box<dyn EpConnection>> {
        &self.read_conn
    }
    pub fn write_conn(&self) -> &Option<Box<dyn EpConnection>> {
        &self.write_conn
    }
    pub fn kind(&self) -> Option<EpKind> {
        if let Some(read) = &self.read_conn {
            Some(read.kind())
        } else {
            self.write_conn.as_ref().map(|write| write.kind())
        }
    }
}

#[derive(Debug)]
pub enum SqlQueries {
    // Auth queries
    UpdateAuthId,
    UpdateAuthType,

    // Eden Node queries
    UpdateEdenNodeId,
    UpdateEdenNodeDescription,
    UpdateEdenNodeInfo,

    // Endpoint queries
    UpdateEndpointId,
    UpdateEndpointDescription,
    UpdateEndpointConfig,

    // Organization queries
    UpdateOrganizationId,
    UpdateOrganizationDescription,
    UpdateOrganizationRateLimitSettings,

    // Template queries
    UpdateTemplateId,
    UpdateTemplateDescription,
    UpdateTemplateTemplate,

    // Robot queries
    UpdateRobotDescription,
    UpdateRobotApiKey,
    UpdateRobotTtl,

    // User queries
    UpdateUserUsername,
    UpdateUserDescription,
    UpdateUserPassword,
    UpdateUserEmail,
    UpdateUserDisplayName,
    UpdateUserBio,

    // Interlay queries
    UpdateInterlayId,
    UpdateInterlayDescription,

    // Workflow queries
    UpdateWorkflowId,
    UpdateWorkflowDescription,
    UpdateWorkflowDag,
}

impl SqlQueries {
    pub fn as_query_uuid(&self) -> &'static str {
        match self {
            // Auth queries
            Self::UpdateAuthId => sql_file!("update", "auth_id_from_uuid"),
            Self::UpdateAuthType => sql_file!("update", "auth_type"),

            // Eden Node queries
            Self::UpdateEdenNodeId => sql_file!("update", "eden_node_id_from_uuid"),
            Self::UpdateEdenNodeDescription => sql_file!("update", "eden_node_description"),
            Self::UpdateEdenNodeInfo => sql_file!("update", "eden_node_info"),

            // Endpoint queries
            Self::UpdateEndpointId => sql_file!("update", "endpoint_id_from_uuid"),
            Self::UpdateEndpointDescription => sql_file!("update", "endpoint_description_from_uuid"),
            Self::UpdateEndpointConfig => sql_file!("update", "endpoint_config"),

            // Organization queries
            Self::UpdateOrganizationId => sql_file!("update", "organization_id_from_uuid"),
            Self::UpdateOrganizationDescription => {
                sql_file!("update", "organization_description_from_uuid")
            }
            Self::UpdateOrganizationRateLimitSettings => {
                sql_file!("update", "organization_rate_limit_settings_from_uuid")
            }

            // Template queries
            Self::UpdateTemplateId => sql_file!("update", "template_id_from_uuid"),
            Self::UpdateTemplateDescription => {
                sql_file!("update", "template_description_from_uuid")
            }
            Self::UpdateTemplateTemplate => sql_file!("update", "template_template"),

            // Robot queries
            Self::UpdateRobotDescription => sql_file!("update", "robot_description_from_uuid"),
            Self::UpdateRobotApiKey => sql_file!("update", "robot_api_key_from_uuid"),
            Self::UpdateRobotTtl => sql_file!("update", "robot_ttl_from_uuid"),

            // User queries
            Self::UpdateUserUsername => sql_file!("update", "user_username_from_uuid"),
            Self::UpdateUserDescription => sql_file!("update", "user_description_from_uuid"),
            Self::UpdateUserPassword => sql_file!("update", "user_password_from_uuid"),
            Self::UpdateUserEmail => sql_file!("update", "user_email_from_uuid"),
            Self::UpdateUserDisplayName => sql_file!("update", "user_display_name_from_uuid"),
            Self::UpdateUserBio => sql_file!("update", "user_bio_from_uuid"),

            // Interlay queries
            Self::UpdateInterlayId => sql_file!("update", "interlay_id_from_uuid"),
            Self::UpdateInterlayDescription => sql_file!("update", "interlay_description"),

            // Workflow queries
            Self::UpdateWorkflowId => sql_file!("update", "workflow_id_from_uuid"),
            Self::UpdateWorkflowDescription => sql_file!("update", "workflow_description"),
            Self::UpdateWorkflowDag => sql_file!("update", "workflow_dag"),
        }
    }

    pub fn as_query_id(&self) -> &'static str {
        match self {
            // Auth queries
            Self::UpdateAuthId => sql_file!("update", "auth_id_from_id"),
            Self::UpdateAuthType => sql_file!("update", "auth_type"),

            // Eden Node queries
            Self::UpdateEdenNodeId => sql_file!("update", "eden_node_id_from_id"),
            Self::UpdateEdenNodeDescription => sql_file!("update", "eden_node_description"),
            Self::UpdateEdenNodeInfo => sql_file!("update", "eden_node_info"),

            // Endpoint queries
            Self::UpdateEndpointId => sql_file!("update", "endpoint_id_from_id"),
            Self::UpdateEndpointDescription => sql_file!("update", "endpoint_description"),
            Self::UpdateEndpointConfig => sql_file!("update", "endpoint_config"),

            // Organization queries
            Self::UpdateOrganizationId => sql_file!("update", "organization_id_from_id"),
            Self::UpdateOrganizationDescription => {
                sql_file!("update", "organization_description_from_id")
            }
            Self::UpdateOrganizationRateLimitSettings => {
                sql_file!("update", "organization_rate_limit_settings_from_uuid")
            }

            // Template queries
            Self::UpdateTemplateId => sql_file!("update", "template_id_from_id"),
            Self::UpdateTemplateDescription => sql_file!("update", "template_description_from_id"),
            Self::UpdateTemplateTemplate => sql_file!("update", "template_template"),

            // Robot queries (robots only support uuid-based updates)
            Self::UpdateRobotDescription => sql_file!("update", "robot_description_from_uuid"),
            Self::UpdateRobotApiKey => sql_file!("update", "robot_api_key_from_uuid"),
            Self::UpdateRobotTtl => sql_file!("update", "robot_ttl_from_uuid"),

            // User queries
            Self::UpdateUserUsername => sql_file!("update", "user_username_from_id"),
            Self::UpdateUserDescription => sql_file!("update", "user_description_from_id"),
            Self::UpdateUserPassword => sql_file!("update", "user_password_from_id"),
            Self::UpdateUserEmail => sql_file!("update", "user_email_from_id"),
            Self::UpdateUserDisplayName => sql_file!("update", "user_display_name_from_id"),
            Self::UpdateUserBio => sql_file!("update", "user_bio_from_id"),

            // Interlay queries
            Self::UpdateInterlayId => sql_file!("update", "interlay_id_from_id"),
            Self::UpdateInterlayDescription => sql_file!("update", "interlay_description"),

            // Workflow queries
            Self::UpdateWorkflowId => sql_file!("update", "workflow_id_from_id"),
            Self::UpdateWorkflowDescription => sql_file!("update", "workflow_description"),
            Self::UpdateWorkflowDag => sql_file!("update", "workflow_dag"),
        }
    }
}

const SYSTEM_ACTOR: &str = "eden-system";

#[derive(Clone, Copy, Debug)]
pub enum UpdateActor<'a> {
    User(&'a UserUuid),
    System(&'static str),
}

impl UpdateActor<'_> {
    fn as_updated_by_uuid(&self) -> Option<uuid::Uuid> {
        match self {
            Self::User(user_uuid) => Some(user_uuid.uuid()),
            Self::System(_system_actor) => None,
        }
    }

    fn as_user_uuid(&self) -> Option<&UserUuid> {
        match self {
            Self::User(user_uuid) => Some(*user_uuid),
            Self::System(_) => None,
        }
    }
}

pub trait UpdateMethod<T, U, EU, I, EI> {
    fn update_id(
        &self,
        object: &CacheObjectType<U, I>,
        sql: SqlQueries,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_description(
        &self,
        object: &CacheObjectType<U, I>,
        sql: SqlQueries,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_auth_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_eden_node_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_endpoint_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_organization_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_template_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_user_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_workflow_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_eden_node_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_endpoint_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_organization_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_organization_rate_limit_settings(
        &self,
        object: &CacheObjectType<U, I>,
        new_settings: Option<serde_json::Value>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_template_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_user_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_robot_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_robot_api_key(
        &self,
        object: &CacheObjectType<U, I>,
        new_plaintext_key: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_robot_ttl(
        &self,
        object: &CacheObjectType<U, I>,
        new_ttl: i64,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_workflow_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_auth_type(
        &self,
        object: &CacheObjectType<U, I>,
        new_auth: AuthType,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_endpoint_config(
        &self,
        object: &CacheObjectType<U, I>,
        new_config: Box<dyn EpConfig>,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_template_template(
        &self,
        object: &CacheObjectType<U, I>,
        new_template: JsonTemplate,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_template_llm_recommendation(
        &self,
        object: &CacheObjectType<U, I>,
        new_recommendation: Option<String>,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_user_password(
        &self,
        object: &CacheObjectType<U, I>,
        new_password: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_user_email(
        &self,
        object: &CacheObjectType<U, I>,
        new_email: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_user_display_name(
        &self,
        object: &CacheObjectType<U, I>,
        new_display_name: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_user_bio(
        &self,
        object: &CacheObjectType<U, I>,
        new_bio: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn update_workflow_dag(
        &self,
        object: &CacheObjectType<U, I>,
        new_dag: Dag,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Update Postgres database and Cache
    #[named]
    async fn update_cache<T, U, EU, I, EI, UF, F>(
        &self,
        object: &CacheObjectType<U, I>,
        sql: SqlQueries,
        update_field: UF,
        update_fn: F,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<U>>
    where
        T: Table + FromRow + Serialize + DeserializeOwned,
        U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
        EU: EdenUuid,
        I: CacheId + Clone,
        EI: EdenId,
        UF: Clone + ToString + ToSql + Send + Sync,
        F: Fn(&mut T, UF) -> Option<U>, // Modified bound to take &mut T and return the old ID
    {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let _ctx = ctx_with_trace!().with_feature("database");

        let conn = self.pg_connection().await?;

        log_debug!(
            _ctx,
            "Running SQL update",
            audience = eden_logger_internal::LogAudience::Internal,
            sql_query = format!("{:?}", sql),
            query_string = sql.as_query_uuid()
        );
        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        let supports_updated_by = if let Some(uuid) = object.uuid() {
            let query = sql.as_query_uuid();
            if query.contains("$4") {
                conn.execute(query, &[&uuid.uuid(), &update_field, &now, &ub_uuid]).await.map_err(EpError::database)?;
                true
            } else {
                conn.execute(query, &[&uuid.uuid(), &update_field, &now]).await.map_err(EpError::database)?;
                false
            }
        } else if let Some(id) = object.id() {
            let query = sql.as_query_id();
            if query.contains("$4") {
                conn.execute(query, &[&id.id(), &update_field, &now, &ub_uuid]).await.map_err(EpError::database)?;
                true
            } else {
                conn.execute(query, &[&id.id(), &update_field, &now]).await.map_err(EpError::database)?;
                false
            }
        } else {
            return Err(EpError::database("Object does not exist"));
        };

        // get schema in cache
        let mut schema: T = <Self as CacheFunctions<T, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        // update the schema
        let update_output = update_fn(&mut schema, update_field);

        // update the updated_by field in cache
        if supports_updated_by && let Some(ub) = updated_by.as_user_uuid() {
            schema.update_updated_by(ub.clone());
        }

        // set updated schema to cache
        let _ = <Self as CacheFunctions<T, U, EU, I, EI>>::set_ex_cache(self, object.org(), schema, telemetry_wrapper).await;

        Ok::<_, EpError>(update_output)
    }
}

impl<T, U, EU, I, EI, R, P, C> UpdateMethod<T, U, EU, I, EI> for DatabaseManager<R, P, C>
where
    T: Table + FromRow + Serialize + DeserializeOwned,
    U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    EU: EdenUuid,
    I: CacheId + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    EI: EdenId,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    #[named]
    async fn update_id(
        &self,
        object: &CacheObjectType<U, I>,
        sql: SqlQueries,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let cache_timeout = Duration::from_secs(2);
        let mut schema = match tokio::time::timeout(
            cache_timeout,
            <Self as CacheFunctions<T, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(EpError::timeout("Timed out loading object before id update"));
            }
        };
        let old_id = schema.id();

        if old_id.id() == new_id {
            log_debug!(
                ctx_with_trace!().with_feature("database"),
                "Skipping id update because the requested id is unchanged",
                audience = LogAudience::Internal
            );
            return Ok(());
        }

        let map_update_error = |error| match sql {
            SqlQueries::UpdateEndpointId => EpError::database_query_error(error, EntityType::Endpoint),
            _ => EpError::database(error),
        };

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        let affected = if let Some(uuid) = object.uuid() {
            let query = sql.as_query_uuid();
            if query.contains("$4") {
                self.pg_connection().await?.execute(query, &[&uuid.uuid(), &new_id, &now, &ub_uuid]).await.map_err(map_update_error)?
            } else {
                self.pg_connection().await?.execute(query, &[&uuid.uuid(), &new_id, &now]).await.map_err(map_update_error)?
            }
        } else if object.id().is_some() {
            let query = sql.as_query_id();
            if query.contains("$4") {
                self.pg_connection().await?.execute(query, &[&old_id, &new_id, &now, &ub_uuid]).await.map_err(map_update_error)?
            } else {
                self.pg_connection().await?.execute(query, &[&old_id, &new_id, &now]).await.map_err(map_update_error)?
            }
        } else {
            return Err(EpError::database("Object does not exist"));
        };

        if affected == 0 {
            return Err(EpError::database("Object not found while updating id"));
        }

        schema.update_id(new_id);

        if let Some(ub) = updated_by.as_user_uuid() {
            schema.update_updated_by(ub.clone());
        }

        match tokio::time::timeout(
            cache_timeout,
            <Self as CacheIdFunctions<T, I>>::invalidate(self, &I::new(object.org(), old_id), telemetry_wrapper),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log_warn!(
                    ctx_with_trace!().with_feature("database"),
                    "Failed to invalidate previous id cache after id update",
                    audience = LogAudience::Internal,
                    error = error.to_string()
                );
            }
            Err(_) => {
                log_warn!(
                    ctx_with_trace!().with_feature("database"),
                    "Timed out invalidating previous id cache after id update",
                    audience = LogAudience::Internal
                );
            }
        }

        match tokio::time::timeout(
            cache_timeout,
            <Self as CacheFunctions<T, U, EU, I, EI>>::set_ex_cache(self, object.org(), schema.to_owned(), telemetry_wrapper),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log_warn!(
                    ctx_with_trace!().with_feature("database"),
                    "Failed to refresh cache after id update",
                    audience = LogAudience::Internal,
                    error = error.to_string()
                );
            }
            Err(_) => {
                log_warn!(
                    ctx_with_trace!().with_feature("database"),
                    "Timed out refreshing cache after id update",
                    audience = LogAudience::Internal
                );
            }
        }

        Ok(())
    }

    #[named]
    async fn update_description(
        &self,
        object: &CacheObjectType<U, I>,
        sql: SqlQueries,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.update_cache::<T, U, EU, I, EI, String, _>(
            object,
            sql,
            new_description,
            |schema, description| {
                // Call the original update_description method on the schema
                schema.update_description(description);
                // Return None since we don't need to return an old value
                None
            },
            updated_by,
            telemetry_wrapper,
        )
        .await?;

        Ok(())
    }
    #[named]
    async fn update_auth_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateAuthId,
            new_id,
            UpdateActor::System(SYSTEM_ACTOR),
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_eden_node_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateEdenNodeId,
            new_id,
            UpdateActor::System(SYSTEM_ACTOR),
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_endpoint_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateEndpointId,
            new_id,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_organization_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateOrganizationId,
            new_id,
            UpdateActor::System(SYSTEM_ACTOR),
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_template_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateTemplateId,
            new_id,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_user_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateUserUsername,
            new_id,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_workflow_id(
        &self,
        object: &CacheObjectType<U, I>,
        new_id: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_id(
            self,
            object,
            SqlQueries::UpdateWorkflowId,
            new_id,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_eden_node_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateEdenNodeDescription,
            new_description,
            UpdateActor::System(SYSTEM_ACTOR),
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_endpoint_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateEndpointDescription,
            new_description,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_organization_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateOrganizationDescription,
            new_description,
            UpdateActor::System(SYSTEM_ACTOR),
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_organization_rate_limit_settings(
        &self,
        object: &CacheObjectType<U, I>,
        new_settings: Option<serde_json::Value>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut org = <Self as CacheFunctions<OrganizationSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let conn = self.pg_connection().await?;

        conn.execute(
            sql_file!("update", "organization_rate_limit_settings_from_uuid"),
            &[&org.uuid(), &new_settings, &now],
        )
        .await
        .map_err(EpError::database)?;

        let settings: Option<ep_core::database::schema::organization::RateLimitSettings> =
            new_settings.and_then(|v| serde_json::from_value(v).ok());
        org.update_rate_limit_settings(settings);

        <Self as CacheFunctions<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::set_ex_cache(self, object.org(), org, telemetry_wrapper)
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_template_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateTemplateDescription,
            new_description,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_user_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateUserDescription,
            new_description,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_robot_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateRobotDescription,
            new_description,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_robot_api_key(
        &self,
        object: &CacheObjectType<U, I>,
        new_plaintext_key: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut robot = <Self as CacheFunctions<RobotSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let hashed_key = ApiKey::from_plaintext(&new_plaintext_key);
        let pg_key_json = postgres_types::Json(hashed_key.clone());
        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "robot_api_key_from_uuid"), &[&robot.uuid(), &pg_key_json, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        robot.update_api_key(hashed_key);

        if let Some(ub) = updated_by.as_user_uuid() {
            robot.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::set_ex_cache(
            self,
            object.org(),
            robot.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_robot_ttl(
        &self,
        object: &CacheObjectType<U, I>,
        new_ttl: i64,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut robot = <Self as CacheFunctions<RobotSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let ttl_for_cache = if new_ttl > 0 { Some(new_ttl) } else { None };

        let new_expires_at = ttl_for_cache.map(|t| Utc::now() + chrono::Duration::seconds(t));

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(
                sql_file!("update", "robot_ttl_from_uuid"),
                &[&robot.uuid(), &new_ttl, &new_expires_at, &now, &ub_uuid],
            )
            .await
            .map_err(EpError::database)?;

        robot.update_ttl(ttl_for_cache);

        if let Some(ub) = updated_by.as_user_uuid() {
            robot.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::set_ex_cache(
            self,
            object.org(),
            robot.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_workflow_description(
        &self,
        object: &CacheObjectType<U, I>,
        new_description: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as UpdateMethod<T, U, EU, I, EI>>::update_description(
            self,
            object,
            SqlQueries::UpdateWorkflowDescription,
            new_description,
            updated_by,
            telemetry_wrapper,
        )
        .await
    }
    #[named]
    async fn update_auth_type(
        &self,
        object: &CacheObjectType<U, I>,
        new_auth: AuthType,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut auth = <Self as CacheFunctions<AuthSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        self.pg_connection()
            .await?
            .execute(sql_file!("update", "auth_type"), &[&auth.uuid(), &new_auth, &Utc::now()])
            .await
            .map_err(EpError::database)?;

        auth.update_auth(new_auth);

        <Self as CacheFunctions<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId>>::set_ex_cache(
            self,
            object.org(),
            auth.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    // async fn update_eden_node_info(
    //     &self,
    //     object: &CacheObjectType<U, I>,
    //     new_info: Value,
    // ) -> Result<(), EpError> {
    //     let conn = self.p_connection().await?;
    //
    //     let key =
    //         <Self as CacheFunctions<T, U, I>>::get_key_from_cache_object(self, object).await?;
    //
    //     conn.execute(
    //         sql_file!("update", "eden_node_info"),
    //         &[&key.uuid(), &new_info, &Utc::now()],
    //     )
    //     .await
    //     .map_err(EpError::database)?;
    //
    //     let mut value: EdenNodeSchema = self.get_from_cache(object).await?;
    //     value.update_info(new_info);
    //
    //     <Self as CacheFunctions<T, U, I>>::set_ex_cache(
    //         self,
    //         &key.to_string(),
    //         serde_json::to_string(&value).map_err(EpError::serde)?,
    //     )
    //     .await?;
    //
    //     Ok(())
    // }

    #[named]
    async fn update_endpoint_config(
        &self,
        object: &CacheObjectType<U, I>,
        new_config: Box<dyn EpConfig>,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut endpoint = <Self as CacheFunctions<EndpointSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let pg_new_config = BoxEpConfig::new(new_config.clone());
        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "endpoint_config"), &[&endpoint.uuid(), &pg_new_config, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        endpoint.update_config(new_config);

        if let Some(ub) = updated_by.as_user_uuid() {
            endpoint.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::set_ex_cache(
            self,
            object.org(),
            endpoint.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_template_template(
        &self,
        object: &CacheObjectType<U, I>,
        new_template: JsonTemplate,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut template = <Self as CacheFunctions<TemplateSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "template_template"), &[&template.uuid(), &new_template, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        template.update_template(new_template);

        if let Some(ub) = updated_by.as_user_uuid() {
            template.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::set_ex_cache(
            self,
            object.org(),
            template.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_template_llm_recommendation(
        &self,
        object: &CacheObjectType<U, I>,
        new_recommendation: Option<String>,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut template = <Self as CacheFunctions<TemplateSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(
                sql_file!("update", "template_llm_recommendation_from_uuid"),
                &[&template.uuid(), &new_recommendation, &now, &ub_uuid],
            )
            .await
            .map_err(EpError::database)?;

        template.update_llm_recommendation(new_recommendation.clone());

        if let Some(ub) = updated_by.as_user_uuid() {
            template.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::set_ex_cache(
            self,
            object.org(),
            template.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_user_email(
        &self,
        object: &CacheObjectType<U, I>,
        new_email: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut user = <Self as CacheFunctions<UserSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "user_email_from_uuid"), &[&user.uuid(), &new_email, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        user.update_email(new_email);

        if let Some(ub) = updated_by.as_user_uuid() {
            user.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::set_ex_cache(
            self,
            object.org(),
            user.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_user_display_name(
        &self,
        object: &CacheObjectType<U, I>,
        new_display_name: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut user = <Self as CacheFunctions<UserSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(
                sql_file!("update", "user_display_name_from_uuid"),
                &[&user.uuid(), &new_display_name, &now, &ub_uuid],
            )
            .await
            .map_err(EpError::database)?;

        user.update_display_name(new_display_name);

        if let Some(ub) = updated_by.as_user_uuid() {
            user.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::set_ex_cache(
            self,
            object.org(),
            user.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_user_bio(
        &self,
        object: &CacheObjectType<U, I>,
        new_bio: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut user = <Self as CacheFunctions<UserSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "user_bio_from_uuid"), &[&user.uuid(), &new_bio, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        user.update_bio(new_bio);

        if let Some(ub) = updated_by.as_user_uuid() {
            user.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::set_ex_cache(
            self,
            object.org(),
            user.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_user_password(
        &self,
        object: &CacheObjectType<U, I>,
        new_password: String,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut user = <Self as CacheFunctions<UserSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        // Convert the new password string into the Password type so it can be serialized to bytea
        let pg_password = Password::new(new_password.clone());
        // Wrap Password in Json as the database schema expects
        let pg_password_json = postgres_types::Json(pg_password);
        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "user_password_from_uuid"), &[&user.uuid(), &pg_password_json, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        user.update_password(new_password);

        if let Some(ub) = updated_by.as_user_uuid() {
            user.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::set_ex_cache(
            self,
            object.org(),
            user.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn update_workflow_dag(
        &self,
        object: &CacheObjectType<U, I>,
        new_dag: Dag,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut workflow: WorkflowSchema =
            <Self as CacheFunctions<WorkflowSchema, U, EU, I, EI>>::get_from_cache(self, object, telemetry_wrapper).await?;

        let now = Utc::now();
        let ub_uuid = updated_by.as_updated_by_uuid();
        self.pg_connection()
            .await?
            .execute(sql_file!("update", "workflow_dag"), &[&workflow.uuid(), &new_dag, &now, &ub_uuid])
            .await
            .map_err(EpError::database)?;

        workflow.update_dag(new_dag);

        if let Some(ub) = updated_by.as_user_uuid() {
            workflow.set_updated_by(ub.clone());
        }

        <Self as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::set_ex_cache(
            self,
            object.org(),
            workflow,
            telemetry_wrapper,
        )
        .await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod method_update {
    use super::*;
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::insert::user::tests::insert_user;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::OrganizationCacheUuid;
    use ep_core::database::schema::eden_node::EdenNodeSchema;
    use ep_core::database::schema::organization::OrganizationSchema;

    pub(crate) async fn setup() -> (
        DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        TelemetryWrapper,
        UserSchema,
        EdenNodeSchema,
        OrganizationSchema,
        OrganizationCacheUuid,
    ) {
        let db_manager = create_database_manager().await;

        let mut test_telemetry = test_telemetry();

        let (user_schema, eden_node_schema, organization_schema) = initialize_organization(&db_manager, &mut test_telemetry).await;

        let org_cache_uuid = OrganizationCacheUuid::new(None, organization_schema.uuid());

        (db_manager, test_telemetry, user_schema, eden_node_schema, organization_schema, org_cache_uuid)
    }

    #[tokio::test]
    async fn update_user_description() {
        // start containers + setup test data
        let (db_manager, mut test_telemetry, _user_schema, _eden_node_schema, organization_schema, org_cache_uuid) = setup().await;

        let test_telemetry = &mut test_telemetry;

        // insert user
        let user_schema: UserSchema = insert_user(
            &db_manager,
            test_telemetry,
            eden_core::format::UserId::from("test_user"),
            eden_core::auth::Password::new("password".to_string()),
            Some("initial".to_string()),
            organization_schema.uuid(),
        )
        .await;

        // perform update (fully-qualified to avoid inference ambiguity)
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            UserSchema,
            eden_core::format::cache_uuid::UserCacheUuid,
            eden_core::format::UserUuid,
            eden_core::format::cache_id::UserCacheId,
            eden_core::format::UserId,
        >>::update_user_description(
            &db_manager,
            &CacheObjectType::from((Some(org_cache_uuid.clone()), user_schema.username().to_string())),
            "updated".to_string(),
            UpdateActor::System("infra-test"),
            test_telemetry,
        )
        .await
        .expect("update failed");

        // verify
        let fetched: UserSchema = db_manager
            .select_user_id(user_schema.username(), &organization_schema.uuid(), test_telemetry)
            .await
            .expect("select failed");

        assert_eq!(fetched.description(), Some("updated".to_string()));
    }
}
