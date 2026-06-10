pub mod api;
pub mod auth;
pub mod eden_node;
pub mod endpoint;
pub mod endpoint_group;
pub mod interlay;
pub mod organization;
pub mod rbac;
pub mod robot;
pub mod template;
#[cfg(not(embedded_db))]
mod test_utils;
pub mod user;
pub mod workflow;

use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::{cache::CacheFunctions, lib::DatabaseManager};
#[cfg(not(embedded_db))]
use crate::sql_file;
use eden_core::error::ResultEP;
use eden_core::format::{
    ApiUuid, AuthUuid, EdenId, EdenNodeUuid, EdenUuid, EndpointUuid, InterlayUuid, OrganizationUuid, RobotUuid, TemplateUuid, UserUuid,
    WorkflowUuid,
};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::EpError,
    format::{
        CacheObjectType, IdKind,
        cache_id::CacheId,
        cache_uuid::{CacheUuid, OrganizationCacheUuid},
    },
};
use ep_core::database::schema::Table;
use function_name::named;
#[cfg(not(embedded_db))]
use serde::Deserialize;
use serde::Serialize;
#[cfg(not(embedded_db))]
use serde_json::Value;
use std::borrow::Cow;
use std::future::Future;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Default, PartialEq, Serialize, ToSchema)]
pub struct UuidsToUpdate {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub apis: Vec<ApiUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub auths: Vec<AuthUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub eden_nodes: Vec<EdenNodeUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<EndpointUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub interlays: Vec<InterlayUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub organizations: Vec<OrganizationUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub robots: Vec<RobotUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub templates: Vec<TemplateUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<UserUuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<WorkflowUuid>,
}

impl UuidsToUpdate {
    pub fn api(&self) -> Vec<ApiUuid> {
        self.apis.clone()
    }
    pub fn mut_api(&mut self) -> &mut Vec<ApiUuid> {
        &mut self.apis
    }
    pub fn auth(&self) -> Vec<AuthUuid> {
        self.auths.clone()
    }
    pub fn mut_auth(&mut self) -> &mut Vec<AuthUuid> {
        &mut self.auths
    }
    pub fn eden_node(&self) -> Vec<EdenNodeUuid> {
        self.eden_nodes.clone()
    }
    pub fn mut_eden_node(&mut self) -> &mut Vec<EdenNodeUuid> {
        &mut self.eden_nodes
    }
    pub fn endpoint(&self) -> Vec<EndpointUuid> {
        self.endpoints.clone()
    }
    pub fn mut_endpoint(&mut self) -> &mut Vec<EndpointUuid> {
        &mut self.endpoints
    }
    pub fn interlay(&self) -> Vec<InterlayUuid> {
        self.interlays.clone()
    }
    pub fn mut_interlay(&mut self) -> &mut Vec<InterlayUuid> {
        &mut self.interlays
    }
    pub fn organization(&self) -> Vec<OrganizationUuid> {
        self.organizations.clone()
    }
    pub fn mut_organization(&mut self) -> &mut Vec<OrganizationUuid> {
        &mut self.organizations
    }
    pub fn robot(&self) -> Vec<RobotUuid> {
        self.robots.clone()
    }
    pub fn mut_robot(&mut self) -> &mut Vec<RobotUuid> {
        &mut self.robots
    }
    pub fn template(&self) -> Vec<TemplateUuid> {
        self.templates.clone()
    }
    pub fn mut_template(&mut self) -> &mut Vec<TemplateUuid> {
        &mut self.templates
    }
    pub fn user(&self) -> Vec<UserUuid> {
        self.users.clone()
    }
    pub fn mut_user(&mut self) -> &mut Vec<UserUuid> {
        &mut self.users
    }
    pub fn workflow(&self) -> Vec<WorkflowUuid> {
        self.workflows.clone()
    }
    pub fn mut_workflow(&mut self) -> &mut Vec<WorkflowUuid> {
        &mut self.workflows
    }
}

pub trait DeleteMethod<T, U, EU, I, EI, R, P, C>
where
    T: Table,
    U: CacheUuid,
    EU: EdenUuid,
    I: CacheId,
    EI: EdenId,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<U, I>) -> Self;
    fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<U>>;
    fn primary_object(&self) -> &CacheObjectType<U, I>;
    #[named]
    fn delete(
        &self,
        db: &DatabaseManager<R, P, C>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<UuidsToUpdate, EpError>> {
        async move {
            let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

            let removed_uuids = self
                .delete_database(db, telemetry_wrapper)
                .await
                .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

            span.add_simple_event("endpoint deleted from database");

            self.delete_cache(db, &removed_uuids, telemetry_wrapper)
                .await
                .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

            span.add_simple_event("endpoint deleted from cache");

            Ok(removed_uuids)
        }
    }
    #[named]
    fn delete_cache(
        &self,
        db: &DatabaseManager<R, P, C>,
        uuids: &UuidsToUpdate,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>> {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("cache.{}", function_name!()));

            // Delete primary-object from Cache
            let (cache_uuid, _cache_id) =
                <DatabaseManager<R, P, C> as CacheFunctions<T, U, EU, I, EI>>::invalidate(db, self.primary_object(), telemetry_wrapper)
                    .await
                    .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

            span.add_simple_event("invalidated cache");

            self.update_cache_relations(db, cache_uuid, uuids, self.primary_object().org(), telemetry_wrapper)
                .await
                .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
        }
    }
    fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_cache_uuid: U,
        uuids_to_update: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    #[cfg(not(embedded_db))]
    #[named]
    fn delete_database(
        &self,
        db: &DatabaseManager<R, P, C>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<UuidsToUpdate, EpError>> {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("database.{}", function_name!()));

            let mut conn = db
                .pg_connection()
                .await
                .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

            span.add_simple_event("collected db connection");

            let uuid =
                <DatabaseManager<R, P, C> as CacheFunctions<T, U, EU, I, EI>>::get_cache_uuid(db, self.primary_object(), telemetry_wrapper)
                    .await
                    .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?
                    .uuid();

            let mut removed_uuids = UuidsToUpdate::default();

            let transaction = conn.transaction().await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(format!("Transaction failed during record deletion: {}", e))
            })?;

            match self.primary_object().kind() {
                IdKind::Api => {
                    let rows = transaction.query(sql_file!("delete", "api"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);
                    }
                }
                IdKind::Auth => {
                    let rows = transaction.query(sql_file!("delete", "auth"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        removed_uuids.mut_auth().push(row.try_get::<_, AuthUuid>("auth").map_err(EpError::database)?);
                    }
                }
                IdKind::EdenNode => {
                    let rows = transaction.query(sql_file!("delete", "eden_node"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);

                        let uuids = row.try_get::<_, Value>("endpoint_uuids").map_err(EpError::database)?;
                        let endpoint_uuids = serde_json::from_value::<Vec<EndpointUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_endpoint().extend(endpoint_uuids);
                    }
                }
                IdKind::Endpoint => {
                    let rows = transaction.query(sql_file!("delete", "endpoint"), &[&uuid]).await.map_err(EpError::database)?;

                    for row in &rows {
                        let removed_org_uuids = row.try_get::<_, Vec<Uuid>>("organization_uuids").map_err(EpError::database)?;
                        let removed_eden_node_uuids = row.try_get::<_, Vec<Uuid>>("eden_node_uuids").map_err(EpError::database)?;
                        let removed_auth_uuids = row.try_get::<_, Vec<Uuid>>("auth_uuids").map_err(EpError::database)?;

                        removed_uuids
                            .mut_organization()
                            .extend(removed_org_uuids.into_iter().map(OrganizationUuid::from).collect::<Vec<OrganizationUuid>>());
                        removed_uuids
                            .mut_eden_node()
                            .extend(removed_eden_node_uuids.into_iter().map(EdenNodeUuid::from).collect::<Vec<EdenNodeUuid>>());
                        removed_uuids.mut_auth().extend(removed_auth_uuids.into_iter().map(AuthUuid::from).collect::<Vec<AuthUuid>>());
                    }
                }
                IdKind::EndpointGroup => {
                    let rows = transaction.query(sql_file!("delete", "endpoint_group"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);
                    }
                }
                IdKind::Interlay => {
                    let rows = transaction.query(sql_file!("delete", "interlay"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);
                    }
                }
                IdKind::ToolServer => {
                    return Err(EpError::database("Deletion for tool servers is not yet implemented"));
                }
                IdKind::Organization => {
                    #[derive(Deserialize)]
                    #[allow(dead_code)]
                    struct IdUuid {
                        id: String,
                        uuid: Uuid,
                    }

                    #[derive(Deserialize)]
                    #[allow(dead_code)]
                    struct Removed {
                        #[serde(default)]
                        apis: Vec<IdUuid>,
                        #[serde(default)]
                        auths: Vec<IdUuid>,
                        #[serde(default)]
                        endpoints: Vec<IdUuid>,
                        #[serde(default)]
                        interlays: Vec<IdUuid>,
                        #[serde(default)]
                        organization: Vec<IdUuid>,
                        #[serde(default)]
                        robots: Vec<IdUuid>,
                        #[serde(default)]
                        templates: Vec<IdUuid>,
                        #[serde(default)]
                        users: Vec<IdUuid>,
                        #[serde(default)]
                        workflows: Vec<IdUuid>,
                    }

                    let rows = transaction.query(sql_file!("delete", "organization"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let removed_json = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let removed = serde_json::from_value::<Removed>(removed_json).map_err(EpError::database)?;

                        removed_uuids
                            .mut_api()
                            .extend(removed.apis.iter().map(|iu| ApiUuid::new(iu.uuid.to_owned())).collect::<Vec<ApiUuid>>());
                        removed_uuids
                            .mut_auth()
                            .extend(removed.auths.iter().map(|iu| AuthUuid::new(iu.uuid.to_owned())).collect::<Vec<AuthUuid>>());
                        removed_uuids.mut_endpoint().extend(
                            removed.endpoints.iter().map(|iu| EndpointUuid::new(iu.uuid.to_owned())).collect::<Vec<EndpointUuid>>(),
                        );
                        removed_uuids.mut_interlay().extend(
                            removed.interlays.iter().map(|iu| InterlayUuid::new(iu.uuid.to_owned())).collect::<Vec<InterlayUuid>>(),
                        );
                        removed_uuids.mut_organization().extend(
                            removed
                                .organization
                                .iter()
                                .map(|iu| OrganizationUuid::new(iu.uuid.to_owned()))
                                .collect::<Vec<OrganizationUuid>>(),
                        );
                        removed_uuids
                            .mut_robot()
                            .extend(removed.robots.iter().map(|iu| RobotUuid::new(iu.uuid.to_owned())).collect::<Vec<RobotUuid>>());
                        removed_uuids.mut_template().extend(
                            removed.templates.iter().map(|iu| TemplateUuid::new(iu.uuid.to_owned())).collect::<Vec<TemplateUuid>>(),
                        );
                        removed_uuids
                            .mut_user()
                            .extend(removed.users.iter().map(|iu| UserUuid::new(iu.uuid.to_owned())).collect::<Vec<UserUuid>>());
                        removed_uuids.mut_workflow().extend(
                            removed.workflows.iter().map(|iu| WorkflowUuid::new(iu.uuid.to_owned())).collect::<Vec<WorkflowUuid>>(),
                        );
                    }
                }
                IdKind::Project => {
                    return Err(EpError::database("Deletion for projects is not supported by generic delete"));
                }
                IdKind::Robot => {
                    let rows = transaction.query(sql_file!("delete", "robot"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);
                    }
                }
                IdKind::Template => {
                    let rows = transaction.query(sql_file!("delete", "template"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);

                        let uuids = row.try_get::<_, Value>("workflow_uuids").map_err(EpError::database)?;
                        let workflow_uuids = serde_json::from_value::<Vec<WorkflowUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_workflow().extend(workflow_uuids);
                    }
                }
                IdKind::User => {
                    let rows = transaction.query(sql_file!("delete", "user"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);
                    }
                }
                IdKind::Workflow => {
                    let rows = transaction.query(sql_file!("delete", "workflow"), &[&uuid]).await.map_err(EpError::database)?;
                    for row in &rows {
                        let uuids = row.try_get::<_, Value>("organization_uuids").map_err(EpError::database)?;
                        let org_uuids = serde_json::from_value::<Vec<OrganizationUuid>>(uuids).map_err(EpError::database)?;
                        removed_uuids.mut_organization().extend(org_uuids);
                    }
                }
                IdKind::Policy => {
                    return Err(EpError::database("Policy deletion is handled by ELS-specific methods"));
                }
            }

            transaction.commit().await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(format!("Transaction commit failed during record deletion: {}", e))
            })?;
            Ok(removed_uuids)
        }
    }

    /// SQLite-compatible delete implementation for the `embedded-db` feature.
    ///
    /// Uses simple sequential DELETEs in a transaction instead of PostgreSQL
    /// writable CTEs, `json_agg`, `unnest`, and `json_build_object`.
    /// Junction tables are queried before deletion to collect related UUIDs
    /// that callers need for cache invalidation.
    #[cfg(embedded_db)]
    #[named]
    fn delete_database(
        &self,
        db: &DatabaseManager<R, P, C>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<UuidsToUpdate, EpError>> {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("database.{}", function_name!()));

            let conn = db
                .pg_connection()
                .await
                .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;

            span.add_simple_event("collected db connection");

            let uuid =
                <DatabaseManager<R, P, C> as CacheFunctions<T, U, EU, I, EI>>::get_cache_uuid(db, self.primary_object(), telemetry_wrapper)
                    .await
                    .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?
                    .uuid();

            let mut removed_uuids = UuidsToUpdate::default();

            let transaction = conn.transaction().await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(format!("Transaction failed during record deletion: {}", e))
            })?;

            /// Helper: query a junction table for related UUIDs before deleting.
            /// Returns the UUID column values as `Vec<Uuid>`.
            async fn collect_junction_uuids(
                tx: &crate::db::turso::TursoTransaction<'_>,
                table: &str,
                filter_col: &str,
                select_col: &str,
                id: &Uuid,
            ) -> Result<Vec<Uuid>, EpError> {
                let sql = format!("SELECT {select_col} FROM {table} WHERE {filter_col} = $1");
                let rows = tx.query(&sql, &[id]).await.map_err(EpError::database)?;
                let mut uuids = Vec::new();
                for row in &rows {
                    let val: String = row.try_get(select_col).map_err(EpError::database)?;
                    let parsed =
                        Uuid::parse_str(&val).map_err(|e| EpError::database(format!("invalid UUID in {table}.{select_col}: {e}")))?;
                    uuids.push(parsed);
                }
                Ok(uuids)
            }

            /// Helper: delete rows from a table matching a column value.
            async fn delete_where(tx: &crate::db::turso::TursoTransaction<'_>, table: &str, col: &str, id: &Uuid) -> Result<(), EpError> {
                let sql = format!("DELETE FROM {table} WHERE {col} = $1");
                tx.execute(&sql, &[id]).await.map_err(EpError::database)?;
                Ok(())
            }

            match self.primary_object().kind() {
                IdKind::Api => {
                    // Collect org UUIDs from junction table
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_apis", "api_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    // Delete junction rows, then the entity
                    delete_where(&transaction, "organization_apis", "api_uuid", &uuid).await?;
                    delete_where(&transaction, "apis", "uuid", &uuid).await?;
                }
                IdKind::Auth => {
                    // Auth has no junction tables; just delete
                    delete_where(&transaction, "auths", "uuid", &uuid).await?;
                }
                IdKind::EdenNode => {
                    // Collect related UUIDs
                    let endpoint_uuids =
                        collect_junction_uuids(&transaction, "eden_node_endpoints", "eden_node_uuid", "endpoint_uuid", &uuid).await?;
                    removed_uuids.mut_endpoint().extend(endpoint_uuids.into_iter().map(EndpointUuid::from));

                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_eden_nodes", "eden_node_uuid", "organization_uuid", &uuid)
                            .await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    // Delete junction rows, then the entity
                    delete_where(&transaction, "eden_node_endpoints", "eden_node_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_eden_nodes", "eden_node_uuid", &uuid).await?;
                    delete_where(&transaction, "eden_nodes", "uuid", &uuid).await?;
                }
                IdKind::Endpoint => {
                    // Collect related UUIDs
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_endpoints", "endpoint_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    let eden_node_uuids =
                        collect_junction_uuids(&transaction, "eden_node_endpoints", "endpoint_uuid", "eden_node_uuid", &uuid).await?;
                    removed_uuids.mut_eden_node().extend(eden_node_uuids.into_iter().map(EdenNodeUuid::from));

                    let auth_uuids = collect_junction_uuids(&transaction, "auths", "endpoint_uuid", "uuid", &uuid).await?;
                    removed_uuids.mut_auth().extend(auth_uuids.into_iter().map(AuthUuid::from));

                    // Delete junction rows, auths, then the entity
                    delete_where(&transaction, "els_policies", "endpoint_uuid", &uuid).await?;
                    delete_where(&transaction, "endpoint_group_members", "endpoint_uuid", &uuid).await?;
                    delete_where(&transaction, "eden_node_endpoints", "endpoint_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_endpoints", "endpoint_uuid", &uuid).await?;
                    delete_where(&transaction, "auths", "endpoint_uuid", &uuid).await?;
                    delete_where(&transaction, "endpoints", "uuid", &uuid).await?;
                }
                IdKind::EndpointGroup => {
                    let org_uuids = collect_junction_uuids(
                        &transaction,
                        "organization_endpoint_groups",
                        "endpoint_group_uuid",
                        "organization_uuid",
                        &uuid,
                    )
                    .await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    delete_where(&transaction, "organization_endpoint_groups", "endpoint_group_uuid", &uuid).await?;
                    delete_where(&transaction, "endpoint_group_members", "endpoint_group_uuid", &uuid).await?;
                    delete_where(&transaction, "endpoint_groups", "uuid", &uuid).await?;
                }
                IdKind::Interlay => {
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_interlays", "interlay_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    delete_where(&transaction, "organization_interlays", "interlay_uuid", &uuid).await?;
                    delete_where(&transaction, "interlays", "uuid", &uuid).await?;
                }
                IdKind::ToolServer => {
                    return Err(EpError::database("Deletion for tool servers is not yet implemented"));
                }
                IdKind::Organization => {
                    // Collect all related entity UUIDs before deleting anything
                    let api_uuids =
                        collect_junction_uuids(&transaction, "organization_apis", "organization_uuid", "api_uuid", &uuid).await?;
                    let workflow_uuids =
                        collect_junction_uuids(&transaction, "organization_workflows", "organization_uuid", "workflow_uuid", &uuid).await?;
                    let template_uuids =
                        collect_junction_uuids(&transaction, "organization_templates", "organization_uuid", "template_uuid", &uuid).await?;
                    let endpoint_uuids =
                        collect_junction_uuids(&transaction, "organization_endpoints", "organization_uuid", "endpoint_uuid", &uuid).await?;
                    let interlay_uuids =
                        collect_junction_uuids(&transaction, "organization_interlays", "organization_uuid", "interlay_uuid", &uuid).await?;
                    let robot_uuids =
                        collect_junction_uuids(&transaction, "organization_robots", "organization_uuid", "robot_uuid", &uuid).await?;
                    let user_uuids =
                        collect_junction_uuids(&transaction, "organization_users", "organization_uuid", "user_uuid", &uuid).await?;

                    // Collect auth UUIDs from the endpoints being deleted
                    let mut auth_uuids = Vec::new();
                    for ep_uuid in &endpoint_uuids {
                        let auths = collect_junction_uuids(&transaction, "auths", "endpoint_uuid", "uuid", ep_uuid).await?;
                        auth_uuids.extend(auths);
                    }

                    // Populate removed_uuids
                    removed_uuids.mut_api().extend(api_uuids.iter().map(|u| ApiUuid::from(*u)));
                    removed_uuids.mut_auth().extend(auth_uuids.iter().map(|u| AuthUuid::from(*u)));
                    removed_uuids.mut_endpoint().extend(endpoint_uuids.iter().map(|u| EndpointUuid::from(*u)));
                    removed_uuids.mut_interlay().extend(interlay_uuids.iter().map(|u| InterlayUuid::from(*u)));
                    removed_uuids.mut_organization().push(OrganizationUuid::from(uuid));
                    removed_uuids.mut_robot().extend(robot_uuids.iter().map(|u| RobotUuid::from(*u)));
                    removed_uuids.mut_template().extend(template_uuids.iter().map(|u| TemplateUuid::from(*u)));
                    removed_uuids.mut_user().extend(user_uuids.iter().map(|u| UserUuid::from(*u)));
                    removed_uuids.mut_workflow().extend(workflow_uuids.iter().map(|u| WorkflowUuid::from(*u)));

                    // Delete cross-entity junction tables first
                    for wf_uuid in &workflow_uuids {
                        delete_where(&transaction, "workflow_templates", "workflow_uuid", wf_uuid).await?;
                    }
                    for ep_uuid in &endpoint_uuids {
                        delete_where(&transaction, "eden_node_endpoints", "endpoint_uuid", ep_uuid).await?;
                    }
                    // Delete organization junction tables
                    delete_where(&transaction, "organization_apis", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_workflows", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_templates", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_endpoints", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_interlays", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_robots", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_eden_nodes", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_admins", "organization_uuid", &uuid).await?;
                    delete_where(&transaction, "organization_users", "organization_uuid", &uuid).await?;

                    // Delete entities
                    for wf_uuid in &workflow_uuids {
                        delete_where(&transaction, "workflows", "uuid", wf_uuid).await?;
                    }
                    for t_uuid in &template_uuids {
                        delete_where(&transaction, "templates", "uuid", t_uuid).await?;
                    }
                    for a_uuid in &auth_uuids {
                        delete_where(&transaction, "auths", "uuid", a_uuid).await?;
                    }
                    for ep_uuid in &endpoint_uuids {
                        delete_where(&transaction, "endpoints", "uuid", ep_uuid).await?;
                    }
                    for il_uuid in &interlay_uuids {
                        delete_where(&transaction, "interlays", "uuid", il_uuid).await?;
                    }
                    for api_uuid in &api_uuids {
                        delete_where(&transaction, "apis", "uuid", api_uuid).await?;
                    }
                    for r_uuid in &robot_uuids {
                        delete_where(&transaction, "robots", "uuid", r_uuid).await?;
                    }
                    for u_uuid in &user_uuids {
                        delete_where(&transaction, "users", "uuid", u_uuid).await?;
                    }

                    // Finally delete the organization itself
                    delete_where(&transaction, "organizations", "uuid", &uuid).await?;
                }
                IdKind::Project => {
                    return Err(EpError::database("Deletion for projects is not supported by generic delete"));
                }
                IdKind::Robot => {
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_robots", "robot_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    delete_where(&transaction, "organization_robots", "robot_uuid", &uuid).await?;
                    delete_where(&transaction, "robots", "uuid", &uuid).await?;
                }
                IdKind::Template => {
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_templates", "template_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    let wf_uuids =
                        collect_junction_uuids(&transaction, "workflow_templates", "template_uuid", "workflow_uuid", &uuid).await?;
                    removed_uuids.mut_workflow().extend(wf_uuids.into_iter().map(WorkflowUuid::from));

                    delete_where(&transaction, "organization_templates", "template_uuid", &uuid).await?;
                    delete_where(&transaction, "workflow_templates", "template_uuid", &uuid).await?;
                    delete_where(&transaction, "templates", "uuid", &uuid).await?;
                }
                IdKind::User => {
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_users", "user_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    delete_where(&transaction, "organization_users", "user_uuid", &uuid).await?;
                    delete_where(&transaction, "users", "uuid", &uuid).await?;
                }
                IdKind::Workflow => {
                    let org_uuids =
                        collect_junction_uuids(&transaction, "organization_workflows", "workflow_uuid", "organization_uuid", &uuid).await?;
                    removed_uuids.mut_organization().extend(org_uuids.into_iter().map(OrganizationUuid::from));

                    delete_where(&transaction, "organization_workflows", "workflow_uuid", &uuid).await?;
                    delete_where(&transaction, "workflow_templates", "workflow_uuid", &uuid).await?;
                    delete_where(&transaction, "workflows", "uuid", &uuid).await?;
                }
                IdKind::Policy => {
                    return Err(EpError::database("Policy deletion is handled by ELS-specific methods"));
                }
            }

            transaction.commit().await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(format!("Transaction commit failed during record deletion: {}", e))
            })?;
            Ok(removed_uuids)
        }
    }
}
