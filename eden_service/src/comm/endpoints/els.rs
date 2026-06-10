//! # ELS Policy Management API
//!
//! Named policies per endpoint with user assignments in sync or copy mode.
//!
//! ## Policy CRUD
//! - `POST   /{endpoint}/els/policies` — create
//! - `GET    /{endpoint}/els/policies` — list
//! - `GET    /{endpoint}/els/policies/{policy_uuid}` — get
//! - `PUT    /{endpoint}/els/policies/{policy_uuid}` — update (re-caches sync'd users)
//! - `DELETE /{endpoint}/els/policies/{policy_uuid}` — delete (cascades)
//! - `DELETE /{endpoint}/els/policies` — delete all
//! - `POST   /{endpoint}/els/validate` — validate a strategy/config pair
//!
//! ## Version Lifecycle
//! - `POST   /{endpoint}/els/policies/{policy_uuid}/versions` — create draft version
//! - `GET    /{endpoint}/els/policies/{policy_uuid}/versions` — list versions
//! - `GET    /{endpoint}/els/policies/{policy_uuid}/versions/active` — get the active version directly
//! - `GET    /{endpoint}/els/policies/{policy_uuid}/versions/{version}` — get version
//! - `GET    /{endpoint}/els/policies/{policy_uuid}/pointer` — get active pointer
//! - `POST   /{endpoint}/els/policies/{policy_uuid}/versions/{version}/promote` — promote
//! - `POST   /{endpoint}/els/policies/{policy_uuid}/versions/{version}/reject` — reject a draft
//! - `POST   /{endpoint}/els/policies/{policy_uuid}/versions/{version}/rollback` — rollback
//!
//! ## User Assignment CRUD
//! - `PUT    /{endpoint}/els/users/{user_uuid}` — assign
//! - `GET    /{endpoint}/els/users/{user_uuid}` — get effective policy
//! - `GET    /{endpoint}/els/users` — list assignments
//! - `POST   /{endpoint}/els/users/{user_uuid}/refresh` — refresh a copy-mode snapshot
//! - `POST   /{endpoint}/els/users/unassign` — bulk unassign selected users
//! - `DELETE /{endpoint}/els/users/{user_uuid}` — unassign
//! - `DELETE /{endpoint}/els/users` — unassign all

use crate::comm::rbac::verify_control_perms;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::els::{
    AssignPolicyRequest, BulkAssignUsersRequest, BulkUnassignUsersRequest, CreatePolicyRequest, CreateVersionRequest,
    ELS_DEFAULT_PAGE_LIMIT, ELS_MAX_PAGE_LIMIT, ElsCommands, ElsPolicyRedacted, ElsPolicyVersionRedacted, ElsStrategy,
    PaginationParams as DbPaginationParams, PromoteVersionRequest, RollbackVersionRequest, UpdatePolicyRequest,
    UserPolicyAssignmentRedacted, ValidatePolicyRequest,
};
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, PolicyUuid, UserUuid, parse_kind_uuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_schema::endpoint::EndpointSchema;
use serde::Deserialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::IntoParams;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_uuid(s: &str, label: &str) -> Result<uuid::Uuid, actix_web::Error> {
    uuid::Uuid::parse_str(s).map_err(|e| EpError::parse(format!("Invalid {label}: {e}")).into())
}

fn parse_user_uuid(s: &str) -> Result<UserUuid, actix_web::Error> {
    let uuid = parse_uuid(s, "user UUID")?;
    Ok(UserUuid::from(uuid))
}

fn parse_policy_uuid(s: &str) -> Result<PolicyUuid, actix_web::Error> {
    let uuid = parse_uuid(s, "policy UUID")?;
    Ok(PolicyUuid::from(uuid))
}

const fn default_pagination_limit() -> i64 {
    ELS_DEFAULT_PAGE_LIMIT
}

const fn default_pagination_offset() -> i64 {
    0
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PaginationParams {
    /// Number of items to return (default: 50, max: 1000)
    #[serde(default = "default_pagination_limit")]
    pub limit: i64,
    /// Number of items to skip (default: 0)
    #[serde(default = "default_pagination_offset")]
    pub offset: i64,
}

impl PaginationParams {
    fn validate(&self) -> Result<(), actix_web::Error> {
        if self.limit <= 0 || self.limit > ELS_MAX_PAGE_LIMIT {
            return Err(actix_web::error::ErrorBadRequest("`limit` must be between 1 and 1000"));
        }
        if self.offset < 0 {
            return Err(actix_web::error::ErrorBadRequest("`offset` must be >= 0"));
        }
        Ok(())
    }

    fn into_db(self) -> DbPaginationParams {
        DbPaginationParams { limit: self.limit, offset: self.offset }
    }
}

fn validate_bulk_user_uuids(user_uuids: &[UserUuid]) -> Result<(), actix_web::Error> {
    if user_uuids.is_empty() {
        return Err(actix_web::error::ErrorBadRequest("`user_uuids` may not be empty"));
    }
    if user_uuids.len() > 1_000 {
        return Err(actix_web::error::ErrorBadRequest("`user_uuids` may contain at most 1000 entries"));
    }
    Ok(())
}

/// Resolve endpoint and verify Admin RBAC. Returns (endpoint_schema, endpoint_cache_uuid).
/// **Permissions**: See exact permission-bit checks in the handler body.
async fn resolve_endpoint(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    auth: &ParsedJwt,
    endpoint_name: String,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<(EndpointSchema, EndpointCacheUuid), actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let organization_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_schema = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
        EndpointSchema,
        EndpointCacheUuid,
        EndpointUuid,
        EndpointCacheId,
        EndpointId,
    >>::get_from_cache(
        database,
        &CacheObjectType::from((Some(organization_cache_uuid.clone()), endpoint_name)),
        telemetry_wrapper,
    )
    .await?;

    verify_control_perms(database, auth, Some(endpoint_schema.endpoint_uuid()), ControlPerms::CONFIGURE, telemetry_wrapper).await?;

    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache_uuid), endpoint_schema.uuid());
    Ok((endpoint_schema, endpoint_cache_uuid))
}

/// Validate that the given ELS strategy matches the endpoint type and that the
/// config shape is valid for that strategy.
async fn validate_els_strategy(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    endpoint_schema: &EndpointSchema,
    endpoint_cache_uuid: &EndpointCacheUuid,
    strategy: &ElsStrategy,
    config: &serde_json::Value,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<(), actix_web::Error> {
    let expected = ElsStrategy::from_ep_kind(endpoint_schema.kind())
        .ok_or_else(|| EpError::parse(format!("ELS is not supported for {:?} endpoints", endpoint_schema.kind())))?;
    if *strategy != expected {
        return Err(
            EpError::parse(format!("Strategy mismatch: {:?} endpoints require {:?} strategy", endpoint_schema.kind(), expected)).into(),
        );
    }
    strategy.validate_config(config)?;

    if *strategy == ElsStrategy::Redis {
        if let Some(target_endpoint_uuid) = config.get("endpoint_uuid").and_then(serde_json::Value::as_str) {
            let target_endpoint_uuid = parse_kind_uuid::<EndpointUuid>(target_endpoint_uuid)
                .map_err(|e| EpError::parse(format!("Invalid Redis ELS endpoint_uuid: {e}")))?;
            let target_cache_uuid = EndpointCacheUuid::new(endpoint_cache_uuid.org(), target_endpoint_uuid);
            let target_schema =
                <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                    EndpointSchema,
                    EndpointCacheUuid,
                    EndpointUuid,
                    EndpointCacheId,
                    EndpointId,
                >>::get_from_cache(database, &CacheObjectType::new(Some(target_cache_uuid), None), telemetry_wrapper)
                .await?;

            if target_schema.kind() != endpoint_schema.kind() {
                return Err(EpError::parse(format!(
                    "Redis ELS endpoint switch target must also be {:?}, got {:?}",
                    endpoint_schema.kind(),
                    target_schema.kind()
                ))
                .into());
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Policy CRUD
// ---------------------------------------------------------------------------

/// Create a named ELS policy for an endpoint.
///
/// POST `/{endpoint}/els/policies`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies",
    operation_id = "create_els_policy",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn create_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    body: web::Json<CreatePolicyRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint_name = endpoint.into_inner();
    let (endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    validate_els_strategy(&database, &endpoint_schema, &endpoint_cache_uuid, &body.strategy, &body.config, telemetry_wrapper).await?;

    let policy_uuid = database.els_create_policy(&endpoint_cache_uuid, &body).await?;

    EdenResponse::response(serde_json::json!({ "policy_uuid": policy_uuid })).into()
}

/// Validate an ELS strategy/config pair for an endpoint without storing it.
///
/// POST `/{endpoint}/els/validate`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/validate",
    operation_id = "validate_els_policy",
    request_body = ValidatePolicyRequest,
    responses((status = OK, body = serde_json::Value))
)]
pub async fn validate_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    body: web::Json<ValidatePolicyRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint_name = endpoint.into_inner();
    let (endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    validate_els_strategy(&database, &endpoint_schema, &endpoint_cache_uuid, &body.strategy, &body.config, telemetry_wrapper).await?;

    EdenResponse::response(serde_json::json!({
        "valid": true,
        "strategy": body.strategy,
    }))
    .into()
}

/// Get a specific ELS policy.
///
/// GET `/{endpoint}/els/policies/{policy_uuid}`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}",
    operation_id = "get_els_policy",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn get_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let policy = database.els_get_policy(&endpoint_cache_uuid, &policy_uuid).await?;

    // Redact credentials from API response
    let redacted = policy.map(ElsPolicyRedacted::from);
    EdenResponse::response(redacted).into()
}

/// List all ELS policies for an endpoint.
///
/// GET `/{endpoint}/els/policies`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies",
    operation_id = "list_els_policies",
    params(PaginationParams),
    responses((status = OK, body = serde_json::Value))
)]
pub async fn list_policies(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    query: web::Query<PaginationParams>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    query.validate()?;
    let endpoint_name = endpoint.into_inner();
    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let policies = database.els_list_policies(&endpoint_cache_uuid, query.into_inner().into_db()).await?;

    let redacted = policies.map_items(ElsPolicyRedacted::from);
    EdenResponse::response(redacted).into()
}

/// Update an ELS policy (re-caches sync'd users).
///
/// PUT `/{endpoint}/els/policies/{policy_uuid}`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}",
    operation_id = "update_els_policy",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn update_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    body: web::Json<UpdatePolicyRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    validate_els_strategy(&database, &endpoint_schema, &endpoint_cache_uuid, &body.strategy, &body.config, telemetry_wrapper).await?;

    database.els_update_policy(&endpoint_cache_uuid, &policy_uuid, &body.strategy, &body.config).await?;

    EdenResponse::<String>::ok("els policy updated").into()
}

/// Delete an ELS policy (cascades to assignments).
///
/// DELETE `/{endpoint}/els/policies/{policy_uuid}`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}",
    operation_id = "delete_els_policy",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn delete_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let deleted = database.els_delete_policy(&endpoint_cache_uuid, &policy_uuid).await?;

    if !deleted {
        return Err(actix_web::error::ErrorNotFound("ELS policy not found"));
    }
    EdenResponse::<String>::ok("els policy deleted").into()
}

/// Delete all ELS policies for an endpoint (cascades to assignments).
///
/// DELETE `/{endpoint}/els/policies`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies",
    operation_id = "delete_all_els_policies",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn delete_all_policies(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint_name = endpoint.into_inner();
    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    database.els_delete_all_policies(&endpoint_cache_uuid).await?;

    EdenResponse::<String>::ok("all els policies deleted").into()
}

// ---------------------------------------------------------------------------
// User Assignment CRUD
// ---------------------------------------------------------------------------

/// Assign a policy to a user.
///
/// PUT `/{endpoint}/els/users/{user_uuid}`
///
/// Body: `{ "policy_uuid": "...", "mode": "sync" }`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users/{user_uuid}",
    operation_id = "assign_els_user",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn assign_user(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    body: web::Json<AssignPolicyRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, user_uuid_str) = path.into_inner();
    let target_user_uuid = parse_user_uuid(&user_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    database.els_assign_user(&endpoint_cache_uuid, &target_user_uuid, &body).await?;

    EdenResponse::<String>::ok("els policy assigned").into()
}

/// Assign a policy to many users at once.
///
/// PUT `/{endpoint}/els/users`
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users",
    operation_id = "assign_els_users_bulk",
    request_body = BulkAssignUsersRequest,
    responses((status = OK, body = serde_json::Value))
)]
pub async fn assign_users(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    body: web::Json<BulkAssignUsersRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    validate_bulk_user_uuids(&body.user_uuids)?;

    let endpoint_name = endpoint.into_inner();
    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let summary = database.els_assign_users(&endpoint_cache_uuid, &body).await?;
    EdenResponse::response(summary).into()
}

/// Get a user's effective (resolved) ELS policy.
///
/// GET `/{endpoint}/els/users/{user_uuid}`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users/{user_uuid}",
    operation_id = "get_els_user_policy",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn get_user_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, user_uuid_str) = path.into_inner();
    let target_user_uuid = parse_user_uuid(&user_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let assignment = database.els_get_user_policy(&endpoint_cache_uuid, &target_user_uuid).await?;

    // Redact credentials from API response
    let redacted = assignment.map(UserPolicyAssignmentRedacted::from);
    EdenResponse::response(redacted).into()
}

/// Refresh a copy-mode assignment from the policy's current effective config.
///
/// POST `/{endpoint}/els/users/{user_uuid}/refresh`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users/{user_uuid}/refresh",
    operation_id = "refresh_copy_mode_els_user",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn refresh_user_policy(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, user_uuid_str) = path.into_inner();
    let target_user_uuid = parse_user_uuid(&user_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    database.els_refresh_user_policy(&endpoint_cache_uuid, &target_user_uuid).await?;

    EdenResponse::<String>::ok("copy-mode els assignment refreshed").into()
}

/// List all user assignments for an endpoint.
///
/// GET `/{endpoint}/els/users`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users",
    operation_id = "list_els_user_assignments",
    params(PaginationParams),
    responses((status = OK, body = serde_json::Value))
)]
pub async fn list_user_assignments(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    query: web::Query<PaginationParams>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    query.validate()?;
    let endpoint_name = endpoint.into_inner();
    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let assignments = database.els_list_user_assignments(&endpoint_cache_uuid, query.into_inner().into_db()).await?;

    let redacted = assignments.map_items(UserPolicyAssignmentRedacted::from);
    EdenResponse::response(redacted).into()
}

/// Unassign a user from their ELS policy.
///
/// DELETE `/{endpoint}/els/users/{user_uuid}`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users/{user_uuid}",
    operation_id = "unassign_els_user",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn unassign_user(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, user_uuid_str) = path.into_inner();
    let target_user_uuid = parse_user_uuid(&user_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let deleted = database.els_unassign_user(&endpoint_cache_uuid, &target_user_uuid).await?;

    if !deleted {
        return Err(actix_web::error::ErrorNotFound("ELS assignment not found"));
    }
    EdenResponse::<String>::ok("els policy unassigned").into()
}

/// Unassign all users from ELS policies for an endpoint.
///
/// DELETE `/{endpoint}/els/users`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users",
    operation_id = "unassign_all_els_users",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn unassign_all(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint_name = endpoint.into_inner();
    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    database.els_unassign_all(&endpoint_cache_uuid).await?;

    EdenResponse::<String>::ok("all els assignments removed").into()
}

/// Unassign selected users from ELS policies for an endpoint.
///
/// POST `/{endpoint}/els/users/unassign`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/users/unassign",
    operation_id = "bulk_unassign_els_users",
    request_body = BulkUnassignUsersRequest,
    responses((status = OK, body = serde_json::Value))
)]
pub async fn unassign_users(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    body: web::Json<BulkUnassignUsersRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint_name = endpoint.into_inner();
    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    validate_bulk_user_uuids(&body.user_uuids)?;
    let removed = database.els_unassign_users(&endpoint_cache_uuid, &body.user_uuids).await?;

    EdenResponse::response(serde_json::json!({ "unassigned": removed })).into()
}

// ---------------------------------------------------------------------------
// Version Lifecycle
// ---------------------------------------------------------------------------

/// Create a new draft version for a policy.
///
/// POST `/{endpoint}/els/policies/{policy_uuid}/versions`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions",
    operation_id = "create_els_version",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn create_version(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    body: web::Json<CreateVersionRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    validate_els_strategy(&database, &endpoint_schema, &endpoint_cache_uuid, &body.strategy, &body.config, telemetry_wrapper).await?;

    let caller = auth.user_uuid().clone();
    let version = database.els_create_version(&endpoint_cache_uuid, &policy_uuid, &body.strategy, &body.config, &caller).await?;

    EdenResponse::response(serde_json::json!({ "version": version })).into()
}

/// List all versions for a policy (newest first).
///
/// GET `/{endpoint}/els/policies/{policy_uuid}/versions`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions",
    operation_id = "list_els_versions",
    params(PaginationParams),
    responses((status = OK, body = serde_json::Value))
)]
pub async fn list_versions(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    query: web::Query<PaginationParams>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    query.validate()?;
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let versions = database.els_list_versions(&endpoint_cache_uuid, &policy_uuid, query.into_inner().into_db()).await?;

    let redacted = versions.map_items(ElsPolicyVersionRedacted::from);
    EdenResponse::response(redacted).into()
}

/// Get the currently active version directly.
///
/// GET `/{endpoint}/els/policies/{policy_uuid}/versions/active`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/active",
    operation_id = "get_active_els_version",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn get_active_version(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let pointer = database.els_get_pointer(&policy_uuid).await?;
    let active_version = pointer
        .and_then(|pointer| pointer.active_version)
        .ok_or_else(|| actix_web::error::ErrorNotFound("No active ELS version"))?;
    let version = database
        .els_get_version(&endpoint_cache_uuid, &policy_uuid, active_version)
        .await?
        .ok_or_else(|| actix_web::error::ErrorNotFound("Active ELS version not found"))?;

    let redacted = ElsPolicyVersionRedacted::from(version);
    EdenResponse::response(redacted).into()
}

/// Get a specific version of a policy.
///
/// GET `/{endpoint}/els/policies/{policy_uuid}/versions/{version}`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}",
    operation_id = "get_els_version",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn get_version(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String, i32)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str, version) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let v = database.els_get_version(&endpoint_cache_uuid, &policy_uuid, version).await?;

    let redacted = v.map(ElsPolicyVersionRedacted::from);
    EdenResponse::response(redacted).into()
}

/// Get the active version pointer for a policy.
///
/// GET `/{endpoint}/els/policies/{policy_uuid}/pointer`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/pointer",
    operation_id = "get_els_pointer",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn get_pointer(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    // Silence unused variable warning — endpoint_cache_uuid is needed for RBAC but
    // els_get_pointer only needs the policy_uuid.
    let _ = &endpoint_cache_uuid;

    let pointer = database.els_get_pointer(&policy_uuid).await?;

    EdenResponse::response(pointer).into()
}

/// Promote a draft version to active.
///
/// POST `/{endpoint}/els/policies/{policy_uuid}/versions/{version}/promote`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}/promote",
    operation_id = "promote_els_version",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn promote_version(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String, i32)>,
    body: web::Json<PromoteVersionRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str, version) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let caller = auth.user_uuid().clone();
    database.els_promote_version(&endpoint_cache_uuid, &policy_uuid, version, body.expected_current, &caller).await?;

    EdenResponse::<String>::ok("version promoted").into()
}

/// Reject a draft version.
///
/// POST `/{endpoint}/els/policies/{policy_uuid}/versions/{version}/reject`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}/reject",
    operation_id = "reject_els_version",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn reject_version(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String, i32)>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str, version) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    database.els_reject_version(&endpoint_cache_uuid, &policy_uuid, version).await?;

    EdenResponse::<String>::ok("version rejected").into()
}

/// Rollback to a previously superseded version.
///
/// POST `/{endpoint}/els/policies/{policy_uuid}/versions/{version}/rollback`
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}/rollback",
    operation_id = "rollback_els_version",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn rollback_version(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String, i32)>,
    body: web::Json<RollbackVersionRequest>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint_name, policy_uuid_str, target_version) = path.into_inner();
    let policy_uuid = parse_policy_uuid(&policy_uuid_str)?;

    let (_endpoint_schema, endpoint_cache_uuid) = resolve_endpoint(&database, &auth, endpoint_name, telemetry_wrapper).await?;

    let caller = auth.user_uuid().clone();
    database.els_rollback(&endpoint_cache_uuid, &policy_uuid, target_version, body.expected_current, &caller).await?;

    EdenResponse::<String>::ok("version rolled back").into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn pagination_validation_enforces_bounds() {
        assert!(PaginationParams { limit: 1, offset: 0 }.validate().is_ok());
        assert!(PaginationParams { limit: ELS_MAX_PAGE_LIMIT, offset: 0 }.validate().is_ok());
        assert!(PaginationParams { limit: 0, offset: 0 }.validate().is_err());
        assert!(PaginationParams { limit: ELS_MAX_PAGE_LIMIT + 1, offset: 0 }.validate().is_err());
        assert!(PaginationParams { limit: 10, offset: -1 }.validate().is_err());
    }

    #[test]
    fn bulk_user_validation_rejects_empty_and_oversized_requests() {
        assert!(validate_bulk_user_uuids(&[]).is_err());

        let valid = vec![UserUuid::from(Uuid::new_v4())];
        assert!(validate_bulk_user_uuids(&valid).is_ok());

        let oversized = (0..=1_000).map(|_| UserUuid::from(Uuid::new_v4())).collect::<Vec<UserUuid>>();
        assert!(validate_bulk_user_uuids(&oversized).is_err());
    }
}
