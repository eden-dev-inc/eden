use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::rbac::ControlPlaneRbac;
use database::methods::update::{UpdateActor, UpdateMethod};
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::RobotCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, OrganizationUuid, RobotId, RobotUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::robot::RobotSchema;
use endpoint_core::ep_core::settings::EdenSettings;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Rotate API key for an existing Robot (Machine Account)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["IAM"],
    path="/iam/agents/{agent}/rotate-key",
    operation_id = "rotate_agent_api_key",
    responses((status = OK, body = EdenResponse<RotateKeyResponse>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    db_manager: web::Data<EdenDb>,
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    robot: web::Path<String>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    // Only admins can rotate robot keys
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let robot_cache = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), robot.clone())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| actix_web::error::ErrorBadRequest(format!("robot {robot} doesn't exist: {e}")))?;

    // Robot updates target the `WHERE uuid = $1` SQL, so use a uuid-based
    // CacheObjectType here. Passing the username path string would let
    // `update_cache` fall through to the id branch and bind a String to a
    // UUID column, failing with a serializer error.
    let robot_object: CacheObjectType<RobotCacheUuid, RobotCacheId> = CacheObjectType::new(Some(robot_cache.clone()), None);

    log::info!(
        "Robot API key rotation requested: robot={} org_uuid={} user_uuid={} route=rotate-key",
        robot,
        auth.org_uuid(),
        auth.user_uuid()
    );

    let (plaintext, _hashed) = eden_core::auth::ApiKey::generate();
    <EdenDb as UpdateMethod<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::update_robot_api_key(
        &db_manager,
        &robot_object,
        plaintext.clone(),
        UpdateActor::User(auth.user_uuid()),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    log::info!(
        "Robot API key rotation succeeded: robot={} org_uuid={} user_uuid={} route=rotate-key",
        robot,
        auth.org_uuid(),
        auth.user_uuid()
    );

    // Get the updated robot data
    let updated_robot: RobotSchema = db_manager
        .select_robot_uuid(&RobotUuid::from(robot_cache.uuid()), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Get the updated exact permission bits
    let updated_robot_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), IdKind::Robot, robot_cache.uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let perms = updated_robot_entries
        .iter()
        .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid())
        .map(|entry| entry.perms)
        .unwrap_or(ControlPerms::empty());

    let verbose = EdenSettings::from(_req.headers()).verbose();
    let updated_at = if verbose {
        Some(updated_robot.updated_at().to_rfc3339())
    } else {
        None
    };
    let description = if verbose { updated_robot.description() } else { None };
    let ttl = if verbose { updated_robot.ttl() } else { None };
    let response = RotateKeyResponse {
        id: updated_robot.id(),
        uuid: updated_robot.uuid(),
        perms,
        org_uuid: auth.org_uuid().clone(),
        api_key: plaintext,
        updated_at,
        description,
        ttl,
    };

    EdenResponse::response(response).into()
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
/// Robot key rotation response payload.
/// `api_key` contains the new plaintext key — only returned once.
pub struct RotateKeyResponse {
    /// Robot username.
    pub id: RobotId,
    /// Robot UUID.
    pub uuid: RobotUuid,
    /// Exact organization control-plane permission bits.
    pub perms: ControlPerms,
    /// Organization UUID from the request context.
    pub org_uuid: OrganizationUuid,
    /// The new plaintext API key. Only returned once at rotation time.
    pub api_key: String,
    /// Last update timestamp, only included when `verbose=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// Description, only included when `verbose=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// TTL in seconds, only included when `verbose=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
}
