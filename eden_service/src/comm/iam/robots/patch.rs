use crate::EdenDb;
use crate::comm::iam::robots::post::ALLOWED_AGENT_PERMS;
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
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, IdKind, OrganizationUuid, RobotId, RobotUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::robot::RobotSchema;
use endpoint_core::ep_core::settings::EdenSettings;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Partial robot update payload (JSON `PATCH` body).
///
/// Semantics:
/// - `None` on any field means "leave the current value unchanged".
/// - **`ttl`**: positive values set `expires_at = now + ttl` in the database;
///   zero or negative values clear `expires_at` to `NULL` (no expiration).
///   There is currently no way to unset `ttl` back to `None` via this
///   endpoint — only to replace it with a new value.
/// - To rotate a robot's API key, use `POST /iam/agents/{agent}/rotate-key`.
#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct OptionalRobotInput {
    /// New description. `None` leaves the current value unchanged.
    pub description: Option<String>,
    /// New TTL in seconds. `None` leaves unchanged.
    pub ttl: Option<i64>,
    /// New org-scoped agent control-plane permission bits. `None` leaves unchanged.
    pub perms: Option<ControlPerms>,
}

/// Update an existing Robot (Machine Account)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["IAM"],
    path="/iam/agents/{agent}",
    operation_id = "update_agent",
    request_body = OptionalRobotInput,
    responses((status = OK, body = EdenResponse<Response>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    db_manager: web::Data<EdenDb>,
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    robot: web::Path<String>,
    input: web::Json<OptionalRobotInput>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    // Only admins can update robots
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    if let Some(perms) = input.perms
        && !(perms - ALLOWED_AGENT_PERMS).is_empty()
    {
        return Err(actix_web::error::ErrorBadRequest(
            "Agents can only hold READ, CONFIGURE, and AUDIT organization permissions",
        ));
    }

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

    if let Some(new_description) = &input.description {
        <EdenDb as UpdateMethod<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::update_robot_description(
            &db_manager,
            &robot_object,
            new_description.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    }

    if let Some(new_ttl) = input.ttl {
        <EdenDb as UpdateMethod<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::update_robot_ttl(
            &db_manager,
            &robot_object,
            new_ttl,
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    }

    if let Some(perms) = input.perms {
        let version_ms = chrono::Utc::now().timestamp_millis();
        let data = ControlPlaneRbacData {
            org_uuid: org_cache.uuid(),
            entity_kind: IdKind::Organization.as_str().to_owned(),
            entity_uuid: org_cache.uuid(),
            subject_kind: IdKind::Robot.as_str().to_owned(),
            subject_uuid: robot_cache.uuid(),
            perms,
        };
        database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;
    }

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
    let response = Response::from_updated(updated_robot, verbose, auth.org_uuid().clone(), perms);

    EdenResponse::response(response).into()
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
/// Robot patch response payload.
/// `id`, `uuid`, `org_uuid`, and `perms` are always present.
/// Verbose metadata fields are only present when `verbose=true`.
pub struct Response {
    /// Robot username (always present).
    pub id: RobotId,
    /// Robot UUID (always present).
    pub uuid: RobotUuid,
    /// Exact organization control-plane permission bits (always present).
    pub perms: ControlPerms,
    /// Organization UUID from the request context (always present).
    pub org_uuid: OrganizationUuid,
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

impl Response {
    fn from_updated(schema: RobotSchema, verbose: bool, org_uuid: OrganizationUuid, perms: ControlPerms) -> Self {
        let updated_at = if verbose { Some(schema.updated_at().to_rfc3339()) } else { None };
        let description = if verbose { schema.description() } else { None };
        let ttl = if verbose { schema.ttl() } else { None };

        Self {
            id: schema.id(),
            uuid: schema.uuid(),
            perms,
            org_uuid,
            updated_at,
            description,
            ttl,
        }
    }
}
