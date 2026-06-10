use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::RobotCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, RobotId, RobotUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::robot::RobotSchema;
use endpoint_core::ep_core::settings::EdenSettings;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

type RobotCacheMgr = EdenDb;

fn robot_not_found_error() -> EpError {
    EpError::rbac("Robot has been deleted or has no access in this organization")
}

/// Get a Robot (Machine Account)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/agents/{agent}",
    operation_id = "get_agent",
    responses((status = OK, body = Response))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    robot: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let robot = robot.into_inner();

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let robot_cache = <RobotCacheMgr as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_key.clone()), robot.clone())),
        telemetry_wrapper,
    )
    .await
    .map_err(|_| robot_not_found_error())
    .map_err(|e| error_handling(e, &mut span))?;

    // Check if the robot still has RBAC access in the organization
    let entries = database
        .control_plane_list_by_subject(org_key.uuid(), IdKind::Robot, robot_cache.uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    if !entries.iter().any(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_key.uuid()) {
        return Err(error_handling(robot_not_found_error(), &mut span));
    }

    let robot_cache_object = CacheObjectType::new(Some(robot_cache.clone()), None);
    let robot_schema = <RobotCacheMgr as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_from_cache(
        &database,
        &robot_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|_| robot_not_found_error())
    .map_err(|e| error_handling(e, &mut span))?;

    let verbose = EdenSettings::from(req.headers()).verbose();
    let response = Response::from((robot_schema, verbose));

    EdenResponse::response(response).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response {
    pub uuid: RobotUuid,
    pub username: RobotId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl From<(RobotSchema, bool)> for Response {
    fn from((schema, verbose): (RobotSchema, bool)) -> Self {
        let created = if verbose { Some(schema.created_at().to_rfc3339()) } else { None };
        let updated = if verbose { Some(schema.updated_at().to_rfc3339()) } else { None };
        let description = if verbose { schema.description() } else { None };
        let ttl = if verbose { schema.ttl() } else { None };
        let expires_at = if verbose {
            schema.expires_at().map(|dt| dt.to_rfc3339())
        } else {
            None
        };

        Self {
            uuid: schema.uuid(),
            username: schema.id(),
            description,
            ttl,
            expires_at,
            created_at: created,
            updated_at: updated,
        }
    }
}
