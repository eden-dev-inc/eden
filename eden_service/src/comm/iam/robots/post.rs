use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::rbac::ControlPlaneRbac;
use database::methods::insert::InsertMethod;
use database::methods::insert::robot::InsertRobot;
use eden_core::auth::{ApiKey, ParsedJwt};
use eden_core::error::{DatabaseError, EpError};
use eden_core::format::cache_id::{CacheId, RobotCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, IdKind, OrganizationUuid, RobotId, RobotUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::robot::{RobotInput, RobotSchema};
use endpoint_core::ep_core::settings::EdenSettings;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

pub(crate) const ALLOWED_AGENT_PERMS: ControlPerms = ControlPerms::READ.union(ControlPerms::CONFIGURE).union(ControlPerms::AUDIT);

/// Create a New Robot (Machine Account)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["IAM"],
    path="/iam/agents",
    operation_id = "create_agent",
    request_body = RobotInput,
    responses((status = CREATED, body = EdenResponse<Response>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    db_manager: web::Data<EdenDb>,
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<RobotInput>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let robot_perms = input.perms().unwrap_or(ControlPerms::READ);
    if !(robot_perms - ALLOWED_AGENT_PERMS).is_empty() {
        return Err(actix_web::error::ErrorBadRequest(
            "Agents can only hold READ, CONFIGURE, and AUDIT organization permissions",
        ));
    }

    // Only admins can create robots
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Check if the robot username already exists
    let existing_robot_cache = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), input.username().to_owned())),
        telemetry_wrapper,
    )
    .await;

    match existing_robot_cache {
        Ok(robot_cache) => {
            let robot_id = RobotId::from(input.username().to_owned());
            match database.select_robot_id::<RobotSchema>(&robot_id, auth.org_uuid(), telemetry_wrapper).await {
                Ok(_) => return Err(actix_web::error::ErrorBadRequest(format!("robot {} exists", input.username()))),
                Err(EpError::Database(DatabaseError::RobotNotFound)) => {
                    // Cache says the robot exists, but postgres does not. Remove stale cache entries and continue creation.
                    let stale_robot_cache_object =
                        CacheObjectType::new(Some(robot_cache), Some(RobotCacheId::new(Some(org_cache.clone()), robot_id)));
                    <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::invalidate(
                        &database,
                        &stale_robot_cache_object,
                        telemetry_wrapper,
                    )
                    .await
                    .map_err(|cache_err| error_handling(cache_err, &mut span))?;

                    log::warn!(
                        "Removed stale robot cache entry for username {} in org {} after postgres verification",
                        input.username(),
                        auth.org_uuid()
                    );
                }
                Err(e) => return Err(error_handling(e, &mut span)),
            }
        }
        Err(EpError::Database(DatabaseError::RobotNotFound)) => {}
        Err(e) => return Err(error_handling(e, &mut span)),
    }

    // Generate the API key (plaintext returned once, hashed stored)
    let (plaintext_key, hashed_key) = ApiKey::generate();

    let robot_schema = RobotSchema::from((input.into_inner(), auth.org_uuid().clone(), hashed_key, auth.user_uuid().clone()));

    let insert_robot = InsertRobot::new(robot_schema.clone());

    <EdenDb as InsertMethod<RobotSchema, RobotCacheUuid, RobotCacheId, InsertRobot>>::insert(&db_manager, insert_robot, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Read RobotCacheUuid from cache
    let robot_cache = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), robot_schema.id().to_string())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // Add RBAC rule for robot → organization
    let version_ms = chrono::Utc::now().timestamp_millis();
    let data = ControlPlaneRbacData {
        org_uuid: org_cache.uuid(),
        entity_kind: IdKind::Organization.as_str().to_owned(),
        entity_uuid: org_cache.uuid(),
        subject_kind: IdKind::Robot.as_str().to_owned(),
        subject_uuid: robot_cache.uuid(),
        perms: robot_perms,
    };
    database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;

    let robot_cache_object = CacheObjectType::new(Some(robot_cache.clone()), None);
    let created_robot: RobotSchema =
        <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_from_cache(
            &database,
            &robot_cache_object,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let verbose = EdenSettings::from(req.headers()).verbose();
    let response = Response::from_created(created_robot, verbose, auth.org_uuid().clone(), robot_perms, plaintext_key);

    Ok(HttpResponse::Created()
        .append_header(("Location", format!("/iam/agents/{}", response.uuid)))
        .json(EdenResponse::response(response)))
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
/// Robot create response payload.
/// `id`, `uuid`, `org_uuid`, and `perms` are always present.
/// `api_key` is returned only on create, and verbose fields are returned only when `verbose=true`.
pub struct Response {
    /// Robot username (always present).
    pub id: RobotId,
    /// Robot UUID (always present).
    pub uuid: RobotUuid,
    /// Exact organization control-plane permission bits (always present).
    pub perms: ControlPerms,
    /// Organization UUID from the request context (always present).
    pub org_uuid: OrganizationUuid,
    /// The plaintext API key — only returned on creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Creation timestamp, only included when `verbose=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// Description, only included when `verbose=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// TTL in seconds, only included when `verbose=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
}

impl Response {
    fn from_created(schema: RobotSchema, verbose: bool, org_uuid: OrganizationUuid, perms: ControlPerms, api_key: String) -> Self {
        let created_at = if verbose { Some(schema.created_at().to_rfc3339()) } else { None };
        let description = if verbose { schema.description() } else { None };
        let ttl = if verbose { schema.ttl() } else { None };

        Self {
            id: schema.id(),
            uuid: schema.uuid(),
            perms,
            org_uuid,
            api_key: Some(api_key),
            created_at,
            description,
            ttl,
        }
    }
}
