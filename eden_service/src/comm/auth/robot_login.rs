use crate::EdenDb;
use crate::comm::auth::JwtResponse;
use crate::comm::auth::login::check_robot_rbac_access;
use crate::comm::lib::get_org_from_header;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::format::cache_id::{CacheId, OrganizationCacheId, RobotCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::{CacheObjectType, EdenId, OrganizationId, OrganizationUuid, RobotId, RobotUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
use endpoint_core::ep_core::database::schema::robot::RobotSchema;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct RobotLoginRequest {
    /// The robot's API key.
    pub api_key: String,
    /// The robot's username (required for lookup).
    pub username: String,
}

/// Exchange a robot API key for a JWT token.
///
/// Behavior:
/// - Requires `X-Org-Id` or `X-Org-Uuid` request header to scope authentication.
/// - Resolves the robot by `(username, organization)` and verifies the provided API key.
/// - Verifies the robot still has RBAC access in the target organization.
/// - Returns `401 Unauthorized` for invalid API keys or missing org access.
/// - Returns server/database errors for infrastructure failures.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Authorization"],
    path="/auth/robots/login",
    operation_id = "auth_robot_login",
    request_body = RobotLoginRequest,
    params(
        ("X-Org-Id" = String, Header, description = "Organization ID", example = "TestOrg"),
    ),
    responses((status = OK, body = JwtResponse), (status = UNAUTHORIZED))
)]
pub async fn robot_login(
    req: HttpRequest,
    body: web::Json<RobotLoginRequest>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    // Resolve the organization from the X-Org-Id / X-Org-Uuid header
    let org_cache_object = get_org_from_header(req.headers()).map_err(|e| {
        span.add_event("missing organization header", vec![]);
        error_handling(e, &mut span)
    })?;

    let org_uuid = <EdenDb as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_uuid(&database, &org_cache_object, telemetry_wrapper)
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let org_id = <EdenDb as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_id(&database, &org_cache_object, telemetry_wrapper)
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    // Look up the robot by username within the organization
    let robot_cache_object =
        CacheObjectType::new(None, Some(RobotCacheId::new(Some(org_key.clone()), RobotId::new(body.username.clone()))));

    // Verify the API key
    let is_valid = database
        .verify_robot_auth(&robot_cache_object, &body.api_key, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    if !is_valid {
        span.add_event("Robot authentication failed", vec![]);
        return Err(actix_web::error::ErrorUnauthorized("Invalid API key"));
    }

    // Get the robot's details for token generation
    let robot: RobotSchema = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_from_cache(
        &database,
        &robot_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let robot_cache_uuid = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        &database,
        &robot_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    check_robot_rbac_access(&database, &robot_cache_uuid, &org_key, telemetry_wrapper, &mut span).await?;

    // Create a robot JWT token
    let (token, jti) = database
        .create_robot_token_with_jti(&robot.id(), &robot.uuid(), &org_id, &org_uuid)
        .map_err(|e| error_handling(e, &mut span))?;

    // Record session with jti for revocation tracking
    let client_ip = req.connection_info().realip_remote_addr().unwrap_or("unknown").to_string();
    let user_agent = req.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    crate::user_sessions::SESSION_STORE.record_session_with_jti(
        &org_uuid.to_string(),
        &robot.uuid().to_string(),
        robot.id().as_str(),
        &client_ip,
        &user_agent,
        analytics_schema::events::AuthMethod::ApiKey,
        Some(&jti),
    );

    span.add_event("Generated robot JWT", vec![]);

    Ok::<HttpResponse, actix_web::error::Error>(HttpResponse::Ok().json(EdenResponse::response(JwtResponse { token: token.to_string() })))
}
