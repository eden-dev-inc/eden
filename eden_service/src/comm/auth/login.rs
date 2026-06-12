use crate::EdenDb;
use crate::comm::auth::JwtResponse;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid, UserCacheUuid};
use eden_core::format::{EdenUuid, IdKind, RobotUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::{FastSpan, FastSpanStatus, MetricEvent, TelemetryWrapper};
use log;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

// New struct to represent user data from the database
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
struct UserData {
    user_id: String,
    user_uuid: String,
    org_id: String,
    org_uuid: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct Organization {
    id: String,
}

/// Check if user still has RBAC access in the organization
/// Returns an error if the user has been deleted or has no access
pub async fn check_user_rbac_access(
    database: &web::Data<EdenDb>,
    user_cache_uuid: &UserCacheUuid,
    org_cache_uuid: &OrganizationCacheUuid,
    _telemetry_wrapper: &mut TelemetryWrapper,
    span: &mut FastSpan,
) -> Result<(), actix_web::error::Error> {
    let org_uuid = org_cache_uuid.uuid();
    let subject_uuid = user_cache_uuid.uuid();

    let has_org_access = database.control_plane_has_org_access_cached(org_uuid, IdKind::User, subject_uuid).await.map_err(|e| {
        log::error!("Failed to verify cached user org access: {}", e);
        error_handling(e, span)
    })?;

    if !has_org_access {
        span.set_status(FastSpanStatus::Error {
            message: Cow::Owned("User has been deleted or has no access in this organization".to_owned()),
        });
        return Err(actix_web::error::ErrorForbidden("User has been deleted or has no access in this organization"));
    }

    Ok(())
}

/// Check if a robot still has RBAC access in the organization.
/// Returns an error if the robot has been deleted or has no access.
pub async fn check_robot_rbac_access(
    database: &web::Data<EdenDb>,
    robot_cache_uuid: &RobotCacheUuid,
    org_cache_uuid: &OrganizationCacheUuid,
    _telemetry_wrapper: &mut TelemetryWrapper,
    span: &mut FastSpan,
) -> Result<(), actix_web::error::Error> {
    let org_uuid = org_cache_uuid.uuid();
    let subject_uuid = robot_cache_uuid.uuid();

    let has_org_access = database.control_plane_has_org_access_cached(org_uuid, IdKind::Robot, subject_uuid).await.map_err(|e| {
        log::error!("Failed to verify cached robot org access: {}", e);
        error_handling(e, span)
    })?;

    if !has_org_access {
        span.set_status(FastSpanStatus::Error {
            message: Cow::Owned("Robot has been deleted or has no access in this organization".to_owned()),
        });
        return Err(actix_web::error::ErrorForbidden("Robot has been deleted or has no access in this organization"));
    }

    Ok(())
}

/// Revalidate the current JWT subject against live organization membership.
///
/// JWT signature validation proves the token was issued by Eden, but this
/// additional check ensures the subject still has an active RBAC row in the
/// organization after account removal, permission revocation, or org changes.
pub async fn check_subject_rbac_access(
    database: &web::Data<EdenDb>,
    auth: &ParsedJwt,
    telemetry_wrapper: &mut TelemetryWrapper,
    span: &mut FastSpan,
) -> Result<(), actix_web::error::Error> {
    let org_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    if auth.is_robot() {
        let robot_uuid = auth.robot_uuid().ok_or_else(|| actix_web::error::ErrorUnauthorized("robot token is missing robot identity"))?;
        let robot_cache_uuid = RobotCacheUuid::new(Some(org_cache_uuid.clone()), RobotUuid::from(robot_uuid.uuid()));
        check_robot_rbac_access(database, &robot_cache_uuid, &org_cache_uuid, telemetry_wrapper, span).await
    } else {
        let user_cache_uuid = UserCacheUuid::new(Some(org_cache_uuid.clone()), auth.user_uuid().clone());
        check_user_rbac_access(database, &user_cache_uuid, &org_cache_uuid, telemetry_wrapper, span).await
    }
}

/// Return JWT Token from Login
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Authorization"],
    path="/auth/login",
    operation_id = "auth_login",
    security(("basicAuth" = [])),
    params(
        ("X-Org-Id" = String, Header, description = "Organization ID", example = "TestOrg"),
    ),
    responses((status = OK, body = JwtResponse), (status = UNAUTHORIZED))
)]
pub async fn login(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    // The basic auth middleware has already verified the credentials and RBAC access,
    // and it provided the authenticated subject in `auth`.
    let (token, jti) = database
        .create_token_with_jti(auth.user_id(), auth.user_uuid(), auth.org_id(), auth.org_uuid())
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid_str = auth.org_uuid().to_string();
    telemetry_wrapper.record_event(MetricEvent::LoginWith {
        user_id: auth.user_id().as_str(),
        org_id: auth.org_id().as_str(),
        org_uuid: &org_uuid_str,
    });

    // Record session on successful login with jti for revocation tracking
    let client_ip = req.connection_info().realip_remote_addr().unwrap_or("unknown").to_string();
    let user_agent = req.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    crate::user_sessions::SESSION_STORE.record_session_with_jti(
        &org_uuid_str,
        &auth.user_uuid().to_string(),
        auth.user_id().as_str(),
        &client_ip,
        &user_agent,
        analytics_schema::events::AuthMethod::Basic,
        Some(&jti),
    );

    span.add_simple_event("Generated JWT");

    Ok::<HttpResponse, actix_web::error::Error>(HttpResponse::Ok().json(JwtResponse { token: token.to_string() }))
}

/// Refresh JWT Token for Login
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Authorization"],
    path="/auth/refresh",
    operation_id = "auth_refresh",
    responses((status = OK, body = EdenResponse<JwtResponse>))
)]
pub async fn refresh(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().clone());
    let user_cache_uuid = UserCacheUuid::new(Some(org_cache_uuid.clone()), auth.user_uuid().clone());
    check_user_rbac_access(&database, &user_cache_uuid, &org_cache_uuid, telemetry_wrapper, &mut span).await?;

    let (token, jti) = database
        .create_token_with_jti(auth.user_id(), auth.user_uuid(), auth.org_id(), auth.org_uuid())
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid_str = auth.org_uuid().to_string();
    telemetry_wrapper.record_event(MetricEvent::LoginWith {
        user_id: auth.user_id().as_str(),
        org_id: auth.org_id().as_str(),
        org_uuid: &org_uuid_str,
    });

    // Record session on refresh with jti for revocation tracking
    let client_ip = req.connection_info().realip_remote_addr().unwrap_or("unknown").to_string();
    let user_agent = req.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    crate::user_sessions::SESSION_STORE.record_session_with_jti(
        &org_uuid_str,
        &auth.user_uuid().to_string(),
        auth.user_id().as_str(),
        &client_ip,
        &user_agent,
        analytics_schema::events::AuthMethod::Bearer,
        Some(&jti),
    );

    span.add_simple_event("Refreshed JWT");

    EdenResponse::response(JwtResponse { token: token.to_string() }).into()
}
