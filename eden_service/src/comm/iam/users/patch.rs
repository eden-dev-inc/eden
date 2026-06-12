use crate::EdenDb;
use crate::comm::auth::check_user_rbac_access;
use crate::comm::rbac::verify_control_perms;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use database::methods::update::{UpdateActor, UpdateMethod};
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::UserCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, EdenUuid, IdKind, OrganizationUuid, UserId, UserUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::FastSpanStatus;
use email_address::EmailAddress;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::user::UserSchema;
use endpoint_core::ep_core::settings::EdenSettings;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

fn requires_superadmin(perms: ControlPerms) -> bool {
    perms.intersects(ControlPerms::CONFIGURE | ControlPerms::PROMOTE | ControlPerms::GRANT | ControlPerms::AUDIT | ControlPerms::DESTROY)
}

// UserInput with all fields optional, so only the requested fields can change
#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct OptionalUserInput {
    pub username: Option<String>,
    pub password: Option<String>,
    pub description: Option<String>,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub perms: Option<ControlPerms>,
}

/// Update an existing IAM User
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["IAM"],
    path="/iam/humans/{human}",
    operation_id = "update_human",
    request_body = OptionalUserInput,
    responses((status = OK, body = EdenResponse<Response>))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    db_manager: web::Data<EdenDb>,
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    user: web::Path<String>,
    input: web::Json<OptionalUserInput>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let user_cache = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), user.clone())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| actix_web::error::ErrorBadRequest(format!("user {user} doesn't exist: {e}")))?;

    let existing_user: UserSchema =
        db_manager.select_user_uuid(&UserUuid::from(user_cache.uuid()), telemetry_wrapper).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;

    let admin_user_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), IdKind::User, auth.user_uuid().uuid())
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    // Check if the target user still has RBAC access in the organization
    check_user_rbac_access(&database, &user_cache, &org_cache, telemetry_wrapper, &mut span).await?;

    // Get the existing user's RBAC for authorization checks later
    let existing_user_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), IdKind::User, user_cache.uuid())
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    // - Anyone can change it's own data
    // - Only Admin and SuperAdmin can change other user's data, but only if they have lower access
    //   level, i.e. Admins can't change other Admins
    // - only SuperAdmins can change passwords
    // - SuperAdmins can change anything, even other SuperAdmins

    // Check if this is modifying another user (not self-update)
    // A self-update means the authenticated user is updating their own profile
    let is_self_update = existing_user.uuid() == *auth.user_uuid();

    if !is_self_update {
        // For updates to OTHER users, check authorization
        let admin_perms = admin_user_entries
            .iter()
            .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid())
            .map(|entry| entry.perms)
            .unwrap_or(ControlPerms::empty());
        let existing_user_perms = existing_user_entries
            .iter()
            .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid())
            .map(|entry| entry.perms)
            .unwrap_or(ControlPerms::empty());

        log::info!("PATCH: Admin perms: {:?}, trying to update user: {}", admin_perms, user);

        verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
            .await
            .map_err(actix_web::error::ErrorForbidden)?;

        if !existing_user_perms.is_empty() {
            verify_control_perms(&database, &auth, None, ControlPerms::GRANT | existing_user_perms, telemetry_wrapper)
                .await
                .map_err(actix_web::error::ErrorForbidden)?;
            if requires_superadmin(existing_user_perms) {
                verify_control_perms(&database, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
                    .await
                    .map_err(actix_web::error::ErrorForbidden)?;
            }
        }

        if input.password.is_some() {
            verify_control_perms(&database, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
                .await
                .map_err(actix_web::error::ErrorForbidden)?;
        }
    }

    let mut current_user = user.clone();
    if let Some(new_username) = &input.username {
        <EdenDb as UpdateMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::update_user_id(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), current_user.clone())),
            new_username.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
        // Update the current_user to the new username for subsequent operations
        current_user = new_username.clone();
    }

    if let Some(new_description) = &input.description {
        <EdenDb as UpdateMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::update_user_description(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), current_user.clone())),
            new_description.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
    }

    if let Some(new_email) = &input.email {
        if !EmailAddress::is_valid(new_email) {
            return Err(actix_web::error::ErrorBadRequest("invalid email format"));
        }
        <EdenDb as UpdateMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::update_user_email(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), current_user.clone())),
            new_email.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
    }

    if let Some(new_display_name) = &input.display_name {
        if new_display_name.len() > 255 {
            return Err(actix_web::error::ErrorBadRequest("display_name must be 255 characters or fewer"));
        }
        <EdenDb as UpdateMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::update_user_display_name(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), current_user.clone())),
            new_display_name.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
    }

    if let Some(new_bio) = &input.bio {
        <EdenDb as UpdateMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::update_user_bio(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), current_user.clone())),
            new_bio.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
    }

    if let Some(new_password) = &input.password {
        <EdenDb as UpdateMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::update_user_password(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), current_user.clone())),
            new_password.to_owned(),
            UpdateActor::User(auth.user_uuid()),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
    }

    let current_user_perms = existing_user_entries
        .iter()
        .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid())
        .map(|entry| entry.perms)
        .unwrap_or(ControlPerms::empty());

    if let Some(perms) = input.perms {
        verify_control_perms(&database, &auth, None, ControlPerms::GRANT | current_user_perms | perms, telemetry_wrapper)
            .await
            .map_err(actix_web::error::ErrorForbidden)?;
        if requires_superadmin(current_user_perms | perms) {
            verify_control_perms(&database, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
                .await
                .map_err(actix_web::error::ErrorForbidden)?;
        }

        let version_ms = chrono::Utc::now().timestamp_millis();
        let data = ControlPlaneRbacData {
            org_uuid: org_cache.uuid(),
            entity_kind: IdKind::Organization.as_str().to_owned(),
            entity_uuid: org_cache.uuid(),
            subject_kind: IdKind::User.as_str().to_owned(),
            subject_uuid: user_cache.uuid(),
            perms,
        };
        database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;
    }

    // Get the updated user data
    let updated_user: UserSchema =
        db_manager.select_user_uuid(&UserUuid::from(user_cache.uuid()), telemetry_wrapper).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;

    // Get the updated organization permission bits if they were changed.
    let updated_user_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), IdKind::User, user_cache.uuid())
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let perms = updated_user_entries
        .iter()
        .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid())
        .map(|entry| entry.perms)
        .unwrap_or(ControlPerms::empty());

    let verbose = EdenSettings::from(_req.headers()).verbose();

    let mut response = Response::from((updated_user, verbose));
    // attach org and access info determined by the handler
    response.org_uuid = auth.org_uuid().clone();
    response.perms = perms;

    EdenResponse::response(response).into()
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
pub struct Response {
    pub id: UserId,
    pub uuid: UserUuid,
    pub perms: ControlPerms,
    pub org_uuid: OrganizationUuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
}

impl From<(UserSchema, bool)> for Response {
    fn from((schema, verbose): (UserSchema, bool)) -> Self {
        let updated_at = if verbose { Some(schema.updated_at().to_rfc3339()) } else { None };

        let description = if verbose { schema.description() } else { None };

        Self {
            id: schema.id(),
            uuid: schema.uuid(),
            // placeholders: handler can set proper values after From
            perms: ControlPerms::empty(),
            org_uuid: OrganizationUuid::new_uuid(),
            email: schema.email(),
            display_name: schema.display_name(),
            updated_at,
            description,
            bio: schema.bio(),
        }
    }
}
