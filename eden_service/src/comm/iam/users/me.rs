use crate::EdenDb;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use database::methods::update::{UpdateActor, UpdateMethod};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::UserCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, OrganizationUuid, UserId, UserUuid};
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

type UserCacheMgr = EdenDb;

fn user_not_found_error() -> EpError {
    EpError::rbac("User has been deleted or has no access in this organization")
}

/// Get the authenticated user's profile
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/humans/me",
    operation_id = "get_current_human",
    responses((status = OK, body = GetResponse))
)]
pub async fn get_me(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let user_cache = UserCacheUuid::new(Some(org_key.clone()), auth.user_uuid().clone());

    // Check if the user still has RBAC access in the organization
    let entries = database
        .control_plane_list_by_subject(org_key.uuid(), IdKind::User, user_cache.uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    if !entries.iter().any(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_key.uuid()) {
        return Err(error_handling(user_not_found_error(), &mut span));
    }

    let user_cache_object = CacheObjectType::new(Some(user_cache.clone()), None);
    let user_schema = <UserCacheMgr as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        &database,
        &user_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|_| user_not_found_error())
    .map_err(|e| error_handling(e, &mut span))?;

    let verbose = EdenSettings::from(req.headers()).verbose();
    let response = GetResponse::from((user_schema, verbose));

    EdenResponse::response(response).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct GetResponse {
    pub uuid: UserUuid,
    pub username: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl From<(UserSchema, bool)> for GetResponse {
    fn from((schema, verbose): (UserSchema, bool)) -> Self {
        let created = if verbose { Some(schema.created_at().to_rfc3339()) } else { None };
        let updated = if verbose { Some(schema.updated_at().to_rfc3339()) } else { None };
        let description = if verbose { schema.description() } else { None };

        Self {
            uuid: schema.uuid(),
            username: schema.id(),
            email: schema.email(),
            display_name: schema.display_name(),
            description,
            bio: schema.bio(),
            created_at: created,
            updated_at: updated,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct MeUpdateInput {
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub bio: Option<String>,
}

/// Update the authenticated user's profile
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["IAM"],
    path="/iam/humans/me",
    operation_id = "update_current_human",
    request_body = MeUpdateInput,
    responses((status = OK, body = PatchResponse))
)]
pub async fn patch_me(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    db_manager: web::Data<EdenDb>,
    database: web::Data<EdenDb>,
    input: web::Json<MeUpdateInput>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());
    let user_cache = UserCacheUuid::new(Some(org_cache.clone()), auth.user_uuid().clone());

    // Verify user has RBAC access in the organization
    let user_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), IdKind::User, user_cache.uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let user_org_entry = user_entries
        .iter()
        .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid());

    if user_org_entry.is_none() {
        return Err(error_handling(user_not_found_error(), &mut span));
    }

    // Resolve cache object for updates using the user's UUID
    let user_cache_object = CacheObjectType::new(Some(user_cache.clone()), None);

    // Get current username for the CacheObjectType needed by update methods
    let user_schema = <UserCacheMgr as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        &database,
        &user_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|_| user_not_found_error())
    .map_err(|e| error_handling(e, &mut span))?;

    let current_user = user_schema.id().to_string();

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

    // Get the updated user data
    let updated_user: UserSchema =
        db_manager.select_user_uuid(&UserUuid::from(user_cache.uuid()), telemetry_wrapper).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            actix_web::error::ErrorInternalServerError(e.to_string())
        })?;

    let perms = user_org_entry.map(|entry| entry.perms).unwrap_or(ControlPerms::empty());

    let verbose = EdenSettings::from(req.headers()).verbose();
    let mut response = PatchResponse::from((updated_user, verbose));
    response.org_uuid = auth.org_uuid().clone();
    response.perms = perms;

    EdenResponse::response(response).into()
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
pub struct PatchResponse {
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

impl From<(UserSchema, bool)> for PatchResponse {
    fn from((schema, verbose): (UserSchema, bool)) -> Self {
        let updated_at = if verbose { Some(schema.updated_at().to_rfc3339()) } else { None };
        let description = if verbose { schema.description() } else { None };

        Self {
            id: schema.id(),
            uuid: schema.uuid(),
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
