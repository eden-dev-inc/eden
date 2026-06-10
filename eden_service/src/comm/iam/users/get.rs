use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use database::db::cache::CacheFunctions;
use database::db::methods::select::user::UsersPaginatedQuery;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::UserCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenUuid, IdKind, UserId, UserUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::user::UserSchema;
use endpoint_core::ep_core::settings::EdenSettings;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use telemetry_extensions_macro::with_telemetry;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

type UserCacheMgr = EdenDb;

fn user_not_found_error() -> EpError {
    EpError::rbac("User has been deleted or has no access in this organization")
}

/// Get an IAM User
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/humans/{human}",
    operation_id = "get_human",
    responses((status = OK, body = Response))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    user: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let user = user.into_inner();

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // if Admin is making a request, get user_uuid of the requested user
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let user_cache = <UserCacheMgr as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_key.clone()), user.clone())),
        telemetry_wrapper,
    )
    .await
    .map_err(|_| user_not_found_error())
    .map_err(|e| error_handling(e, &mut span))?;

    // Check if the target user still has RBAC access in the organization
    // If they were deleted, they will have no RBAC entry for this organization
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
    let response = Response::from((user_schema, verbose));

    EdenResponse::response(response).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response {
    // required
    pub uuid: UserUuid,
    pub username: UserId,
    // optional
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

impl From<(UserSchema, bool)> for Response {
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

const MAX_LIMIT: usize = 1000;

const fn default_limit() -> usize {
    100
}

#[derive(Debug, PartialEq, Eq, Deserialize, ToSchema)]
pub enum UserStatus {
    Active,
    Deleted,
}

impl Display for UserStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Active => "active",
                Self::Deleted => "deleted",
            }
        )
    }
}

/// Query parameters for listing IAM users
#[derive(Debug, Deserialize, IntoParams)]
pub struct GetAllUsersQuery {
    /// Filter by status (`Active` or `Deleted`)
    #[param(example = "Active")]
    pub status: Option<UserStatus>,
    /// Filter by exact organization control-plane permission bits (for example `RG` or `RCPA`)
    #[param(example = "RG")]
    pub perms: Option<ControlPerms>,
    /// Number of users to return (default: 100, max: 1000)
    #[param(example = 100)]
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor (`{nanos}_{uuid}`)
    #[param(example = "1735689600000000000_550e8400-e29b-41d4-a716-446655440000")]
    pub cursor: Option<String>,
}

impl GetAllUsersQuery {
    fn validate(&self) -> Result<(), actix_web::Error> {
        // Deleted users have no active org perms, so perms filter doesn't apply
        if self.status.eq(&Some(UserStatus::Deleted)) && self.perms.is_some() {
            return Err(actix_web::error::ErrorBadRequest("Cannot filter by `perms` when `status=Deleted`"));
        }

        if self.limit == 0 || self.limit > MAX_LIMIT {
            return Err(actix_web::error::ErrorBadRequest("`limit` must be between 1 and 1000"));
        }

        Ok(())
    }
}

/// Cursor containing timestamp (nanos) and UUID for pagination.
/// Cursor encodes only position, not query shape; clients should keep
/// `status`, `perms`, and `limit` unchanged when reusing a cursor
struct Cursor {
    created_at: DateTime<Utc>,
    uuid: UserUuid,
}

impl Cursor {
    /// Parse cursor string in format: `{nanos}_{uuid}`
    fn parse(cursor: &str) -> Option<Self> {
        let (nanos_str, uuid_str) = cursor.rsplit_once('_')?;
        let nanos: i64 = nanos_str.parse().ok()?;
        let created_at = DateTime::from_timestamp_nanos(nanos);
        let uuid = UserUuid::from(Uuid::parse_str(uuid_str).ok()?);
        Some(Self { created_at, uuid })
    }

    /// Create cursor string from timestamp and UUID.
    fn encode(created_at: &DateTime<Utc>, uuid: &UserUuid) -> String {
        format!("{}_{}", created_at.timestamp_nanos_opt().unwrap_or_default(), uuid.uuid())
    }
}

/// Paginated users response.
#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct GetAllUsersResponse {
    pub humans: Vec<Response>,
    /// Next cursor for pagination (format: {nanos}_{uuid})
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Get all IAM users in the organization
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/humans",
    operation_id = "get_all_humans",
    params(GetAllUsersQuery),
    responses((status = OK, body = GetAllUsersResponse))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get_all(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    query: web::Query<GetAllUsersQuery>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    // Validate query prams
    query.validate()?;

    // Parse cursor - return 400 if invalid format, default to MAX for initial request
    let (cursor_created_at, cursor_uuid) = match &query.cursor {
        Some(cursor_str) => Cursor::parse(cursor_str)
            .map(|c| (c.created_at, c.uuid))
            .ok_or_else(|| actix_web::error::ErrorBadRequest("Invalid `cursor`"))?,
        None => (DateTime::<Utc>::MAX_UTC, UserUuid::from(Uuid::max())),
    };

    // Verify Admin access
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let paginated_query = UsersPaginatedQuery {
        cursor_created_at,
        cursor_uuid,
        status_filter: query.status.as_ref().map(ToString::to_string),
        perms_filter: query.perms.map(|perms| perms.to_perm_string()),
        // Fetch one extra row to detect whether there is another page
        limit: query.limit.saturating_add(1) as i64,
    };

    let users_from_db: Vec<UserSchema> = database
        .select_users_paginated_filtered(auth.org_uuid(), paginated_query)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Build response
    let next_cursor = if users_from_db.len() > query.limit {
        users_from_db.get(query.limit - 1).map(|user| Cursor::encode(&user.created_at(), &user.uuid()))
    } else {
        None
    };

    let verbose = EdenSettings::from(req.headers()).verbose();
    let users = users_from_db.into_iter().take(query.limit).map(|user| Response::from((user, verbose))).collect();

    EdenResponse::response(GetAllUsersResponse { humans: users, next_cursor }).into()
}
