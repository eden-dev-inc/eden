use crate::EdenDb;
use crate::comm::notifications::NotificationService;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use database::methods::insert::InsertMethod;
use database::methods::insert::user::InsertUser;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::UserCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, EdenUuid, IdKind, OrganizationUuid, UserId, UserUuid};
use eden_core::response::EdenResponse;
use email_address::EmailAddress;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::user::{UserInput, UserSchema};
use endpoint_core::ep_core::settings::EdenSettings;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

fn requires_superadmin(perms: ControlPerms) -> bool {
    perms.intersects(ControlPerms::CONFIGURE | ControlPerms::PROMOTE | ControlPerms::GRANT | ControlPerms::AUDIT | ControlPerms::DESTROY)
}

/// Create a New IAM User (user must not exist)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["IAM"],
    path="/iam/humans",
    operation_id = "create_human",
    request_body = UserInput,
    responses((status = CREATED, body = EdenResponse<Response>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    db_manager: web::Data<EdenDb>,
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<UserInput>, // relation_type, username_string
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let user_perms = input.perms().unwrap_or(ControlPerms::READ);

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | user_perms, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    if requires_superadmin(user_perms) {
        verify_control_perms(&database, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;
    }
    // validate email format if provided
    if let Some(ref email) = input.email()
        && !EmailAddress::is_valid(email)
    {
        return Err(actix_web::error::ErrorBadRequest("invalid email format"));
    }

    // validate display_name length if provided
    if let Some(ref display_name) = input.display_name()
        && display_name.len() > 255
    {
        return Err(actix_web::error::ErrorBadRequest("display_name must be 255 characters or fewer"));
    }

    // check if the username already exists
    match <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), input.username().to_owned())),
        telemetry_wrapper,
    )
    .await
    {
        Ok(_) => {
            return Err(actix_web::error::ErrorBadRequest(format!("user {} exists", input.username())));
        }
        Err(e) => match &e {
            EpError::Database(db_err) => {
                // If user is not found, we continue, but report any other database error
                if !matches!(db_err, eden_core::error::DatabaseError::UserNotFound) {
                    return Err(error_handling(e, &mut span));
                }
            }
            _ => return Err(error_handling(e, &mut span)),
        },
    }

    let insert_user = InsertUser::new(UserSchema::from((input.clone(), auth.org_uuid().clone(), auth.user_uuid().clone())));

    <EdenDb as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertUser>>::insert(
        &db_manager,
        insert_user.clone(),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // read UserCacheUuid from cache, "insert" above has created an entry in cache
    let user_cache = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), input.username().to_owned())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();
    let data = ControlPlaneRbacData {
        org_uuid: org_cache.uuid(),
        entity_kind: IdKind::Organization.as_str().to_owned(),
        entity_uuid: org_cache.uuid(),
        subject_kind: IdKind::User.as_str().to_owned(),
        subject_uuid: user_cache.uuid(),
        perms: user_perms,
    };
    database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;

    let user_cache_object = CacheObjectType::new(Some(user_cache.clone()), None);
    let created_user: UserSchema = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        &database,
        &user_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let verbose = EdenSettings::from(req.headers()).verbose();

    let mut response = Response::from((created_user, verbose));
    response.org_uuid = auth.org_uuid().clone();
    response.perms = user_perms;

    // Notify the new user that they've been added to the organization
    let _ = NotificationService::notify_system_update(
        &database,
        auth.org_uuid().uuid(),
        user_cache.uuid(),
        "Welcome to the organization",
        &format!("You have been added to the organization with {} access.", user_perms),
        None,
        None,
        telemetry_wrapper,
    )
    .await;

    Ok(HttpResponse::Created()
        .append_header(("Location", format!("/iam/humans/{}", response.uuid)))
        .json(EdenResponse::response(response)))
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
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<(UserSchema, bool)> for Response {
    fn from((schema, verbose): (UserSchema, bool)) -> Self {
        let created_at = if verbose { Some(schema.created_at().to_rfc3339()) } else { None };

        let description = if verbose { schema.description() } else { None };

        Self {
            id: schema.id(),
            uuid: schema.uuid(),
            // placeholders: handler will overwrite these
            perms: ControlPerms::READ,
            org_uuid: OrganizationUuid::new_uuid(),
            email: schema.email(),
            display_name: schema.display_name(),
            created_at,
            description,
        }
    }
}
