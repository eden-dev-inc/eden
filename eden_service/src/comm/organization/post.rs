use crate::EdenDb;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::cache::CacheFunctions;
use database::db::methods::insert::InsertMethod;
use database::db::methods::insert::organization::InsertOrganization;
use database::db::rbac::ControlPlaneRbac;
use database::methods::insert::user::InsertUser;
use eden_core::auth::Password;
use eden_core::comm::NodeData;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::{OrganizationCacheId, UserCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, EdenId, IdKind, OrganizationId, OrganizationUuid, UserId, UserUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::{FastSpanAttribute, TelemetryWrapper};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_info};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::organization::{OrganizationInput, OrganizationSchema};
use endpoint_core::ep_core::database::schema::user::UserSchema;
use function_name::named;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Create a New Organization
///
/// An endpoint for creating a new organization.
/// Requires the EDEN_NEW_ORG_TOKEN as a Bearer token for authorization.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Organization"],
    path="/new",
    operation_id = "create_organization",
    request_body = OrganizationInput,
    security(("bearerToken" = [])),
    responses((status = OK, body = String))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    _req: HttpRequest,
    db_manager: web::Data<EdenDb>,
    input: web::Json<OrganizationInput>,
    node_data: web::Data<NodeData>,
) -> Result<impl Responder, actix_web::Error> {
    let input = input.into_inner();
    let super_admins = input.super_admins().to_vec();

    let mut insert_org = InsertOrganization::try_from(input).map_err(|e| error_handling(e, &mut span))?;

    let _ctx = ctx_with_trace!()
        .with_feature("organization")
        .with_organization_uuid(insert_org.organization_schema().uuid().to_string())
        .with_eden_node_uuid(node_data.uuid().to_string());
    log_info!(_ctx.clone(), "Creating new organization", audience = LogAudience::Internal);

    insert_org.add_eden_node(node_data.id().to_owned(), node_data.uuid()).map_err(|e| error_handling(e, &mut span))?;

    span.add_event(
        "inserting new org".to_string(),
        vec![FastSpanAttribute::new(
            "org_uuid",
            insert_org.organization_schema().uuid().to_string(),
        )],
    );

    let response = EdenResponse::response(Response::new(
        insert_org.organization_schema().id().clone(),
        insert_org.organization_schema().uuid().clone(),
    ))
    .into();

    let organization_uuid = insert_org.organization_schema().uuid().clone();
    let org_cache = OrganizationCacheUuid::new(None, organization_uuid.clone());

    insert_organization(&db_manager, insert_org, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    // now we have organization, create users
    for user in &super_admins {
        let user_schema = UserSchema::new(
            UserId::new(user.username().to_string()),
            Password::new(user.password().to_string()),
            organization_uuid.clone(),
            user.description(),
            user.email(),
            user.display_name(),
        );

        let insert_user = InsertUser::new(user_schema.clone());

        <EdenDb as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertUser>>::insert(&db_manager, insert_user, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        // read UserCacheUuid from cache, "insert" above has created an entry in cache
        let user_cache = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
            &db_manager,
            &CacheObjectType::from((Some(org_cache.clone()), user.username().to_owned())),
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
            perms: ControlPerms::all(),
        };
        db_manager.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;

        span.add_event(
            "inserted new super admin".to_string(),
            vec![FastSpanAttribute::new("super_admin", user.username().to_string())],
        );

        log_debug!(
            _ctx.clone(),
            "Super admin user created",
            audience = LogAudience::Internal,
            user_id = user.username()
        );
    }

    log_info!(
        _ctx,
        "Organization created successfully",
        audience = LogAudience::Client,
        super_admin_count = super_admins.len().to_string()
    );

    response
}

pub(crate) async fn insert_organization(
    db_manager: &EdenDb,
    insert_org: InsertOrganization,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    <EdenDb as InsertMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationCacheId, InsertOrganization>>::insert(
        db_manager,
        insert_org,
        telemetry_wrapper,
    )
    .await
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    id: OrganizationId,
    uuid: OrganizationUuid,
}

impl Response {
    fn new(id: OrganizationId, uuid: OrganizationUuid) -> Self {
        Self { id, uuid }
    }
}
