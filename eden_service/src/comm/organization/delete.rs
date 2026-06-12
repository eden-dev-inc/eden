use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, web};
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::db::methods::delete::organization::DeleteOrganization;
use database::methods::delete::UuidsToUpdate;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::OrganizationCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Delete an Organization
/// **Permissions**: `ControlPerms::DESTROY` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Organization"],
    path="/organizations",
    operation_id = "delete_organization",
    responses((status = OK, body = String))
)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let del_organization = <DeleteOrganization as DeleteMethod<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(CacheObjectType::new(Some(OrganizationCacheUuid::new(None, auth.org_uuid().to_owned())), None));

    let removed_uuids =
        delete_organization(&database, telemetry_wrapper, &del_organization).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(auth.org_id().clone(), auth.org_uuid().clone(), removed_uuids)).into()
}

pub(crate) async fn delete_organization(
    db_manager: &EdenDb,
    telemetry_wrapper: &mut TelemetryWrapper,
    delete_organization: &DeleteOrganization,
) -> ResultEP<UuidsToUpdate> {
    <DeleteOrganization as DeleteMethod<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete(delete_organization, db_manager, telemetry_wrapper)
    .await
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response {
    id: OrganizationId,
    uuid: OrganizationUuid,
    removed_objects: UuidsToUpdate,
}

impl Response {
    fn new(id: OrganizationId, uuid: OrganizationUuid, removed_objects: UuidsToUpdate) -> ResultEP<Self> {
        Ok(Self { id, uuid, removed_objects })
    }
}
