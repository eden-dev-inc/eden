use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::db::methods::delete::endpoint_group::DeleteEndpointGroup;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointGroupCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointGroupCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointGroupId, EndpointGroupUuid};
use endpoint_core::ep_core::database::schema::endpoint_group::EndpointGroupSchema;
use telemetry_extensions_macro::with_telemetry;

/// Delete an Endpoint Group
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups/{group}",
    operation_id = "delete_endpoint_group",
    responses((status = NO_CONTENT, description = "Endpoint group deleted successfully"))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    auth: web::ReqData<ParsedJwt>,
    group: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let cache_object = CacheObjectType::from((Some(org_key), group.into_inner()));

    let delete_group = <DeleteEndpointGroup as DeleteMethod<
        EndpointGroupSchema,
        EndpointGroupCacheUuid,
        EndpointGroupUuid,
        EndpointGroupCacheId,
        EndpointGroupId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(cache_object);

    <DeleteEndpointGroup as DeleteMethod<
        EndpointGroupSchema,
        EndpointGroupCacheUuid,
        EndpointGroupUuid,
        EndpointGroupCacheId,
        EndpointGroupId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete(&delete_group, &database, telemetry_wrapper)
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    Ok(HttpResponse::NoContent().finish())
}
