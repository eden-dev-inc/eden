use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointGroupCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointGroupCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointGroupId, EndpointGroupUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::endpoint_group::{EndpointGroupSchema, EndpointGroupSchemaIds};
use telemetry_extensions_macro::with_telemetry;

/// Get an Endpoint Group
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups/{group}",
    operation_id = "get_endpoint_group",
    responses((status = OK, body = EndpointGroupSchema))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    auth: web::ReqData<ParsedJwt>,
    group: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let schema: EndpointGroupSchema = <EdenDb as CacheFunctions<
        EndpointGroupSchema,
        EndpointGroupCacheUuid,
        EndpointGroupUuid,
        EndpointGroupCacheId,
        EndpointGroupId,
    >>::get_from_cache(
        &database, &CacheObjectType::from((Some(org_key), group.into_inner())), telemetry_wrapper
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // Load members from database
    let members = database
        .select_endpoint_group_members(&schema.uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let mut schema = schema;
    schema.set_members(members);

    EdenResponse::response(schema).into()
}

/// Get all Endpoint Groups for an organization
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups",
    operation_id = "get_all_endpoint_groups",
    responses((status = OK, body = Vec<EndpointGroupSchemaIds>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get_all(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let schemas = database.select_all_endpoint_groups_ids(&org_uuid, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(schemas).into()
}
