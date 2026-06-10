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
use endpoint_core::ep_core::database::schema::endpoint_group::{EndpointGroupSchema, UpdateEndpointGroupSchema};
use telemetry_extensions_macro::with_telemetry;

/// Update an Endpoint Group
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups/{group}",
    operation_id = "update_endpoint_group",
    request_body = UpdateEndpointGroupSchema,
    responses((status = OK, body = EndpointGroupSchema))
)]
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    auth: web::ReqData<ParsedJwt>,
    group: web::Path<String>,
    database: web::Data<EdenDb>,
    input: web::Json<UpdateEndpointGroupSchema>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());
    let user_uuid = auth.user_uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let mut schema: EndpointGroupSchema = <EdenDb as CacheFunctions<
        EndpointGroupSchema,
        EndpointGroupCacheUuid,
        EndpointGroupUuid,
        EndpointGroupCacheId,
        EndpointGroupId,
    >>::get_from_cache(
        &database,
        &CacheObjectType::from((Some(org_key.clone()), group.into_inner())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let update = input.into_inner();
    update.update(&mut schema);
    schema.set_updated_by(user_uuid.clone());

    // Persist to database
    database
        .update_endpoint_group(
            &schema.uuid(),
            update.id().map(|id| id.to_string()).as_deref(),
            update.description().map(|d| d.as_str()),
            update.default_endpoint(),
            &auth.user_uuid(),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Update cache
    <EdenDb as CacheFunctions<
        EndpointGroupSchema,
        EndpointGroupCacheUuid,
        EndpointGroupUuid,
        EndpointGroupCacheId,
        EndpointGroupId,
    >>::set_ex_cache(&database, Some(org_key), schema.clone(), telemetry_wrapper)
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(schema).into()
}
