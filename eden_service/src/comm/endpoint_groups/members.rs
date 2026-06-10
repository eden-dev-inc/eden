use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::EndpointGroupCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointGroupCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointGroupId, EndpointGroupUuid, EndpointUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::endpoint_group::EndpointGroupSchema;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MemberInput {
    pub endpoint_uuid: EndpointUuid,
}

/// Add a member endpoint to an endpoint group
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups/{group}/members",
    operation_id = "add_endpoint_group_member",
    request_body = MemberInput,
    responses((status = OK, body = EndpointGroupSchema))
)]
#[allow(clippy::too_many_arguments)]
pub async fn add_member(
    auth: web::ReqData<ParsedJwt>,
    group: web::Path<String>,
    database: web::Data<EdenDb>,
    input: web::Json<MemberInput>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

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

    let member = input.into_inner();

    // Insert member in database
    database
        .insert_endpoint_group_member(&schema.uuid(), &member.endpoint_uuid, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Update in-memory schema
    schema.add_member(member.endpoint_uuid);

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

/// Remove a member endpoint from an endpoint group
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups/{group}/members/{endpoint}",
    operation_id = "remove_endpoint_group_member",
    responses((status = OK, body = EndpointGroupSchema))
)]
#[allow(clippy::too_many_arguments)]
pub async fn remove_member(
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());
    let (group_identifier, endpoint_identifier) = path.into_inner();
    let endpoint_uuid = EndpointUuid::from(
        uuid::Uuid::parse_str(&endpoint_identifier).map_err(|e| error_handling(EpError::parse(e.to_string()), &mut span))?,
    );

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
        &database, &CacheObjectType::from((Some(org_key.clone()), group_identifier)), telemetry_wrapper
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // Delete member from database
    database
        .delete_endpoint_group_member(&schema.uuid(), &endpoint_uuid, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Update in-memory schema
    schema.remove_member(&endpoint_uuid);

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
