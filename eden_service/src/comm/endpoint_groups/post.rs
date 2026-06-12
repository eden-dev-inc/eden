use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use database::db::methods::insert::InsertMethod;
use database::db::methods::insert::endpoint_group::InsertEndpointGroup;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::EndpointGroupCacheId;
use eden_core::format::cache_uuid::EndpointGroupCacheUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{EdenId, EndpointGroupId, EndpointUuid};
use endpoint_core::ep_core::database::schema::endpoint_group::{EndpointGroupBuilder, EndpointGroupSchema};
use telemetry_extensions_macro::with_telemetry;

/// Create an Endpoint Group
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoint Groups"],
    path="/endpoint-groups",
    operation_id = "create_endpoint_group",
    request_body = EndpointGroupBuilder,
    responses((status = CREATED, body = EndpointGroupSchema))
)]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<EndpointGroupBuilder>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let user_uuid = auth.user_uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let builder = input.into_inner();

    // Resolve member endpoint UUIDs from string identifiers
    let members: Vec<EndpointUuid> = builder
        .members
        .iter()
        .map(|m| uuid::Uuid::parse_str(m).map(EndpointUuid::from).map_err(|e| EpError::parse(e.to_string())))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| error_handling(e, &mut span))?;

    let default_endpoint = builder
        .default_endpoint
        .as_ref()
        .map(|d| uuid::Uuid::parse_str(d).map(EndpointUuid::from).map_err(|e| EpError::parse(e.to_string())))
        .transpose()
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_group_id = EndpointGroupId::new(builder.id);
    let schema = EndpointGroupSchema::new(
        endpoint_group_id,
        builder.description,
        builder.ep_kind,
        default_endpoint,
        members,
        user_uuid.clone(),
    );

    let insert = InsertEndpointGroup::new(org_uuid.clone(), schema.clone());
    <EdenDb as InsertMethod<EndpointGroupSchema, EndpointGroupCacheUuid, EndpointGroupCacheId, InsertEndpointGroup>>::insert(
        &database,
        insert,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    Ok(HttpResponse::Created().json(schema))
}
