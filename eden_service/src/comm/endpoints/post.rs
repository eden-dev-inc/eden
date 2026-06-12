use crate::EdenDb;
use crate::comm::notifications::NotificationService;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::methods::insert::endpoint::InsertEndpoint;
use database::db::rbac::DataPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::comm::NodeData;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::rbac::{ControlPerms, DataPerms, DataPlaneRbacData};
use eden_core::format::{CacheObjectType, CacheUuid};
use eden_core::format::{EdenUuid, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_info};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::routing::{
    EndpointRouting, EndpointRoutingInput, ShardEndpoint, ShardEndpointInput, ShardGroup, ShardGroupInput,
};
#[cfg(feature = "llm")]
use endpoint_core::llm_core::tools::clear_tool_discovery_cache;
use endpoint_schema::EndpointSchemaInput;
use endpoint_schema::endpoint::EndpointSchema;
use ep_runtime::comp::MyEngineService;
use function_name::named;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path="/endpoints",
    operation_id = "create_endpoint",
    request_body = EndpointSchemaInput,
    responses((status = OK, body = EdenResponse<Response>)),
)]
#[named]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    node_data: web::Data<NodeData>,
    engine_service: web::Data<MyEngineService>,
    input: web::Json<EndpointSchemaInput>,
) -> Result<impl Responder, actix_web::Error> {
    // Record endpoint request start time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_request(chrono::Utc::now()));

    let input = input.into_inner();
    let parsed_jwt = auth.clone().into_inner();
    let node_data = node_data.into_inner();

    let _ctx = ctx_with_trace!().with_feature("endpoints").with_organization_uuid(parsed_jwt.org_uuid().to_string());

    log_debug!(_ctx.clone(), "Attempting to add new endpoint", audience = LogAudience::Internal);

    //TODO RUN global consensus across all nodes to make sure they all add the right endpoint
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper).await?;

    //TODO pass eden_node uuid
    log::info!("POST /endpoints: resolved node_id={} node_uuid={}", node_data.id(), node_data.uuid());

    let org_key = OrganizationCacheUuid::new(None, parsed_jwt.org_uuid().clone());
    let routing_input = input.routing.clone();
    let mut endpoint_schema =
        EndpointSchema::try_from((input, auth.user_uuid().clone())).map_err(actix_web::error::ErrorInternalServerError)?;
    let resolved_routing = resolve_endpoint_routing(&database, &org_key, endpoint_schema.kind(), routing_input.as_ref(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    endpoint_schema.set_routing(resolved_routing);

    let insert_endpoint = InsertEndpoint::new(parsed_jwt.org_uuid().clone(), endpoint_schema, node_data.uuid().clone());

    // telemetry_wrapper
    //     .mut_labels(|labels| {
    //         labels.set_endpoint_uuid(insert_endpoint.get_endpoint_schema().uuid());
    //         labels.set_endpoint_id(insert_endpoint.get_endpoint_schema().id());
    //         labels.set_endpoint_kind(insert_endpoint.get_endpoint_schema().kind());
    //     })
    //     .await;

    span.add_event("built insert_endpoint", vec![]);

    engine_service.connect(&database, &insert_endpoint, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    // Grant the creator data-plane READ+WRITE access so the new endpoint is
    // immediately visible in listings that apply RBAC filtering (e.g. /llm/endpoints).
    database
        .data_plane_grant(
            &DataPlaneRbacData {
                org_uuid: parsed_jwt.org_uuid().uuid(),
                endpoint_uuid: insert_endpoint.get_endpoint_schema().uuid().uuid(),
                subject_kind: IdKind::User.as_str().to_string(),
                subject_uuid: parsed_jwt.user_uuid().uuid(),
                perms: DataPerms::READ | DataPerms::WRITE,
            },
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    #[cfg(feature = "llm")]
    {
        clear_tool_discovery_cache();
    }

    let endpoint_schema = insert_endpoint.get_endpoint_schema();
    let _ctx = _ctx
        .with_endpoint_uuid(endpoint_schema.uuid().to_string())
        .with_endpoint_id(endpoint_schema.id().to_string())
        .with_endpoint_kind(endpoint_schema.kind().to_string());

    let response = EdenResponse::response(Response { id: endpoint_schema.id(), uuid: endpoint_schema.uuid() });

    log_info!(_ctx, "Endpoint created successfully", audience = LogAudience::Client);

    // Notify user about the new endpoint
    let _ = NotificationService::notify_new_service(
        &database,
        parsed_jwt.org_uuid().uuid(),
        parsed_jwt.user_uuid().uuid(),
        &format!("Endpoint '{}' created", endpoint_schema.id()),
        &format!("Your {} endpoint is now connected and ready to use.", endpoint_schema.kind()),
        None,
        None,
        telemetry_wrapper,
    )
    .await;

    // Record endpoint response end time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

    response.into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    pub id: EndpointId,
    pub uuid: EndpointUuid,
}

async fn resolve_endpoint_routing(
    database: &EdenDb,
    org_key: &OrganizationCacheUuid,
    expected_kind: EpKind,
    routing: Option<&EndpointRoutingInput>,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> ResultEP<Option<EndpointRouting>> {
    let Some(routing) = routing else {
        return Ok(None);
    };

    let resolved = match routing {
        EndpointRoutingInput::Direct { endpoint } => EndpointRouting::Direct {
            endpoint: resolve_routing_endpoint(database, org_key, expected_kind, endpoint, telemetry_wrapper).await?,
        },
        EndpointRoutingInput::ReadReplica { primary, replicas, strategy } => EndpointRouting::ReadReplica {
            primary: resolve_routing_endpoint(database, org_key, expected_kind, primary, telemetry_wrapper).await?,
            replicas: resolve_routing_endpoint_list(database, org_key, expected_kind, replicas, telemetry_wrapper).await?,
            strategy: strategy.clone(),
        },
        EndpointRoutingInput::Sharded { shards, rule } => EndpointRouting::Sharded {
            shards: resolve_shard_endpoints(database, org_key, expected_kind, shards, telemetry_wrapper).await?,
            rule: rule.clone(),
        },
        EndpointRoutingInput::ShardedWithReplicas { shards, rule } => EndpointRouting::ShardedWithReplicas {
            shards: resolve_shard_groups(database, org_key, expected_kind, shards, telemetry_wrapper).await?,
            rule: rule.clone(),
        },
    };

    resolved.validate()?;
    Ok(Some(resolved))
}

async fn resolve_routing_endpoint_list(
    database: &EdenDb,
    org_key: &OrganizationCacheUuid,
    expected_kind: EpKind,
    endpoints: &[String],
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> ResultEP<Vec<EndpointUuid>> {
    let mut resolved = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        resolved.push(resolve_routing_endpoint(database, org_key, expected_kind, endpoint, telemetry_wrapper).await?);
    }
    Ok(resolved)
}

async fn resolve_shard_endpoints(
    database: &EdenDb,
    org_key: &OrganizationCacheUuid,
    expected_kind: EpKind,
    shards: &[ShardEndpointInput],
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> ResultEP<Vec<ShardEndpoint>> {
    let mut resolved = Vec::with_capacity(shards.len());
    for shard in shards {
        resolved.push(ShardEndpoint {
            endpoint: resolve_routing_endpoint(database, org_key, expected_kind, &shard.endpoint, telemetry_wrapper).await?,
            range: shard.range.clone(),
        });
    }
    Ok(resolved)
}

async fn resolve_shard_groups(
    database: &EdenDb,
    org_key: &OrganizationCacheUuid,
    expected_kind: EpKind,
    shards: &[ShardGroupInput],
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> ResultEP<Vec<ShardGroup>> {
    let mut resolved = Vec::with_capacity(shards.len());
    for shard in shards {
        resolved.push(ShardGroup {
            primary: resolve_routing_endpoint(database, org_key, expected_kind, &shard.primary, telemetry_wrapper).await?,
            replicas: resolve_routing_endpoint_list(database, org_key, expected_kind, &shard.replicas, telemetry_wrapper).await?,
            range: shard.range.clone(),
            replica_strategy: shard.replica_strategy.clone(),
        });
    }
    Ok(resolved)
}

async fn resolve_routing_endpoint(
    database: &EdenDb,
    org_key: &OrganizationCacheUuid,
    expected_kind: EpKind,
    endpoint: &str,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> ResultEP<EndpointUuid> {
    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            database,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(org_key.clone()), endpoint.to_owned())),
            telemetry_wrapper,
        )
        .await?;

    if endpoint_schema.kind() != expected_kind {
        return Err(EpError::request(format!(
            "routing endpoint '{}' has kind {}; expected {}",
            endpoint,
            endpoint_schema.kind(),
            expected_kind
        )));
    }

    Ok(endpoint_schema.uuid())
}
