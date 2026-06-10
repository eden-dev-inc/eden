use crate::EdenDb;
use crate::comm::endpoints::runtime_cleanup::evict_endpoint_runtime_resources;
use crate::comm::interlays::shard::ShardRouter;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::methods::update::UpdateActor;
use database::stc::deserialize_endpoint_config_for_kind;
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenUuid, EndpointId, EndpointUuid, OrganizationUuid, UserUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
#[cfg(feature = "llm")]
use endpoint_core::llm_core::tools::clear_tool_discovery_cache;
use endpoint_schema::endpoint::{EndpointSchema, UpdateEndpointSchema};
use ep_runtime::comp::MyEngineService;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Update an Endpoint
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}",
    operation_id = "update_endpoint",
    request_body = UpdateEndpointSchema,
    responses((status = OK, body = String))
)]
pub async fn patch(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
    engine_service: web::Data<MyEngineService>,
    shard_router: web::Data<ShardRouter>,
    input: web::Json<UpdateEndpointSchema>,
) -> impl Responder {
    // Record endpoint request start time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_request(chrono::Utc::now()));

    let org_uuid = auth.org_uuid();

    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_uuid = EndpointUuid::new(
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .uuid(),
    );

    // telemetry_wrapper
    //     .mut_labels(|labels| {
    //         labels.set_endpoint_uuid(endpoint_uuid.clone());
    //     })
    //     .await;

    verify_control_perms(&database, &auth, Some(endpoint_uuid.clone()), ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .inspect(|_| span.add_event("Verified RBAC", vec![]))?;

    let update_request = input.into_inner();
    let mut updated_fields: Vec<&str> = Vec::new();
    if update_request.id().is_some() {
        updated_fields.push("id");
    }
    if update_request.description().is_some() {
        updated_fields.push("description");
    }
    if update_request.config().is_some() {
        updated_fields.push("config");
    }

    log::info!(
        "Endpoint update requested: endpoint_uuid={} org_uuid={} user_uuid={} fields=[{}]",
        endpoint_uuid,
        org_uuid,
        auth.user_uuid(),
        updated_fields.join(",")
    );

    let endpoint_cache_object = CacheObjectType::new(Some(EndpointCacheUuid::new(Some(org_key.clone()), endpoint_uuid.clone())), None);

    let Some(config_value) = update_request.config().cloned() else {
        update_endpoint(&database, &endpoint_cache_object, auth.user_uuid(), telemetry_wrapper, update_request)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        log::info!(
            "Endpoint update succeeded: endpoint_uuid={} org_uuid={} user_uuid={}",
            endpoint_uuid,
            org_uuid,
            auth.user_uuid()
        );

        let response: Result<actix_web::HttpResponse, actix_web::error::Error> =
            EdenResponse::response(Response::new("updated".to_string())).into();

        // Record endpoint response end time for duration tracking
        telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

        return response;
    };

    let current_endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &endpoint_cache_object,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let parsed_config =
        deserialize_endpoint_config_for_kind(current_endpoint_schema.kind(), &config_value).map_err(|e| error_handling(e, &mut span))?;
    let mut candidate_endpoint_schema = current_endpoint_schema.clone();
    candidate_endpoint_schema.update_config(parsed_config);

    engine_service
        .validate_endpoint_runtime_config(&database, &candidate_endpoint_schema, org_uuid, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    update_endpoint(
        &database,
        &endpoint_cache_object,
        auth.user_uuid(),
        telemetry_wrapper,
        UpdateEndpointSchema::new(None, Some(config_value), None),
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    if let Err(reconnect_error) = engine_service.reconnect(&database, &candidate_endpoint_schema, org_uuid, telemetry_wrapper).await {
        log::error!(
            "Endpoint live reconnect failed after config update: endpoint_uuid={} org_uuid={} kind={} error={}",
            endpoint_uuid,
            org_uuid,
            candidate_endpoint_schema.kind(),
            reconnect_error
        );

        rollback_endpoint_config_update(
            &database,
            &engine_service,
            &endpoint_cache_object,
            &current_endpoint_schema,
            org_uuid,
            auth.user_uuid(),
            telemetry_wrapper,
            &reconnect_error,
        )
        .await;

        evict_endpoint_runtime_resources(
            &shard_router,
            org_uuid,
            &endpoint_uuid,
            "endpoint_config_reconnect_failed_rollback",
            telemetry_wrapper,
        )
        .await;

        return Err(error_handling(reconnect_error, &mut span));
    }

    if let Some(metadata_update) = endpoint_metadata_update_request(&update_request) {
        let metadata_update_result =
            update_endpoint(&database, &endpoint_cache_object, auth.user_uuid(), telemetry_wrapper, metadata_update).await;
        evict_endpoint_runtime_resources(&shard_router, org_uuid, &endpoint_uuid, "endpoint_config_updated", telemetry_wrapper).await;
        metadata_update_result.map_err(|e| error_handling(e, &mut span))?;
    } else {
        evict_endpoint_runtime_resources(&shard_router, org_uuid, &endpoint_uuid, "endpoint_config_updated", telemetry_wrapper).await;
    }

    log::info!(
        "Endpoint update succeeded: endpoint_uuid={} org_uuid={} user_uuid={}",
        endpoint_uuid,
        org_uuid,
        auth.user_uuid()
    );

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> =
        EdenResponse::response(Response::new("updated".to_string())).into();

    // Record endpoint response end time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

    response
}

async fn rollback_endpoint_config_update(
    db_manager: &EdenDb,
    engine_service: &MyEngineService,
    cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    current_endpoint_schema: &EndpointSchema,
    org_uuid: &OrganizationUuid,
    updated_by: &UserUuid,
    telemetry_wrapper: &mut TelemetryWrapper,
    reconnect_error: &EpError,
) {
    match current_endpoint_schema.config().serialize() {
        Ok(previous_config) => {
            if let Err(rollback_error) = update_endpoint(
                db_manager,
                cache_object,
                updated_by,
                telemetry_wrapper,
                UpdateEndpointSchema::new(None, Some(previous_config), None),
            )
            .await
            {
                log::error!(
                    "Endpoint config rollback persistence failed: endpoint_uuid={} org_uuid={} kind={} reconnect_error={} rollback_error={}",
                    current_endpoint_schema.endpoint_uuid(),
                    org_uuid,
                    current_endpoint_schema.kind(),
                    reconnect_error,
                    rollback_error
                );
            }
        }
        Err(rollback_error) => {
            log::error!(
                "Endpoint config rollback serialization failed: endpoint_uuid={} org_uuid={} kind={} reconnect_error={} rollback_error={}",
                current_endpoint_schema.endpoint_uuid(),
                org_uuid,
                current_endpoint_schema.kind(),
                reconnect_error,
                rollback_error
            );
        }
    }

    if let Err(rollback_error) = engine_service.reconnect(db_manager, current_endpoint_schema, org_uuid, telemetry_wrapper).await {
        log::error!(
            "Endpoint config rollback reconnect failed: endpoint_uuid={} org_uuid={} kind={} reconnect_error={} rollback_error={}",
            current_endpoint_schema.endpoint_uuid(),
            org_uuid,
            current_endpoint_schema.kind(),
            reconnect_error,
            rollback_error
        );
    }
}

fn endpoint_metadata_update_request(update_request: &UpdateEndpointSchema) -> Option<UpdateEndpointSchema> {
    if update_request.id().is_none() && update_request.description().is_none() {
        return None;
    }

    Some(UpdateEndpointSchema::new(update_request.id().cloned(), None, update_request.description().cloned()))
}

pub(crate) async fn update_endpoint(
    db_manager: &EdenDb,
    cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    updated_by: &UserUuid,
    telemetry_wrapper: &mut TelemetryWrapper,
    update_endpoint_schema: UpdateEndpointSchema,
) -> ResultEP<()> {
    db_manager
        .update_endpoint_schema(update_endpoint_schema, cache_object, UpdateActor::User(updated_by), telemetry_wrapper)
        .await?;
    #[cfg(feature = "llm")]
    {
        clear_tool_discovery_cache();
    }
    Ok(())
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    status: String,
}

impl Response {
    fn new(status: String) -> ResultEP<Self> {
        Ok(Self { status })
    }
}
