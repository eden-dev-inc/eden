use crate::EdenDb;
use crate::comm::apis::run::cache::run_cache_api;
use crate::comm::endpoints::hydrate_llm_endpoint_config;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_http::header;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::template::{EndpointRequestTemplate, TemplateOutput};
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::{EndpointCacheId, TemplateCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, OrganizationUuid, TemplateId, TemplateUuid};
use eden_core::request::ServerData;
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_trace, log_warn};
use endpoint_core::ep_core::database::cache::{CacheLogic, TemplateCache};
use endpoint_core::ep_core::database::schema::routing::{EndpointRouting, RoutingResolver};
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use endpoint_core::ep_core::database::template::TemplateFields;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use endpoint_core::ep_core::settings::EdenSettings;
use endpoint_schema::endpoint::EndpointSchema;
use endpoints::endpoint::{EpRequest, EpTransaction};
use ep_runtime::comp::MyEngineService;
use function_name::named;
use serde::Serialize;
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use tokio::{join, try_join};
use utoipa::ToSchema;

/// Run a Template
/// **Permissions**: `ControlPerms::READ` or `ControlPerms::CONFIGURE` on the Template or Organization
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Templates"],
    path="/templates/{template}",
    operation_id = "run_template",
    request_body = Value,
    responses((status = OK, body = String))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    template: web::Path<String>,
    templates: web::Data<TemplateRegistry>,
    database: web::Data<EdenDb>,
    server_data: web::Data<ServerData>,
    engine_service: web::Data<MyEngineService>,
    input: web::Json<Value>,
) -> Result<impl Responder, actix_web::Error> {
    let settings = EdenSettings::from(req.headers());

    let auth = auth.into_inner();
    let auth_clone = auth.clone();

    let org_uuid = auth.org_uuid();

    let organization_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let auth_header = req.headers().get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()).map(|value| value.to_string());

    let template_schema = get_template_schema(
        &database,
        &CacheObjectType::from((Some(organization_cache_uuid.clone()), template.to_string())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let value = process_template_output(
        templates.into_inner(),
        template_schema.template_uuid(),
        &TemplateFields::try_from(input.into_inner())?,
        database.into_inner(),
        engine_service.into_inner(),
        &auth_clone,
        org_uuid,
        settings,
        template_schema.template().cache(),
        server_data.clone(),
        auth_header,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(value)).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response(Value);

impl Response {
    fn new(value: Value) -> Self {
        Self(value)
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::manual_async_fn)]
// can just use "async" as it fails with non-Send types https://github.com/rust-lang/rust-clippy/issues/12664
#[named]
pub(crate) fn process_template_output<'a>(
    templates: Arc<TemplateRegistry>,
    template_uuid: &'a TemplateUuid,
    fields: &'a TemplateFields,
    database: Arc<EdenDb>,
    engine_service: Arc<MyEngineService>,
    auth: &'a ParsedJwt,
    organization_uuid: &'a OrganizationUuid,
    settings: EdenSettings,
    template_cache: &'a Option<TemplateCache>,
    server_data: web::Data<ServerData>,
    auth_header: Option<String>,
    telemetry_wrapper: &'a mut TelemetryWrapper,
) -> impl Future<Output = ResultEP<Value>> + Send + 'a {
    async move {
        let _ctx = ctx_with_trace!()
            .with_feature("template")
            .with_organization_uuid(organization_uuid.to_string())
            .with_additional("template_uuid", template_uuid.to_string());
        let auth_header = auth_header;
        let template = database.render_template(&templates, template_uuid, organization_uuid, fields, telemetry_wrapper).await?;

        log_debug!(
            _ctx.clone(),
            "Collected template",
            audience = LogAudience::Internal,
            template = format!("{:?}", template)
        );

        let organization_cache_uuid = OrganizationCacheUuid::new(None, organization_uuid.clone());

        match template {
            TemplateOutput::Read(inner) => {
                log_debug!(_ctx.clone(), "Template read", audience = LogAudience::Internal);

                verify_control_perms(&database, auth, None, ControlPerms::READ, telemetry_wrapper).await?;

                let endpoint_cache_object = CacheObjectType::new(
                    Some(EndpointCacheUuid::new(
                        Some(OrganizationCacheUuid::new(None, organization_uuid.clone())),
                        inner.get_endpoint_uuid().to_owned(),
                    )),
                    None,
                );

                if let Some(template_cache) = template_cache {
                    match template_cache.cache_logic() {
                        CacheLogic::CacheAside { read, write } => {
                            log_debug!(_ctx.clone(), "Template cache — Cache aside", audience = LogAudience::Internal);
                            if let Ok(value) = run_cache_api(
                                database.clone(),
                                engine_service.clone(),
                                auth.clone(),
                                templates.clone(),
                                CacheObjectType::from((Some(organization_cache_uuid.clone()), read.id().to_string())),
                                &organization_uuid.clone(),
                                &organization_cache_uuid.clone(),
                                &template_cache.read_object_map(fields),
                                settings,
                                server_data.clone(),
                                auth_header.clone(),
                                &mut telemetry_wrapper.clone(),
                            )
                            .await
                            {
                                log_debug!(_ctx.clone(), "Cache aside, HIT", audience = LogAudience::Internal, value = value.to_string());
                                Ok(value)
                            } else {
                                log_warn!(
                                    _ctx.clone(),
                                    "Cache aside, MISSED — read the data from the endpoint",
                                    audience = LogAudience::Internal
                                );
                                let response = read_endpoint(
                                    inner,
                                    organization_cache_uuid.clone(),
                                    &endpoint_cache_object,
                                    &database.clone(),
                                    &engine_service,
                                    settings,
                                    telemetry_wrapper,
                                    auth,
                                    server_data.as_ref(),
                                    auth_header.as_deref(),
                                )
                                .await?;
                                log_debug!(
                                    _ctx.clone(),
                                    "Cache aside — Backing store read response",
                                    audience = LogAudience::Internal,
                                    response = format!("{:?}", response)
                                );

                                log_debug!(_ctx.clone(), "Cache aside: update cache with output", audience = LogAudience::Internal);
                                let cache_aside_output = run_cache_api(
                                    database,
                                    engine_service,
                                    auth.clone(),
                                    templates,
                                    CacheObjectType::from((Some(organization_cache_uuid.clone()), write.id().to_string())),
                                    &organization_uuid.clone(),
                                    &organization_cache_uuid,
                                    &template_cache.write_object_map(&TemplateFields::try_from(response.clone())?),
                                    settings,
                                    server_data.clone(),
                                    auth_header.clone(),
                                    &mut telemetry_wrapper.clone(),
                                )
                                .await;

                                match cache_aside_output {
                                    Ok(_) => {
                                        log_debug!(_ctx.clone(), "Cache aside updated successfully", audience = LogAudience::Internal);
                                    }
                                    Err(error) => {
                                        log_warn!(
                                            _ctx.clone(),
                                            "Cache aside update failed",
                                            audience = LogAudience::Internal,
                                            error = error.to_string()
                                        );
                                    }
                                }

                                Ok(response)
                            }
                        }
                        CacheLogic::ReadAround(write_cache_api) => {
                            log_debug!(_ctx.clone(), "Template cache — Read around", audience = LogAudience::Internal);
                            let response = read_endpoint(
                                inner,
                                organization_cache_uuid.clone(),
                                &endpoint_cache_object,
                                &database,
                                &engine_service,
                                settings,
                                telemetry_wrapper,
                                auth,
                                server_data.as_ref(),
                                auth_header.as_deref(),
                            )
                            .await?;

                            log_debug!(_ctx.clone(), "Read around: update cache with template output", audience = LogAudience::Internal);
                            // TODO: Await the future or use tokio::spawn to avoid silently dropping it
                            #[allow(clippy::let_underscore_future)]
                            let _ = run_cache_api(
                                database,
                                engine_service,
                                auth.clone(),
                                templates,
                                CacheObjectType::from((Some(organization_cache_uuid.clone()), write_cache_api.id().to_string())),
                                &organization_uuid.clone(),
                                &organization_cache_uuid,
                                &template_cache.write_object_map(fields),
                                settings,
                                server_data.clone(),
                                auth_header.clone(),
                                &mut telemetry_wrapper.clone(),
                            );

                            log_debug!(_ctx, "Read around response", audience = LogAudience::Internal, response = response.to_string());

                            Ok(response)
                        }
                        _ => {
                            read_endpoint(
                                inner,
                                organization_cache_uuid,
                                &endpoint_cache_object,
                                &database,
                                &engine_service,
                                settings,
                                telemetry_wrapper,
                                auth,
                                server_data.as_ref(),
                                auth_header.as_deref(),
                            )
                            .await
                        }
                    }
                } else {
                    read_endpoint(
                        inner,
                        organization_cache_uuid,
                        &endpoint_cache_object,
                        &database,
                        &engine_service,
                        settings,
                        telemetry_wrapper,
                        auth,
                        server_data.as_ref(),
                        auth_header.as_deref(),
                    )
                    .await
                }
            }
            TemplateOutput::Write(inner) => {
                log_debug!(_ctx.clone(), "Template write", audience = LogAudience::Internal);

                let _mut_inner = inner.to_owned();

                verify_control_perms(&database, auth, None, ControlPerms::CONFIGURE, telemetry_wrapper).await?;

                let endpoint_cache_uuid =
                    EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), inner.get_endpoint_uuid().to_owned());

                let endpoint_cache_object = CacheObjectType::new(Some(endpoint_cache_uuid.clone()), None);

                if let Some(template_cache) = template_cache {
                    match template_cache.cache_logic() {
                        CacheLogic::WriteThrough(write_cache_api) => {
                            log_debug!(_ctx.clone(), "Template cache — Write through", audience = LogAudience::Internal);
                            let mut cloned_telemetry_wrapper = telemetry_wrapper.clone();
                            let database_cloned = database.clone();
                            let engine_service_cloned = engine_service.clone();
                            let cache_fields = template_cache.write_object_map(fields);

                            let (endpoint_result, cache_result) = join!(
                                write_endpoint(
                                    inner,
                                    organization_cache_uuid.clone(),
                                    &endpoint_cache_object,
                                    &database,
                                    &engine_service,
                                    settings,
                                    telemetry_wrapper,
                                    auth,
                                    server_data.as_ref(),
                                    auth_header.as_deref(),
                                ),
                                run_cache_api(
                                    database_cloned,
                                    engine_service_cloned,
                                    auth.clone(),
                                    templates,
                                    CacheObjectType::from((Some(organization_cache_uuid.clone()), write_cache_api.id().to_string())),
                                    organization_uuid,
                                    &organization_cache_uuid,
                                    &cache_fields,
                                    settings,
                                    server_data.clone(),
                                    auth_header.clone(),
                                    &mut cloned_telemetry_wrapper
                                )
                            );

                            if let Err(e) = cache_result {
                                log_warn!(
                                    _ctx.clone(),
                                    "Cache write failed (continuing)",
                                    audience = LogAudience::Internal,
                                    error = e.to_string()
                                );
                            }

                            endpoint_result
                        }
                        CacheLogic::WriteBehind(write_cache_api) => {
                            log_debug!(_ctx.clone(), "Template cache — Write behind", audience = LogAudience::Internal);

                            let mut cloned_telemetry_wrapper = telemetry_wrapper.clone();
                            let database_cloned = database.clone();
                            let engine_service_cloned = engine_service.clone();
                            let cache_fields = template_cache.write_object_map(fields);

                            let cache_result = run_cache_api(
                                database_cloned,
                                engine_service_cloned,
                                auth.clone(),
                                templates,
                                CacheObjectType::from((Some(organization_cache_uuid.clone()), write_cache_api.id().to_string())),
                                organization_uuid,
                                &organization_cache_uuid,
                                &cache_fields,
                                settings,
                                server_data.clone(),
                                auth_header.clone(),
                                &mut cloned_telemetry_wrapper,
                            )
                            .await;

                            let cloned_inner = inner.clone();
                            let organization_cache_uuid_cloned = organization_cache_uuid.clone();
                            let endpoint_cache_object_cloned = endpoint_cache_object.clone();
                            let database_cloned = database.clone();
                            let engine_service_cloned = engine_service.clone();
                            let mut telemetry_wrapper_cloned = telemetry_wrapper.clone();
                            let auth_cloned = auth.clone();
                            let server_data_cloned = server_data.clone();
                            let auth_header_cloned = auth_header.clone();
                            tokio::spawn(async move {
                                let auth_ref = auth_cloned;
                                let server_data_ref = server_data_cloned;
                                let auth_header_opt = auth_header_cloned.as_deref();
                                match write_endpoint(
                                    cloned_inner,
                                    organization_cache_uuid_cloned.clone(),
                                    &endpoint_cache_object_cloned,
                                    &database_cloned,
                                    &engine_service_cloned,
                                    settings,
                                    &mut telemetry_wrapper_cloned,
                                    &auth_ref,
                                    server_data_ref.as_ref(),
                                    auth_header_opt,
                                )
                                .await
                                {
                                    Ok(_) => {
                                        let _ctx = ctx_with_trace!().with_feature("template");
                                        log_debug!(
                                            _ctx,
                                            "Write behind — successfully wrote to backing store",
                                            audience = LogAudience::Internal
                                        );
                                    }
                                    Err(e) => {
                                        let ctx = ctx_with_trace!().with_feature("template");
                                        log_warn!(
                                            ctx,
                                            "Write behind — failed to write to backing store",
                                            audience = LogAudience::Internal,
                                            error = e.to_string()
                                        );
                                    }
                                }
                            });

                            if let Err(e) = &cache_result {
                                log_warn!(_ctx.clone(), "Cache write failed", audience = LogAudience::Internal, error = e.to_string());
                            }

                            cache_result
                        }
                        CacheLogic::Invalidate(write_cache_api) => {
                            log_debug!(_ctx, "Invalidate", audience = LogAudience::Internal);

                            let mut cloned_telemetry_wrapper = telemetry_wrapper.clone();
                            let database_cloned = database.clone();
                            let engine_service_cloned = engine_service.clone();
                            let cache_fields = template_cache.write_object_map(fields);

                            let (_, response) = try_join!(
                                write_endpoint(
                                    inner,
                                    organization_cache_uuid.clone(),
                                    &endpoint_cache_object,
                                    &database,
                                    &engine_service,
                                    settings,
                                    telemetry_wrapper,
                                    auth,
                                    server_data.as_ref(),
                                    auth_header.as_deref(),
                                ),
                                run_cache_api(
                                    database_cloned,
                                    engine_service_cloned,
                                    auth.clone(),
                                    templates,
                                    CacheObjectType::from((Some(organization_cache_uuid.clone()), write_cache_api.id().to_string())),
                                    organization_uuid,
                                    &organization_cache_uuid,
                                    &cache_fields,
                                    settings,
                                    server_data.clone(),
                                    auth_header.clone(),
                                    &mut cloned_telemetry_wrapper
                                )
                            )?;

                            Ok(response)
                        }
                        _ => {
                            write_endpoint(
                                inner,
                                organization_cache_uuid,
                                &endpoint_cache_object,
                                &database,
                                &engine_service,
                                settings,
                                telemetry_wrapper,
                                auth,
                                server_data.as_ref(),
                                auth_header.as_deref(),
                            )
                            .await
                        }
                    }
                } else {
                    write_endpoint(
                        inner,
                        organization_cache_uuid,
                        &endpoint_cache_object,
                        &database,
                        &engine_service,
                        settings,
                        telemetry_wrapper,
                        auth,
                        server_data.as_ref(),
                        auth_header.as_deref(),
                    )
                    .await
                }
            }
            TemplateOutput::Transaction(inner) => {
                verify_control_perms(&database, auth, None, ControlPerms::CONFIGURE, telemetry_wrapper).await?;

                engine_service
                    .transaction(
                        organization_cache_uuid.clone(),
                        &database,
                        EndpointCacheUuid::new(Some(organization_cache_uuid), inner.get_endpoint_uuid().clone()),
                        &mut *TryInto::<Box<dyn EpTransaction>>::try_into(inner.get_transaction())?,
                        settings,
                        telemetry_wrapper,
                    )
                    .await
            }
            TemplateOutput::TwoPhaseTransaction(inner) => {
                verify_control_perms(&database, auth, None, ControlPerms::CONFIGURE, telemetry_wrapper).await?;

                engine_service
                    .transaction(
                        organization_cache_uuid.clone(),
                        &database,
                        EndpointCacheUuid::new(Some(organization_cache_uuid), inner.get_endpoint_uuid().clone()),
                        &mut *TryInto::<Box<dyn EpTransaction>>::try_into(inner.get_transaction())?,
                        settings,
                        telemetry_wrapper,
                    )
                    .await
            }
        }
    }
}

async fn get_template_schema(
    database_manager: &EdenDb,
    template_cache_object: &CacheObjectType<TemplateCacheUuid, TemplateCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<TemplateSchema> {
    <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_from_cache(
        database_manager,
        template_cache_object,
        telemetry_wrapper,
    )
    .await
}

async fn get_endpoint_schema(
    database_manager: &EdenDb,
    endpoint_cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<EndpointSchema> {
    <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
        database_manager,
        endpoint_cache_object,
        telemetry_wrapper,
    )
    .await
}

/// Resolve routing for a template request. Returns the endpoint schema to
/// actually execute against, which may differ from the primary when routing
/// is ReadReplica (for reads) or Sharded.
///
/// For Direct routing (the common case), returns the primary schema with zero overhead.
async fn resolve_routing_endpoint(
    primary_schema: EndpointSchema,
    organization_cache_uuid: &OrganizationCacheUuid,
    is_read: bool,
    database: &EdenDb,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<EndpointSchema> {
    let routing = primary_schema.routing();

    // Fast path: Direct routing always targets the primary endpoint
    if matches!(routing, EndpointRouting::Direct { .. }) {
        return Ok(primary_schema);
    }

    let resolver = RoutingResolver::new(&routing, Some(organization_cache_uuid))?;
    let target = resolver.select_endpoint(None, is_read);

    // If the resolved target is the same as the primary, no extra fetch needed
    if target.eden_uuid::<EndpointUuid>() == primary_schema.endpoint_uuid() {
        return Ok(primary_schema);
    }

    // Fetch the resolved endpoint schema from cache
    let target_cache_object = CacheObjectType::new(Some(target.clone()), None);
    get_endpoint_schema(database, &target_cache_object, telemetry_wrapper).await
}

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
#[named]
async fn read_endpoint(
    request: EndpointRequestTemplate,
    organization_cache_uuid: OrganizationCacheUuid,
    endpoint_cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    database: &EdenDb,
    engine_service: &MyEngineService,
    settings: EdenSettings,
    telemetry_wrapper: &mut TelemetryWrapper,
    auth: &ParsedJwt,
    server_data: &ServerData,
    auth_header: Option<&str>,
) -> ResultEP<Value> {
    let _ = (server_data, auth_header);

    let _ctx = ctx_with_trace!().with_feature("template").with_organization_uuid(organization_cache_uuid.uuid().to_string());

    let primary_schema: EndpointSchema = get_endpoint_schema(database, endpoint_cache_object, telemetry_wrapper).await?;

    // Resolve routing: for ReadReplica reads, this may redirect to a replica endpoint
    let mut endpoint_schema = resolve_routing_endpoint(primary_schema, &organization_cache_uuid, true, database, telemetry_wrapper).await?;

    hydrate_llm_endpoint_config(database, &mut endpoint_schema, auth.org_uuid(), telemetry_wrapper).await?;

    let effective_request = request.get_request().clone();

    log_debug!(
        _ctx.clone(),
        "Read endpoint",
        audience = LogAudience::Internal,
        request = format!("{:?}", effective_request.request()),
        kind = format!("{:?}", endpoint_schema.kind())
    );

    log_trace!(
        _ctx,
        "Read endpoint full details",
        audience = LogAudience::Internal,
        endpoint_schema = serde_json::to_string(&endpoint_schema).unwrap_or_default(),
        request = serde_json::to_string(effective_request.request()).unwrap_or_default(),
        organization_cache_uuid = organization_cache_uuid.to_string(),
        endpoint_cache_object = format!("{:?}", endpoint_cache_object),
        settings = serde_json::to_string(&settings).unwrap_or_default()
    );
    let response = engine_service
        .read(
            &mut *TryInto::<Box<dyn EpRequest>>::try_into((effective_request, endpoint_schema.kind()))?,
            &endpoint_schema,
            organization_cache_uuid,
            settings,
            telemetry_wrapper,
        )
        .await?;

    Ok(response)
}

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
#[named]
async fn write_endpoint(
    request: EndpointRequestTemplate,
    organization_cache_uuid: OrganizationCacheUuid,
    endpoint_cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    database: &EdenDb,
    engine_service: &MyEngineService,
    settings: EdenSettings,
    telemetry_wrapper: &mut TelemetryWrapper,
    auth: &ParsedJwt,
    server_data: &ServerData,
    auth_header: Option<&str>,
) -> ResultEP<Value> {
    let _ = (server_data, auth_header);

    let _ctx = ctx_with_trace!().with_feature("template").with_organization_uuid(organization_cache_uuid.uuid().to_string());

    let primary_schema: EndpointSchema = get_endpoint_schema(database, endpoint_cache_object, telemetry_wrapper).await?;

    // Resolve routing: writes always go to the primary endpoint
    let mut endpoint_schema =
        resolve_routing_endpoint(primary_schema, &organization_cache_uuid, false, database, telemetry_wrapper).await?;

    hydrate_llm_endpoint_config(database, &mut endpoint_schema, auth.org_uuid(), telemetry_wrapper).await?;

    let request_input = request.get_request().clone();

    log_debug!(
        _ctx,
        "Write endpoint",
        audience = LogAudience::Internal,
        request = format!("{:?}", request_input.request()),
        kind = format!("{:?}", endpoint_schema.kind())
    );

    let response = engine_service
        .write(
            &mut *TryInto::<Box<dyn EpRequest>>::try_into((request_input, endpoint_schema.kind()))?,
            &endpoint_schema,
            organization_cache_uuid.clone(),
            settings,
            telemetry_wrapper,
        )
        .await?;

    Ok(response)
}
