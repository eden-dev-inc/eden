use crate::EdenDb;
use crate::comm::apis::get_api_schema;
use crate::comm::templates::get_template_schema;
use crate::comm::templates::run::process_template_output;
use actix_web::web;
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::ApiCacheId;
use eden_core::format::cache_uuid::{ApiCacheUuid, TemplateCacheUuid};
use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, OrganizationUuid};
use eden_core::request::ServerData;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_warn};
use endpoint_core::ep_core::database::template::ApiFields;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use endpoint_core::ep_core::settings::EdenSettings;
use ep_runtime::comp::MyEngineService;
use function_name::named;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde_json::Value;
use std::sync::Arc;

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
#[named]
pub(crate) async fn run_cache_api(
    database: Arc<EdenDb>,
    engine_service: Arc<MyEngineService>,
    auth: ParsedJwt,
    templates: Arc<TemplateRegistry>,
    cache_object_type: CacheObjectType<ApiCacheUuid, ApiCacheId>,
    org_uuid: &OrganizationUuid,
    organization_cache_uuid: &OrganizationCacheUuid,
    fields: &ApiFields,
    settings: EdenSettings,
    server_data: web::Data<ServerData>,
    auth_header: Option<String>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Value> {
    let _ctx = ctx_with_trace!().with_feature("api").with_organization_uuid(org_uuid.to_string());

    log_debug!(_ctx.clone(), "Run cache api", audience = LogAudience::Internal);
    let api_schema = get_api_schema(&database, &cache_object_type, telemetry_wrapper).await?;

    let bindings = api_schema.bindings().to_vec();

    let mut futures = FuturesUnordered::new();

    // Spawn all tasks
    for binding in bindings {
        let template_uuid = binding.template().to_owned();

        // Clone necessary data for the spawned task
        let templates_clone = templates.clone();
        let input_clone = fields.clone();
        let database_clone = database.clone();
        let engine_service_clone = engine_service.clone();
        let auth_clone = auth.clone();
        let organization_cache_uuid_clone = organization_cache_uuid.clone();
        let org_uuid_clone = org_uuid.clone();
        let settings_clone = settings;
        let mut telemetry_wrapper_clone: TelemetryWrapper = telemetry_wrapper.clone();
        let server_data_clone = server_data.clone();
        let auth_header_clone = auth_header.clone();

        let template_fields = binding.map_value(input_clone.into())?;

        log_debug!(
            _ctx.clone(),
            "Cache Api — Template Map",
            audience = LogAudience::Internal,
            template_fields = format!("{:?}", template_fields)
        );

        let ctx_for_task = _ctx.clone();
        futures.push(tokio::spawn(async move {
            match get_template_schema(
                &database_clone,
                &CacheObjectType::new(Some(TemplateCacheUuid::new(Some(organization_cache_uuid_clone), template_uuid.clone())), None),
                &mut telemetry_wrapper_clone,
            )
            .await
            {
                Ok(template_schema) => {
                    log_debug!(
                        ctx_for_task.clone(),
                        "Cache Api — Template schema",
                        audience = LogAudience::Internal,
                        template_schema = format!("{:?}", template_schema)
                    );
                    let result = process_template_output(
                        templates_clone,
                        &template_uuid,
                        &template_fields,
                        database_clone,
                        engine_service_clone,
                        &auth_clone,
                        &org_uuid_clone,
                        settings_clone,
                        template_schema.template().cache(),
                        server_data_clone,
                        auth_header_clone,
                        &mut telemetry_wrapper_clone,
                    )
                    .await;

                    log_debug!(
                        ctx_for_task.clone(),
                        "Cache Api — Result",
                        audience = LogAudience::Internal,
                        result = format!("{:?}", result)
                    );
                    (template_uuid, result)
                }
                Err(e) => {
                    log_warn!(ctx_for_task, "Cache Api — Error", audience = LogAudience::Internal, error = e.to_string());
                    (template_uuid, Err(e))
                }
            }
        }));
    }

    // Wait for first success, cancel remaining on success
    while let Some(task_result) = futures.next().await {
        match task_result {
            Ok((template_uuid, Ok(success))) => {
                log_debug!(
                    _ctx.clone(),
                    "Cache Api — Successful response",
                    audience = LogAudience::Internal,
                    template_uuid = template_uuid.to_string()
                );
                futures.clear();

                // * For Redis cache a 'Nil' or `null` value should be treated as an error
                if success.is_null() {
                    let e = "Cache responded with a `null` value";
                    log_warn!(
                        _ctx,
                        "Cache Api — Binding failed for template",
                        audience = LogAudience::Client,
                        template_uuid = template_uuid.to_string(),
                        error = e
                    );
                    return Err(EpError::cache(e));
                }

                // When we collect responses in a nested format:
                // {
                //     "kind": CacheEpKind,
                //     "data": {} <-- We want to return this data
                // }
                // The client expects the nested data, so we need to try and collect it
                let _ctx_data = _ctx.clone();
                return match success.get("data") {
                    Some(data) => {
                        if let Some(s) = data.as_str() {
                            if let Ok(obj) = serde_json::from_str::<serde_json::Value>(s) {
                                log_debug!(
                                    _ctx_data,
                                    "Successfully collected data object from cache",
                                    audience = LogAudience::Internal,
                                    data = serde_json::to_string(&obj).unwrap_or_default()
                                );
                                return Ok(obj);
                            }
                            log_debug!(
                                _ctx_data,
                                "Successfully collected data string from cache",
                                audience = LogAudience::Internal,
                                data = s
                            );
                        } else {
                            log_debug!(
                                _ctx_data,
                                "Successfully collected data from cache",
                                audience = LogAudience::Internal,
                                data = format!("{:?}", data)
                            );
                        }
                        Ok(data.to_owned())
                    }
                    None => {
                        log_debug!(_ctx_data, "Failed to collect data cache", audience = LogAudience::Internal);
                        Err(EpError::cache("failed to collect data"))
                    }
                };
            }
            Ok((template_uuid, Err(e))) => {
                log_warn!(
                    _ctx.clone(),
                    "Cache Api — Binding failed for template",
                    audience = LogAudience::Client,
                    template_uuid = template_uuid.to_string(),
                    error = e.to_string()
                );
            }
            Err(join_error) => {
                log_warn!(
                    _ctx.clone(),
                    "Cache Api — Task panicked",
                    audience = LogAudience::Internal,
                    error = join_error.to_string()
                );
            }
        }
    }

    // If we reach here, all bindings failed
    Err(EpError::cache("All cache endpoints failed"))
}
