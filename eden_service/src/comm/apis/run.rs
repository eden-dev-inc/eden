use crate::EdenDb;
pub mod cache;

use crate::comm::rbac::verify_control_perms;
use crate::comm::templates::get_template_schema;
use crate::comm::templates::run::process_template_output;
use crate::error_handling;
use actix_http::header;
use actix_web::{HttpRequest, Responder, web};
use database::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::ApiCacheId;
use eden_core::format::cache_uuid::{ApiCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{ApiId, ApiUuid, CacheObjectType, CacheUuid, OrganizationCacheUuid, OrganizationUuid, TemplateUuid};
use eden_core::request::ServerData;
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_trace};
use endpoint_core::ep_core::database::api::bindings::Binding;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::api::ApiSchema;
use endpoint_core::ep_core::database::template::ApiFields;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use endpoint_core::ep_core::settings::EdenSettings;
use ep_runtime::comp::MyEngineService;
use function_name::named;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use tokio::task::JoinSet;
use utoipa::ToSchema;

/// Run an API with simple sequential execution and enhanced nested field support
/// **Permissions**: `ControlPerms::CONFIGURE` on the Api or Organization
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Apis"],
    path="/apis/{api}",
    request_body = Value,
    operation_id = "run_api",
        responses((status = OK, body = String))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
#[named]
pub async fn run(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    api: web::Path<String>,
    templates: web::Data<TemplateRegistry>,
    database: web::Data<EdenDb>,
    server_data: web::Data<ServerData>,
    engine_service: web::Data<MyEngineService>,
    input: web::Json<Value>,
) -> Result<impl Responder, actix_web::Error> {
    let settings = EdenSettings::from(req.headers());

    let auth_inner = auth.into_inner();
    let org_uuid = auth_inner.org_uuid().clone();
    let organization_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());
    let api_cache_object = CacheObjectType::from((Some(organization_cache_uuid.clone()), api.into_inner()));

    let api_schema = <EdenDb as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::get_from_cache(
        &database,
        &api_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth_inner, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let _ctx = ctx_with_trace!()
        .with_feature("apis")
        .with_organization_uuid(org_uuid.to_string())
        .with_additional("api_id", api_schema.id().to_string())
        .with_additional("api_uuid", api_schema.uuid().to_string());

    log_debug!(_ctx.clone(), "Running API", audience = LogAudience::Internal);

    let database = database.into_inner();
    let api_fields = ApiFields::try_from(input.into_inner())?;
    let bindings = api_schema.bindings().to_vec();

    let auth_header = req.headers().get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()).map(|value| value.to_string());

    {
        run_api_templates(
            bindings,
            templates.into_inner(),
            database.clone(),
            engine_service.into_inner(),
            &api_fields,
            auth_inner,
            &organization_cache_uuid,
            &org_uuid,
            settings,
            server_data.clone(),
            auth_header,
            telemetry_wrapper,
        )
        .await?
        .into()
    }
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response(HashMap<TemplateUuid, Value>);

impl Response {
    fn new(map: HashMap<TemplateUuid, Value>) -> Self {
        Self(map)
    }
}

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
#[named]
pub(crate) async fn run_api_templates(
    bindings: Vec<Binding>,
    templates: Arc<TemplateRegistry>,
    database: Arc<EdenDb>,
    engine_service: Arc<MyEngineService>,
    api_fields: &ApiFields,
    auth_inner: ParsedJwt,
    organization_cache_uuid: &OrganizationCacheUuid,
    organization_uuid: &OrganizationUuid,
    settings: EdenSettings,
    server_data: web::Data<ServerData>,
    auth_header: Option<String>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<EdenResponse<Response>> {
    let mut outputs = HashMap::new();
    let mut join_set = JoinSet::new();

    for binding in bindings {
        let template_uuid = binding.template().to_owned();

        let _ctx = ctx_with_trace!()
            .with_feature("apis")
            .with_organization_uuid(organization_uuid.to_string())
            .with_additional("template_uuid", template_uuid.to_string());

        log_trace!(_ctx.clone(), "Processing template binding", audience = LogAudience::Internal);

        // Clone necessary data for the spawned task
        let templates_clone = templates.clone();
        let template_fields_clone = binding.map_template_fields(api_fields.clone())?;
        let database_clone = database.clone();
        let engine_service_clone = engine_service.clone();
        let auth_inner_clone = auth_inner.clone();
        let organization_cache_uuid_clone = organization_cache_uuid.clone();
        let org_uuid_clone = organization_uuid.clone();
        let settings_clone = settings;
        let mut telemetry_wrapper_clone: TelemetryWrapper = telemetry_wrapper.clone();
        let server_data_clone = server_data.clone();
        let auth_header_clone = auth_header.clone();

        join_set.spawn(async move {
            match get_template_schema(
                &database_clone,
                &CacheObjectType::new(Some(TemplateCacheUuid::new(Some(organization_cache_uuid_clone), template_uuid.clone())), None),
                &mut telemetry_wrapper_clone,
            )
            .await
            {
                Ok(template_schema) => {
                    let result = process_template_output(
                        templates_clone,
                        &template_uuid.clone(),
                        &template_fields_clone,
                        database_clone,
                        engine_service_clone,
                        &auth_inner_clone,
                        &org_uuid_clone,
                        settings_clone,
                        template_schema.template().cache(),
                        server_data_clone,
                        auth_header_clone,
                        &mut telemetry_wrapper_clone,
                    )
                    .await;

                    log::trace!("process_template_output: {} {:?}", template_uuid, result);
                    log_trace!(
                        _ctx.clone(),
                        format!("process_template_output: {} {:?}", template_uuid, result),
                        audience = LogAudience::Internal
                    );
                    (template_uuid, result)
                }
                Err(e) => {
                    let ctx = ctx_with_trace!()
                        .with_feature("apis")
                        .with_additional("template_uuid", template_uuid.to_string())
                        .with_error_category("Template");

                    log_error!(ctx, "Error getting template schema", audience = LogAudience::Internal);
                    (template_uuid, Err(e))
                }
            }
        });
    }

    // Wait for all tasks to complete
    while let Some(join_result) = join_set.join_next().await {
        let (template_uuid, result) = join_result.map_err(EpError::api)?;
        outputs.insert(template_uuid, result.map_err(EpError::api)?);
    }

    Ok(EdenResponse::response(Response::new(outputs)))
}
