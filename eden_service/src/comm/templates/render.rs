use crate::EdenDb;
use crate::comm::rbac::verify_control_perms_for_entity;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::template::TemplateOutput;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenUuid, IdKind, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::FastSpanStatus;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use endpoint_core::ep_core::database::template::TemplateFields;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::borrow::Cow;
use telemetry_extensions_macro::with_telemetry;
use tokio::task::JoinSet;
use utoipa::ToSchema;

/// Run a Template
/// **Permissions**: `ControlPerms::CONFIGURE` on the Template or Organization
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Templates"],
    path="/templates/{template}/render",
    operation_id = "render_template",
    request_body = Value,
    responses((status = OK, body = TemplateOutput))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn render(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    template: web::Path<String>,
    templates: web::Data<TemplateRegistry>,
    database: web::Data<EdenDb>,
    _engine_service: web::Data<MyEngineService>,
    input: web::Json<Value>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let template_uuid = TemplateUuid::new(
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), template.to_string())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .uuid(),
    );

    verify_control_perms_for_entity(&database, &auth, IdKind::Template, template_uuid.uuid(), ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_simple_event("template uuid");

    let template = database
        .render_template(
            &templates,
            &template_uuid,
            org_uuid,
            &TemplateFields::try_from(input.into_inner())?,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_simple_event("template constructed from handlebars");

    EdenResponse::response(Response::new(template)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(TemplateOutput);

impl Response {
    fn new(template: TemplateOutput) -> Self {
        Self(template)
    }
}

/// Run a Template
/// **Permissions**: `ControlPerms::CONFIGURE` on the Template or Organization
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Templates"],
    path="/templates/{template}/render_many",
    request_body = Value,
    responses((status = OK, body = TemplateOutput))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn render_many(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    template: web::Path<String>,
    templates: web::Data<TemplateRegistry>,
    database: web::Data<EdenDb>,
    input: web::Json<Vec<Value>>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid().clone();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let template_uuid = TemplateUuid::new(
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), template.to_string())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .uuid(),
    );

    verify_control_perms_for_entity(&database, &auth, IdKind::Template, template_uuid.uuid(), ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_simple_event("template uuid");

    let mut outputs = Vec::new();
    let input = input.into_inner();
    let mut join_set = JoinSet::new();

    for value in input {
        let mut telemetry_wrapper_clone = telemetry_wrapper.clone();

        let database = database.clone();
        let template_uuid = template_uuid.clone();
        let templates = templates.clone();
        let org_uuid = org_uuid.clone();

        join_set.spawn(async move {
            database
                .render_template(
                    &templates,
                    &template_uuid,
                    &org_uuid,
                    &TemplateFields::try_from(value)?,
                    &mut telemetry_wrapper_clone,
                )
                .await
        });
    }

    // Wait for all tasks to complete
    while let Some(join_result) = join_set.join_next().await {
        let template = join_result
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                actix_web::error::ErrorBadRequest(e.to_string())
            })?
            .map_err(|e| error_handling(e, &mut span))?;
        outputs.push(template);
    }

    span.add_simple_event("template constructed from handlebars");

    EdenResponse::response(ResponseMany::new(outputs)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ResponseMany(Vec<TemplateOutput>);

impl ResponseMany {
    fn new(template: Vec<TemplateOutput>) -> Self {
        Self(template)
    }
}
