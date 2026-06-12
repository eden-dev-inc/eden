use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::comm::templates::recommendation::generate_llm_recommendation;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::methods::insert::InsertMethod;
use database::db::methods::insert::template::InsertTemplate;
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::TemplateCacheUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::request::ServerData;
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::template::{TemplateBuilder, TemplateSchema};
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use log::warn;
use serde::Serialize;
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Create a Template
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Templates"],
    path="/templates",
    operation_id = "create_template",
    request_body = TemplateBuilder,
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn post<'sized>(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    templates: web::Data<TemplateRegistry>,
    server_data: web::Data<ServerData>,
    input: web::Json<TemplateBuilder>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_event("parse template_schema from input", vec![]);
    let mut template_schema =
        TemplateSchema::try_from((input.into_inner(), auth.user_uuid().clone())).map_err(|e| error_handling(e, &mut span))?;

    if let Err(err) = async {
        let generated = generate_llm_recommendation(server_data.get_ref(), &template_schema, telemetry_wrapper).await?;
        if let Some(recommendation) = generated {
            template_schema.update_llm_recommendation(Some(recommendation));
        }
        Ok::<(), EpError>(())
    }
    .await
    {
        warn!("LLM recommendation generation failed for template {}: {}", template_schema.id(), err);
    }

    // INSERT TEMPLATE
    post_template(
        &database,
        InsertTemplate::new(auth.org_uuid().to_owned(), template_schema.clone()),
        telemetry_wrapper,
        template_schema.clone(),
        templates.into_inner(),
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(template_schema.clone())).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response {
    schema: TemplateSchema,
}

impl Response {
    pub fn new(schema: TemplateSchema) -> Self {
        Response { schema }
    }
}

pub(crate) async fn post_template(
    db_manager: &EdenDb,
    insert_template: InsertTemplate,
    telemetry_wrapper: &mut TelemetryWrapper,
    template_schema: TemplateSchema,
    template_registry: Arc<TemplateRegistry>,
) -> ResultEP<()> {
    // INSERT TEMPLATE
    <EdenDb as InsertMethod<TemplateSchema, TemplateCacheUuid, TemplateCacheId, InsertTemplate>>::insert(
        db_manager,
        insert_template,
        telemetry_wrapper,
    )
    .await?;

    // Insert template while maintaining the web::Data wrapper
    template_registry.insert(template_schema, telemetry_wrapper).await
}
