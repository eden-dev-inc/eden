use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::comm::templates::recommendation::generate_llm_recommendation;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenId, TemplateId, TemplateUuid};
use eden_core::request::ServerData;
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use endpoint_core::ep_core::database::template::UpdateTemplateSchema;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use log::warn;
use serde::Serialize;
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Update a Template
/// **Permissions**: `ControlPerms::CONFIGURE` on the Template or Organization
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Templates"],
    path="/templates/{template}",
    operation_id = "update_template",
    request_body = UpdateTemplateSchema,
    responses((status = OK, body = String))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    template: web::Path<String>,
    database: web::Data<EdenDb>,
    server_data: web::Data<ServerData>,
    input: web::Json<UpdateTemplateSchema>,
    template_registry: web::Data<TemplateRegistry>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let template_schema =
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_key), template.to_string())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let template_object = &CacheObjectType::new(
        Some(TemplateCacheUuid::new(
            Some(OrganizationCacheUuid::new(None, org_uuid.to_owned())),
            template_schema.uuid(),
        )),
        None,
    );

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let mut template_input = input.into_inner();
    let mut preview_schema = template_schema.clone();
    template_input.update(&mut preview_schema);

    let should_refresh_recommendation = template_input.template().is_some() || template_input.description().is_some();

    if should_refresh_recommendation
        && let Err(err) = async {
            let generated = generate_llm_recommendation(server_data.get_ref(), &preview_schema, telemetry_wrapper).await?;
            template_input.set_llm_recommendation(generated);
            Ok::<(), EpError>(())
        }
        .await
    {
        warn!("LLM recommendation regeneration failed for template {}: {}", template_schema.id(), err);
    }

    update_template(
        &database,
        &template_schema,
        template_object,
        UpdateActor::User(auth.user_uuid()),
        telemetry_wrapper,
        template_registry.into_inner(),
        template_input,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Response {}

pub(crate) async fn update_template(
    db_manager: &EdenDb,
    template_schema: &TemplateSchema,
    cache_object: &CacheObjectType<TemplateCacheUuid, TemplateCacheId>,
    updated_by: UpdateActor<'_>,
    telemetry_wrapper: &mut TelemetryWrapper,
    template_registry: Arc<TemplateRegistry>,
    update_template: UpdateTemplateSchema,
) -> ResultEP<()> {
    if let Some(id) = update_template.id() {
        <EdenDb as UpdateMethod<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::update_id(
            db_manager,
            cache_object,
            SqlQueries::UpdateTemplateId,
            id.id().to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?;
    }
    if let Some(description) = update_template.description() {
        <EdenDb as UpdateMethod<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::update_description(
            db_manager,
            cache_object,
            SqlQueries::UpdateTemplateDescription,
            description.to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?;
    }
    if let Some(template) = update_template.template() {
        <EdenDb as UpdateMethod<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::update_template_template(
            db_manager,
            cache_object,
            template.to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?;

        if let Some(id) = update_template.id() {
            template_registry.update(id.to_owned(), template_schema.uuid(), template.to_owned(), telemetry_wrapper).await?;
        } else {
            template_registry.update(template_schema.id(), template_schema.uuid(), template.to_owned(), telemetry_wrapper).await?;
        }
    }

    if let Some(recommendation) = update_template.llm_recommendation() {
        <EdenDb as UpdateMethod<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
        >>::update_template_llm_recommendation(db_manager, cache_object, recommendation.clone(), updated_by, telemetry_wrapper)
        .await?;
    }

    Ok(())
}
