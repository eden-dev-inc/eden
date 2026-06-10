use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::cache::CacheFunctions;
use database::db::methods::insert::InsertMethod;
use database::methods::insert::api::InsertApi;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::{ApiCacheId, TemplateCacheId};
use eden_core::format::cache_uuid::{ApiCacheUuid, CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{ApiId, ApiUuid, CacheObjectType, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::api::{ApiSchema, Binding, UpdateApiSchema};
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use telemetry_extensions_macro::with_telemetry;

/// Partially update an API. Only the fields present in the body are changed; the
/// API's `uuid`, `created_by`, and `created_at` are preserved (the underlying
/// `INSERT … ON CONFLICT DO UPDATE` upsert never overwrites them). `bindings`
/// resolve template id → uuid exactly like create.
///
/// **Permissions**: `ControlPerms::CONFIGURE` on the API or Organization
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Apis"],
    path = "/apis/{api}",
    operation_id = "update_api",
    request_body = UpdateApiSchema,
    responses((status = OK, body = String))
)]
pub async fn patch(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    api: web::Path<String>,
    database: web::Data<EdenDb>,
    input: web::Json<UpdateApiSchema>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    // Load the current API so unspecified fields are preserved on the merge.
    let mut api_schema = <EdenDb as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::get_from_cache(
        &database,
        &CacheObjectType::from((Some(org_key.clone()), api.into_inner())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let update = input.into_inner();

    if let Some(description) = update.description() {
        api_schema.set_description(Some(description.to_owned()));
    }
    if let Some(fields) = update.fields() {
        api_schema.set_fields(fields.to_owned());
    }
    if let Some(builders) = update.bindings() {
        let mut bindings = vec![];
        for binding in builders {
            let template_uuid =
                <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_uuid(
                    &database,
                    &CacheObjectType::from((Some(org_key.clone()), binding.template().to_owned())),
                    telemetry_wrapper,
                )
                .await
                .map_err(|e| error_handling(e, &mut span))?;
            bindings.push(Binding::new(template_uuid, binding.fields().to_owned()));
        }
        api_schema.set_bindings(bindings);
    }
    if let Some(response_logic) = update.response_logic() {
        api_schema.set_response_logic(Some(response_logic.to_owned()));
    }
    api_schema.set_updated_by(auth.user_uuid().clone());

    let insert_api = InsertApi::new(org_uuid.to_owned(), api_schema);
    <EdenDb as InsertMethod<ApiSchema, ApiCacheUuid, ApiCacheId, InsertApi>>::insert(&database, insert_api, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}
