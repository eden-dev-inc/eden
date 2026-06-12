use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::cache::CacheFunctions;
use database::db::methods::insert::InsertMethod;
use database::methods::insert::api::InsertApi;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::{ApiCacheId, TemplateCacheId};
use eden_core::format::cache_uuid::{ApiCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::api::{ApiBuilder, ApiSchema, Binding};
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Create a Template
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Apis"],
    path="/apis",
    request_body = ApiBuilder,
    operation_id = "create_api",
        responses((status = OK, body = String))
)]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<ApiBuilder>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let constructor = input.into_inner();

    let mut bindings = vec![];
    for binding in constructor.bindings_ref() {
        bindings.push(Binding::new(
            <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_uuid(
                &database,
                &CacheObjectType::from((Some(org_key.clone()), binding.template().to_owned())),
                telemetry_wrapper,
            )
            .await
            .map_err(|e| error_handling(e, &mut span))?,
            binding.fields().to_owned(),
        ));
    }

    let api_schema = ApiSchema::new(
        constructor.id_ref().to_owned(),
        constructor.description_ref().cloned(),
        constructor.fields_ref().to_owned(),
        bindings,
        constructor.response_logic_ref().cloned(),
        auth.user_uuid().clone(),
    );

    let insert_api = InsertApi::new(org_uuid.to_owned(), api_schema.clone());

    <EdenDb as InsertMethod<ApiSchema, ApiCacheUuid, ApiCacheId, InsertApi>>::insert(&database, insert_api, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(api_schema)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    schema: ApiSchema,
}

impl Response {
    pub fn new(schema: ApiSchema) -> Self {
        Response { schema }
    }
}
