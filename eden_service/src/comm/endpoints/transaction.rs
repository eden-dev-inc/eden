use crate::EdenDb;
use crate::comm::els::{apply_els_for_transaction, resolve_els_endpoint_switch_schema, resolve_els_required};
use crate::comm::rbac::{AuthMode, verify_endpoint_access};
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::DataPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::{FastSpan, TelemetryWrapper};
use endpoint_core::ep_core::ep::{ConnectionTier, EpConfig};
use endpoint_core::ep_core::settings::EdenSettings;
use endpoint_schema::endpoint::EndpointSchema;
use endpoints::endpoint::EpTransaction;
use endpoints::endpoint::transaction::EndpointTransactionInput;
use ep_runtime::comp::MyEngineService;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Transactional request from an Endpoint (requires write permissions)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/transaction",
    operation_id = "endpoint_transaction",
    request_body = EndpointTransactionInput,
    responses((status = OK, body = serde_json::Value))
)]
pub async fn transaction(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
    engine_service: web::Data<MyEngineService>,
    input: web::Json<EndpointTransactionInput>,
) -> impl Responder {
    let settings = EdenSettings::from(req.headers());

    let org_uuid = auth.org_uuid();

    let organization_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let cache_object = CacheObjectType::from((Some(organization_cache_uuid.clone()), endpoint.into_inner()));

    endpoint_transaction(
        &database,
        &engine_service,
        organization_cache_uuid,
        &cache_object,
        input.into_inner(),
        &auth.into_inner(),
        settings,
        &mut span,
        telemetry_wrapper,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn endpoint_transaction(
    db_manager: &EdenDb,
    engine_service: &web::Data<MyEngineService>,
    organization_cache_uuid: OrganizationCacheUuid,
    cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    input: EndpointTransactionInput,
    auth: &ParsedJwt,
    settings: EdenSettings,
    span: &mut FastSpan,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<actix_web::HttpResponse, actix_web::error::Error> {
    let mut endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            db_manager,
            cache_object,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, span))?;

    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), endpoint_schema.endpoint_uuid());
    let auth_mode = verify_endpoint_access(
        db_manager,
        auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::WRITE,
        telemetry_wrapper,
    )
    .await
    .inspect(|_| span.add_event("Verified RBAC", vec![]))?;

    let mut request_json = input.request().clone();
    let mut config_override: Option<Box<dyn EpConfig>> = None;
    if auth_mode == AuthMode::Els {
        let els_auth = resolve_els_required(db_manager, auth, &endpoint_cache_uuid).await.map_err(|e| error_handling(e, span))?;
        if let Some(switched_schema) = resolve_els_endpoint_switch_schema(
            db_manager,
            &organization_cache_uuid,
            endpoint_schema.kind(),
            els_auth.as_ref(),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, span))?
        {
            endpoint_schema = switched_schema;
        } else {
            let els_conn = apply_els_for_transaction(
                endpoint_schema.kind(),
                els_auth.as_ref(),
                endpoint_schema.config().as_ref(),
                ConnectionTier::Write,
                &mut request_json,
            )
            .map_err(|e| error_handling(e, span))?;
            if let Some(els_conn) = els_conn {
                let mut override_config = endpoint_schema.config();
                override_config.update_write_conn(els_conn).map_err(|e| error_handling(e, span))?;
                config_override = Some(override_config);
            }
        }
    }

    let mut transaction: Box<dyn EpTransaction> = serde_json::from_value(request_json).map_err(EpError::serde)?;
    let execution_cache_uuid = match config_override.as_ref() {
        Some(_) => EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), EndpointUuid::new_uuid()),
        None => EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), endpoint_schema.endpoint_uuid()),
    };

    let response = engine_service
        .transaction_els(db_manager, execution_cache_uuid, config_override, &mut *transaction, settings, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, span))?;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(Response::new(response)).into();

    response
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response(serde_json::Value);

impl Response {
    fn new(value: serde_json::Value) -> Self {
        Self(value)
    }
}
