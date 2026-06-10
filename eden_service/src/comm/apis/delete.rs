use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::methods::delete::UuidsToUpdate;
use database::methods::delete::api::DeleteApi;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::ApiCacheId;
use eden_core::format::cache_uuid::{ApiCacheUuid, CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{ApiId, ApiUuid, CacheObjectType};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::api::ApiSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Delete (disconnect) a Api
/// **Permissions**: `ControlPerms::CONFIGURE` on the Api or Organization
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Apis"],
    path="/apis/{api}",
    operation_id = "delete_api",
        responses((status = OK, body = String))
)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    api: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let api_object = CacheObjectType::from((Some(org_key), api.into_inner()));

    let _api_uuid = <EdenDb as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::get_uuid(
        &database,
        &api_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    delete_api(&database, api_object, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Response {}

pub(crate) async fn delete_api(
    db_manager: &EdenDb,
    cache_object: CacheObjectType<ApiCacheUuid, ApiCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<UuidsToUpdate> {
    let delete_api =
        <DeleteApi as DeleteMethod<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId, RedisConn, PgConn, ClickhouseConn>>::new(
            cache_object,
        );

    <DeleteApi as DeleteMethod<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId, RedisConn, PgConn, ClickhouseConn>>::delete(
        &delete_api,
        db_manager,
        telemetry_wrapper,
    )
    .await
}
