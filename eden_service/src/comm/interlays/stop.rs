use crate::EdenDb;
use crate::comm::interlays::runtime_cleanup::clear_interlay_runtime_resources;
use crate::comm::interlays::shard::ShardRouter;
use crate::comm::interlays::shutdown_running_interlay;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use dashmap::DashMap;
use database::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::InterlayCacheId;
use eden_core::format::cache_uuid::InterlayCacheUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, CacheUuid, InterlayId, InterlayUuid, OrganizationCacheUuid};
use endpoint_core::ep_core::database::schema::interlay::{InterlaySchema, InterlayState};
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use tokio::sync::Mutex;

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Interlay"],
    path="/interlays/{interlay}/stop",
    responses((status = OK, description = "Interlay stopped successfully", body = ()))
)]
#[allow(clippy::too_many_arguments)]
pub async fn stop(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    interlay: web::Path<String>,
    database_manager: web::Data<EdenDb>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    interlay_locks: web::Data<DashMap<InterlayCacheUuid, Arc<Mutex<()>>>>,
    shard_router: web::Data<ShardRouter>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database_manager, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.into_inner().org_uuid().clone());

    let interlay_uuid = <EdenDb as CacheFunctions<InterlaySchema, InterlayCacheUuid, InterlayUuid, InterlayCacheId, InterlayId>>::get_uuid(
        &database_manager,
        &CacheObjectType::<InterlayCacheUuid, InterlayCacheId>::from((Some(organization_cache_uuid.clone()), interlay.into_inner())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let interlay_cache_uuid = InterlayCacheUuid::new(Some(organization_cache_uuid), interlay_uuid);

    // Acquire per-interlay lock to serialize concurrent mutations.
    let lock = interlay_locks.entry(interlay_cache_uuid.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
    let _guard = lock.lock().await;

    let was_running = if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone()) {
        shutdown_running_interlay(&state).await
    } else {
        false
    };

    clear_interlay_runtime_resources(&shard_router, &interlay_cache_uuid, "interlay_stopped", telemetry_wrapper).await;
    interlay_endpoints.remove(&interlay_cache_uuid);

    if !was_running {
        return Err(actix_web::error::ErrorNotFound("Interlay not running"));
    }

    Ok(HttpResponse::Ok().body("Interlay stopped successfully"))
}
