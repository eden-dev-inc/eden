use crate::EdenDb;
use crate::comm::interlays::get_interlay_schema;
use crate::comm::interlays::runtime_cleanup::retire_interlay_runtime_resources;
use crate::comm::interlays::shard::ShardRouter;
use crate::comm::interlays::shutdown_running_interlay;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use dashmap::DashMap;
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::methods::delete::UuidsToUpdate;
use database::methods::delete::interlay::DeleteInterlay;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::InterlayCacheId;
use eden_core::format::cache_uuid::{CacheUuid, InterlayCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, InterlayId, InterlayUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::interlay::{InterlaySchema, InterlayState};
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use tokio::sync::Mutex;

/// Delete (disconnect) a Api
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Interlays"],
    path="/interlays/{interlay}",
    operation_id = "delete_interlay",
        responses((status = NO_CONTENT, description = "Interlay deleted successfully"))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    interlay: web::Path<String>,
    database: web::Data<EdenDb>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    interlay_locks: web::Data<DashMap<InterlayCacheUuid, Arc<Mutex<()>>>>,
    shard_router: web::Data<ShardRouter>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let interlay_object = CacheObjectType::from((Some(org_key.clone()), interlay.into_inner()));

    let interlay_uuid = get_interlay_schema(&database, &interlay_object, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .uuid();

    let interlay_cache_uuid = InterlayCacheUuid::new(Some(org_key.clone()), interlay_uuid);

    // Acquire per-interlay lock to serialize concurrent mutations.
    let lock = interlay_locks.entry(interlay_cache_uuid.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
    let _guard = lock.lock().await;

    if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone()) {
        // TODO: maybe add config with a force shutdown option.
        shutdown_running_interlay(&state).await;
    }

    retire_interlay_runtime_resources(&shard_router, &interlay_cache_uuid, "interlay_deleted", telemetry_wrapper).await;
    interlay_endpoints.remove(&interlay_cache_uuid);

    delete_interlay(&database, interlay_object, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    // Release the guard before removing the lock entry so any waiters see
    // the mutex unlock before the entry disappears.
    drop(_guard);
    interlay_locks.remove(&interlay_cache_uuid);

    Ok(HttpResponse::NoContent().finish())
}

pub(crate) async fn delete_interlay(
    db_manager: &EdenDb,
    cache_object: CacheObjectType<InterlayCacheUuid, InterlayCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<UuidsToUpdate> {
    let delete_interlay = <DeleteInterlay as DeleteMethod<
        InterlaySchema,
        InterlayCacheUuid,
        InterlayUuid,
        InterlayCacheId,
        InterlayId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(cache_object);

    <DeleteInterlay as DeleteMethod<
        InterlaySchema,
        InterlayCacheUuid,
        InterlayUuid,
        InterlayCacheId,
        InterlayId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete(&delete_interlay, db_manager, telemetry_wrapper)
    .await
}
