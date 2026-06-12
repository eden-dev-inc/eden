use crate::EdenDb;
use crate::comm::interlays::post::{CreateInterlayResponse, init_interlay};
use crate::comm::interlays::runtime_cleanup::clear_interlay_runtime_resources;
use crate::comm::interlays::{
    get_interlay_schema, interlay_conflicting_bind_port, normalize_interlay_listeners, shutdown_running_interlay,
    validate_interlay_mirror_settings, validate_multi_listener_interlay_shape, validate_port,
};
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use dashmap::DashMap;
use database::cache::CacheFunctions;
use database::methods::insert::InsertMethod;
use database::methods::insert::interlay::InsertInterlay;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::{EndpointCacheId, InterlayCacheId};
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, CacheUuid, EndpointId, EndpointUuid, InterlayId, InterlayUuid, OrganizationCacheUuid};
use eden_core::telemetry::{FastSpan, TelemetryWrapper};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::interlay::{
    InterlayListener, InterlaySchema, InterlaySettings, InterlaySignal, InterlayState,
};
use endpoint_core::ep_core::database::schema::interlay_tls::{PatchTls, deserialize_patch_tls};
use endpoint_schema::endpoint::EndpointSchema;
use ep_runtime::comp::MyEngineService;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use tokio::sync::{Mutex, Notify, broadcast};
use tokio::task::AbortHandle;
use utoipa::ToSchema;

type Db = EdenDb;

/// Build an [`InterlayState`] from init handles and insert it into the shared map.
fn register_interlay_state(
    interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
    cache_uuid: &InterlayCacheUuid,
    ep_schema: &EndpointSchema,
    interlay_schema: &InterlaySchema,
    org_key: OrganizationCacheUuid,
    signal_tx: broadcast::Sender<InterlaySignal>,
    abort_handles: Vec<AbortHandle>,
    shutdown_notify: Arc<Notify>,
) {
    let mut state = InterlayState::new(
        ep_schema.cache_key(org_key),
        ep_schema.kind(),
        ep_schema.routing(),
        interlay_schema.settings().command_policy_value().cloned(),
        interlay_schema.settings().audit_config_value().cloned(),
        interlay_schema.settings().mirror().clone(),
    );
    state.update_listener_config(interlay_schema.listeners().to_vec(), interlay_schema.advertise_host().cloned());
    state.set_signal_tx(signal_tx);
    state.set_abort_handles(abort_handles);
    state.set_shutdown_notify(shutdown_notify);
    interlay_endpoints.insert(cache_uuid.clone(), state);
}

/// Restart an interlay with the updated schema, rolling back on failure.
///
/// On success the new listener is registered in `interlay_endpoints`.
/// On failure the old schema is restored in the DB and a best-effort
/// attempt is made to re-start the previous configuration.
#[named]
#[allow(clippy::too_many_arguments)]
async fn restart_interlay_with_rollback(
    engine_service: web::Data<MyEngineService>,
    database_manager: &web::Data<Db>,
    org_key: &OrganizationCacheUuid,
    interlay_schema: &InterlaySchema,
    old_schema: &InterlaySchema,
    interlay_endpoints: &web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    interlay_cache_uuid: &InterlayCacheUuid,
    proxy_runtime: &tokio::runtime::Handle,
    shard_router: Option<Arc<crate::comm::interlays::shard::ShardRouter>>,
    telemetry_wrapper: &mut TelemetryWrapper,
    span: &mut FastSpan,
) -> Result<(), actix_web::Error> {
    let endpoint_schema =
        <Db as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            database_manager,
            &CacheObjectType::new(Some(EndpointCacheUuid::new(Some(org_key.clone()), interlay_schema.endpoint().clone())), None),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, span))?;

    match init_interlay(
        engine_service.clone(),
        database_manager.clone(),
        org_key.clone(),
        interlay_schema.clone(),
        interlay_endpoints.clone(),
        proxy_runtime,
        shard_router.clone(),
        telemetry_wrapper,
    ) {
        Ok((signal_tx, abort_handles, shutdown_notify)) => {
            register_interlay_state(
                interlay_endpoints,
                interlay_cache_uuid,
                &endpoint_schema,
                interlay_schema,
                org_key.clone(),
                signal_tx,
                abort_handles,
                shutdown_notify,
            );
            Ok(())
        }
        Err(init_err) => {
            let rollback_insert = InsertInterlay::new(org_key.eden_uuid(), old_schema.clone());
            if let Err(rollback_err) = <Db as InsertMethod<InterlaySchema, InterlayCacheUuid, InterlayCacheId, InsertInterlay>>::insert(
                database_manager,
                rollback_insert,
                telemetry_wrapper,
            )
            .await
            {
                log_error!(
                    ctx_with_trace!().with_feature("interlay"),
                    "Failed to rollback interlay schema after init failure — DB may contain broken config",
                    audience = LogAudience::Both,
                    interlay_uuid = interlay_cache_uuid.to_string(),
                    rollback_error = rollback_err.to_string(),
                    init_error = init_err.to_string()
                );
                return Err(error_handling(init_err, span));
            }

            // Best-effort: try to bring old config back up.
            let old_endpoint_uuid = old_schema.endpoint().clone();
            if let Ok(old_ep) =
                <Db as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
                    database_manager,
                    &CacheObjectType::new(Some(EndpointCacheUuid::new(Some(org_key.clone()), old_endpoint_uuid)), None),
                    telemetry_wrapper,
                )
                .await
            {
                if let Ok((signal_tx, abort_handles, shutdown_notify)) = init_interlay(
                    engine_service,
                    database_manager.clone(),
                    org_key.clone(),
                    old_schema.clone(),
                    interlay_endpoints.clone(),
                    proxy_runtime,
                    shard_router,
                    telemetry_wrapper,
                ) {
                    register_interlay_state(
                        interlay_endpoints,
                        interlay_cache_uuid,
                        &old_ep,
                        old_schema,
                        org_key.clone(),
                        signal_tx,
                        abort_handles,
                        shutdown_notify,
                    );
                }
            }

            Err(error_handling(init_err, span))
        }
    }
}

/// Partial update input for an existing interlay.
/// All fields are optional — only provided fields will be updated.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InterlayPatchInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    listeners: Option<Vec<InterlayListener>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertise_host: Option<String>,
    #[serde(default, deserialize_with = "deserialize_patch_tls")]
    tls: PatchTls,
    #[serde(skip_serializing_if = "Option::is_none")]
    settings: Option<InterlaySettings>,
}

/// Update an existing Interlay
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Interlays"],
    path = "/interlays/{interlay}",
    operation_id = "patch_interlay",
    request_body = InterlayPatchInput,
    responses((status = OK, body = CreateInterlayResponse))
)]
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    interlay: web::Path<String>,
    engine_service: web::Data<MyEngineService>,
    database_manager: web::Data<EdenDb>,
    patch_input: web::Json<InterlayPatchInput>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    interlay_locks: web::Data<DashMap<InterlayCacheUuid, Arc<Mutex<()>>>>,
    proxy_runtime: web::Data<tokio::runtime::Handle>,
    shard_router: web::Data<crate::comm::interlays::shard::ShardRouter>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());
    let patch = patch_input.into_inner();

    verify_control_perms(&database_manager, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Resolve the interlay UUID so we can acquire a per-interlay lock.
    let initial_schema = get_interlay_schema(
        &database_manager,
        &CacheObjectType::<InterlayCacheUuid, InterlayCacheId>::from((Some(org_key.clone()), interlay.into_inner())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    let interlay_cache_uuid = InterlayCacheUuid::new(Some(org_key.clone()), initial_schema.uuid());

    // Whether the change requires stopping/restarting the interlay listener.
    let needs_restart_fields = patch.port.is_some()
        || patch.listeners.is_some()
        || patch.advertise_host.is_some()
        || patch.endpoint.is_some()
        || !patch.tls.is_absent();

    // Always acquire the per-interlay lock to serialize concurrent mutations
    // (PATCH, DELETE, START, STOP). Even metadata-only updates must hold the
    // lock so they don't race with DELETE (which removes the DB row and cache
    // entry) — otherwise the cache write here could resurrect a deleted interlay.
    let lock = interlay_locks.entry(interlay_cache_uuid.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
    let _guard = lock.lock().await;

    // Re-fetch the schema under the lock to get the authoritative state,
    // in case a concurrent request modified it before we acquired the lock.
    let mut interlay_schema =
        get_interlay_schema(&database_manager, &CacheObjectType::new(Some(interlay_cache_uuid.clone()), None), telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;
    let mut endpoint_schema =
        <Db as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database_manager,
            &CacheObjectType::new(Some(EndpointCacheUuid::new(Some(org_key.clone()), interlay_schema.endpoint().clone())), None),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let running = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.is_running()).unwrap_or(false);
    if running
        && (patch.listeners.is_some() || patch.advertise_host.is_some() || (interlay_schema.is_multi_listener() && patch.port.is_some()))
    {
        return Err(actix_web::error::ErrorConflict(
            "listener topology changes require the interlay to be stopped first",
        ));
    }

    // Description/settings-only changes are pure metadata updates that don't
    // require a restart.
    if !needs_restart_fields {
        if let Some(ref new_description) = patch.description {
            interlay_schema.set_description(Some(new_description.clone()));
        }
        if let Some(ref new_settings) = patch.settings {
            interlay_schema.set_settings(new_settings.clone());
        }

        validate_multi_listener_interlay_shape(
            endpoint_schema.kind(),
            interlay_schema.listeners(),
            interlay_schema.advertise_host().map(String::as_str),
            interlay_schema.settings(),
        )
        .map_err(actix_web::error::ErrorBadRequest)?;

        validate_interlay_mirror_settings(&database_manager, &org_key, &endpoint_schema, &interlay_schema, telemetry_wrapper)
            .await
            .map_err(actix_web::error::ErrorBadRequest)?;

        database_manager
            .update_interlay_metadata(
                &interlay_schema.uuid(),
                patch.description.as_deref(),
                patch.settings.as_ref(),
                &auth.user_uuid(),
                telemetry_wrapper,
            )
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        <Db as CacheFunctions<InterlaySchema, InterlayCacheUuid, InterlayUuid, InterlayCacheId, InterlayId>>::set_ex_cache(
            &database_manager,
            Some(org_key),
            interlay_schema.clone(),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

        if let Some(mut state) = interlay_endpoints.get_mut(&interlay_cache_uuid) {
            state.update_command_policy(interlay_schema.settings().command_policy_value().cloned());
            state.update_audit_config(interlay_schema.settings().audit_config_value().cloned());
            state.update_mirror(interlay_schema.settings().mirror().clone());
        }

        let response = CreateInterlayResponse::from_schema(interlay_schema, running);
        return Ok(HttpResponse::Ok().json(response));
    }

    let old_schema = interlay_schema.clone();
    let mut needs_restart = false;

    if let Some(new_listeners) = patch.listeners {
        let listeners = normalize_interlay_listeners(patch.port, new_listeners).map_err(actix_web::error::ErrorBadRequest)?;

        let listener_ports = listeners.iter().map(|listener| listener.bind_port()).collect::<Vec<_>>();
        let listener_port_params = listener_ports.iter().map(|port| *port as i32).collect::<Vec<_>>();
        if let Some(existing) = database_manager
            .select_interlay_by_ports(&listener_port_params, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?
            .filter(|e| e.uuid() != interlay_schema.uuid())
        {
            let conflict_port = interlay_conflicting_bind_port(&existing, &listener_ports).unwrap_or(existing.port());
            return Err(actix_web::error::ErrorConflict(format!(
                "Port {} is already in use by interlay '{}'",
                conflict_port,
                existing.id()
            )));
        }

        if listeners != interlay_schema.listeners() {
            interlay_schema.set_listeners(listeners);
            needs_restart = true;
        }
    } else if let Some(new_port) = patch.port {
        if interlay_schema.is_multi_listener() {
            return Err(actix_web::error::ErrorBadRequest(
                "multi-listener interlays must be updated through the `listeners` field",
            ));
        }
        if new_port != interlay_schema.port() {
            validate_port(new_port).map_err(actix_web::error::ErrorBadRequest)?;

            if let Some(existing) = database_manager
                .select_interlay_by_port(new_port as i32, telemetry_wrapper)
                .await
                .map_err(|e| error_handling(e, &mut span))?
                .filter(|e| e.uuid() != interlay_schema.uuid())
            {
                return Err(actix_web::error::ErrorConflict(format!(
                    "Port {} is already in use by interlay '{}'",
                    new_port,
                    existing.id()
                )));
            }

            interlay_schema.set_port(new_port);
            needs_restart = true;
        }
    }

    if let Some(advertise_host) = patch.advertise_host {
        if interlay_schema.advertise_host() != Some(&advertise_host) {
            interlay_schema.set_advertise_host(Some(advertise_host));
            needs_restart = true;
        }
    }

    // Apply endpoint change
    if let Some(ref new_endpoint) = patch.endpoint {
        let new_endpoint_schema =
            <Db as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
                &database_manager,
                &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(org_key.clone()), new_endpoint.clone())),
                telemetry_wrapper,
            )
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        if new_endpoint_schema.uuid() != *interlay_schema.endpoint() {
            interlay_schema.set_endpoint(new_endpoint_schema.uuid());
            needs_restart = true;
        }
        endpoint_schema = new_endpoint_schema;
    }

    // Apply description change
    if let Some(new_description) = patch.description {
        interlay_schema.set_description(Some(new_description));
    }

    // Apply TLS change (tri-state: Absent → skip, Clear → remove, Set → apply)
    if let Some(tls_value) = patch.tls.into_option() {
        interlay_schema.set_tls(tls_value);
        needs_restart = true;
    }

    // Apply settings change — wholesale replacement, not deep-merged.
    // NOTE: Callers must send the complete settings object; omitted fields will revert to defaults.
    if let Some(new_settings) = patch.settings {
        interlay_schema.set_settings(new_settings);
    }

    validate_multi_listener_interlay_shape(
        endpoint_schema.kind(),
        interlay_schema.listeners(),
        interlay_schema.advertise_host().map(String::as_str),
        interlay_schema.settings(),
    )
    .map_err(actix_web::error::ErrorBadRequest)?;

    validate_interlay_mirror_settings(&database_manager, &org_key, &endpoint_schema, &interlay_schema, telemetry_wrapper)
        .await
        .map_err(actix_web::error::ErrorBadRequest)?;

    // Stop the running interlay and wait for the task to drain active sessions
    // before rebinding. This avoids intermittent bind failures when the old
    // listener hasn't closed yet and releases client socket FDs promptly.
    if needs_restart {
        if let Some(state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone()) {
            shutdown_running_interlay(&state).await;
        }

        clear_interlay_runtime_resources(&shard_router, &interlay_cache_uuid, "interlay_patch_restart", telemetry_wrapper).await;
        interlay_endpoints.remove(&interlay_cache_uuid);
    }

    // Persist the updated schema via upsert
    let insert_interlay = InsertInterlay::new(org_key.eden_uuid(), interlay_schema.clone());
    <Db as InsertMethod<InterlaySchema, InterlayCacheUuid, InterlayCacheId, InsertInterlay>>::insert(
        &database_manager,
        insert_interlay,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // Restart the interlay with the updated schema (with rollback on failure)
    if needs_restart {
        restart_interlay_with_rollback(
            engine_service,
            &database_manager,
            &org_key,
            &interlay_schema,
            &old_schema,
            &interlay_endpoints,
            &interlay_cache_uuid,
            proxy_runtime.get_ref(),
            Some(shard_router.into_inner()),
            telemetry_wrapper,
            &mut span,
        )
        .await?;
    }

    let response = CreateInterlayResponse::from_schema(interlay_schema, running);
    Ok(HttpResponse::Ok().json(response))
}
