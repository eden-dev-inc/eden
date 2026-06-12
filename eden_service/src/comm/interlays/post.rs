use crate::EdenDb;
use crate::comm::interlays::start::{bind_interlay_listener, start_interlay, validate_interlay_tls_configuration};
use crate::comm::interlays::{
    get_interlay_schema, interlay_conflicting_bind_port, normalize_interlay_listeners, reconnect_interlay_runtime_endpoints,
    validate_interlay_mirror_settings, validate_multi_listener_interlay_shape,
};
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use database::cache::CacheFunctions;
use database::db::methods::insert::endpoint::InsertEndpoint;
use database::methods::insert::InsertMethod;
use database::methods::insert::interlay::InsertInterlay;
use eden_core::auth::ParsedJwt;
use eden_core::comm::NodeData;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{EndpointCacheId, InterlayCacheId};
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{
    CacheObjectType, CacheUuid, EdenNodeUuid, EndpointId, EndpointUuid, InterlayId, InterlayUuid, OrganizationCacheUuid,
};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::interlay::{
    InterlayListener, InterlaySchema, InterlaySettings, InterlaySignal, InterlayState,
};
use endpoint_core::ep_core::database::schema::interlay_tls::{InterlayTls, deserialize_interlay_tls};
use endpoint_schema::EndpointSchemaInput;
use endpoint_schema::endpoint::EndpointSchema;
use ep_runtime::comp::MyEngineService;
use function_name::named;
// use futures::stream::AbortHandle;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use telemetry_extensions_macro::with_telemetry;
use tokio::sync::{Notify, broadcast};
use tokio::task::AbortHandle;
use utoipa::ToSchema;

/// The input fields of a new interlay. If the EndpointId matches an existing endpoint we will
/// assume the user wants to use the existing Endpoints Templates / APIs.
///
/// We will validate that the user wants to connect to the same Endpoint by checking the config
/// information. If the user uses an existing `EndpointId` but the Config is different we will
/// return an error: `Error: EndpointId already exists`
///
/// InterlayInput.endpoint can be either an `EndpointId` or `EndpointUuid`
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InterlayInput {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    listeners: Vec<InterlayListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertise_host: Option<String>,
    #[serde(deserialize_with = "deserialize_interlay_tls")]
    tls: Option<InterlayTls>,
    settings: InterlaySettings,
    /// Optional inline endpoint configuration. If `endpoint` does not match an existing
    /// endpoint, this config will be used to create a new endpoint on the fly.
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint_config: Option<EndpointSchemaInput>,
}

/// Response body for interlay creation/retrieval endpoint.
/// Contains all essential information about the created or existing interlay.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateInterlayResponse {
    /// Human-readable identifier for the interlay
    pub id: InterlayId,

    /// Unique UUID identifier
    pub uuid: InterlayUuid,

    /// The endpoint UUID this interlay routes traffic to
    pub endpoint: EndpointUuid,

    /// Listener topology for this interlay
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<InterlayListener>,

    /// Shared advertised hostname for cluster listeners
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_host: Option<String>,

    /// Legacy single-port shorthand, returned only when exactly one listener exists
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    /// TLS configuration if enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<InterlayTls>,

    /// Interlay request and feature settings.
    pub settings: InterlaySettings,

    /// Whether the interlay is running/active
    pub running: bool,

    /// Timestamp when the interlay was created
    pub created_at: DateTime<Utc>,

    /// Timestamp when the interlay was last updated
    pub updated_at: DateTime<Utc>,
}

impl CreateInterlayResponse {
    /// Create a response from an InterlaySchema and running status
    pub fn from_schema(schema: InterlaySchema, running: bool) -> Self {
        Self {
            id: schema.id(),
            uuid: schema.uuid(),
            endpoint: schema.endpoint().clone(),
            listeners: schema.listeners().to_vec(),
            advertise_host: schema.advertise_host().cloned(),
            port: (schema.listeners().len() == 1).then_some(schema.port()),
            tls: schema.tls().cloned(),
            settings: schema.settings().clone(),
            running,
            created_at: schema.created_at(),
            updated_at: schema.updated_at(),
        }
    }
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Interlay"],
    path="/interlays",
    responses((status = CREATED, body = CreateInterlayResponse))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn post(
    auth: web::ReqData<ParsedJwt>,
    engine_service: web::Data<MyEngineService>,
    database_manager: web::Data<EdenDb>,
    node_data: web::Data<NodeData>,
    interlay: web::Json<InterlayInput>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    proxy_runtime: web::Data<tokio::runtime::Handle>,
    shard_router: web::Data<crate::comm::interlays::shard::ShardRouter>,
) -> Result<impl Responder, actix_web::Error> {
    let interlay_input = interlay.into_inner();

    verify_control_perms(&database_manager, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let parsed_jwt = auth.into_inner();
    let created_by = parsed_jwt.user_uuid().clone();
    let organization_cache_uuid = OrganizationCacheUuid::new(None, parsed_jwt.org_uuid().clone());

    // Resolve endpoint: try cache first, create on the fly if endpoint_config is provided
    let endpoint_schema =
        match <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database_manager,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((
                Some(organization_cache_uuid.clone()),
                interlay_input.endpoint.clone(),
            )),
            telemetry_wrapper,
        )
        .await
        {
            Ok(schema) => schema,
            Err(e) => {
                let Some(endpoint_input) = interlay_input.endpoint_config.clone() else {
                    return Err(error_handling(e, &mut span));
                };
                // Endpoint doesn't exist but inline config was provided — create it on the fly
                let endpoint_schema =
                    EndpointSchema::try_from((endpoint_input, created_by.clone())).map_err(actix_web::error::ErrorBadRequest)?;
                let insert_endpoint =
                    InsertEndpoint::new(organization_cache_uuid.eden_uuid(), endpoint_schema.clone(), node_data.uuid().clone());
                engine_service
                    .connect(&database_manager, &insert_endpoint, telemetry_wrapper)
                    .await
                    .map_err(actix_web::error::ErrorInternalServerError)?;
                endpoint_schema
            }
        };

    let listeners =
        normalize_interlay_listeners(interlay_input.port, interlay_input.listeners).map_err(actix_web::error::ErrorBadRequest)?;
    validate_multi_listener_interlay_shape(
        endpoint_schema.kind(),
        &listeners,
        interlay_input.advertise_host.as_deref(),
        &interlay_input.settings,
    )
    .map_err(actix_web::error::ErrorBadRequest)?;

    let listener_ports = listeners.iter().map(|listener| listener.bind_port()).collect::<Vec<_>>();
    let listener_port_params = listener_ports.iter().map(|port| *port as i32).collect::<Vec<_>>();
    if let Some(existing) = database_manager
        .select_interlay_by_ports(&listener_port_params, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?
    {
        if existing.id().to_string() != interlay_input.id {
            let conflict_port = interlay_conflicting_bind_port(&existing, &listener_ports).unwrap_or(existing.port());
            return Err(actix_web::error::ErrorConflict(format!(
                "Port {} is already in use by interlay '{}'",
                conflict_port,
                existing.id()
            )));
        }
    }

    // Check if Interlay already exists. If not insert the Interlay to the database
    let interlay_schema = match get_interlay_schema(
        &database_manager,
        &CacheObjectType::<InterlayCacheUuid, InterlayCacheId>::from((Some(organization_cache_uuid.clone()), interlay_input.id.clone())),
        telemetry_wrapper,
    )
    .await
    {
        Ok(interlay_schema) => interlay_schema,
        Err(_) => {
            let interlay_schema = InterlaySchema::new(
                interlay_input.id.clone(),
                interlay_input.description.clone(),
                endpoint_schema.uuid(),
                listeners.first().map(|listener| listener.bind_port()).unwrap_or_default(),
                interlay_input.tls.clone(),
                Some(interlay_input.settings.clone()),
                created_by.clone(),
            );
            let interlay_schema = if listeners.len() == 1 && interlay_input.advertise_host.is_none() {
                interlay_schema
            } else {
                InterlaySchema::new_with_listeners(
                    interlay_input.id,
                    interlay_input.description,
                    endpoint_schema.uuid(),
                    listeners.clone(),
                    interlay_input.advertise_host.clone(),
                    interlay_input.tls,
                    Some(interlay_input.settings),
                    created_by.clone(),
                )
            };

            validate_interlay_mirror_settings(
                &database_manager,
                &organization_cache_uuid,
                &endpoint_schema,
                &interlay_schema,
                telemetry_wrapper,
            )
            .await
            .map_err(actix_web::error::ErrorBadRequest)?;

            for listener in interlay_schema.listeners() {
                bind_interlay_listener(listener.bind_port()).map(drop).map_err(|e| actix_web::error::ErrorConflict(e.to_string()))?;
            }
            validate_interlay_tls_configuration(&interlay_schema)?;

            let insert_interlay = InsertInterlay::new(organization_cache_uuid.eden_uuid(), interlay_schema.clone());
            <EdenDb as InsertMethod<InterlaySchema, InterlayCacheUuid, InterlayCacheId, InsertInterlay>>::insert(
                &database_manager,
                insert_interlay,
                telemetry_wrapper,
            )
            .await?;

            interlay_schema
        }
    };

    let interlay_cache_uuid = InterlayCacheUuid::new(Some(organization_cache_uuid.clone()), interlay_schema.uuid());

    // Check if the interlay is already running by checking the abort handle in InterlayState
    let already_running = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.is_running()).unwrap_or(false);

    // Only initialize if the interlay is not already running
    if !already_running {
        reconnect_interlay_runtime_endpoints(
            &engine_service,
            &database_manager,
            &organization_cache_uuid,
            &endpoint_schema,
            &interlay_schema,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

        // Create the InterlayState first (without shutdown/abort handles)
        // Routing is sourced from the endpoint, not the interlay
        let mut interlay_state = InterlayState::new(
            endpoint_schema.cache_key(organization_cache_uuid.clone()),
            endpoint_schema.kind(),
            endpoint_schema.routing(),
            interlay_schema.settings().command_policy_value().cloned(),
            interlay_schema.settings().audit_config_value().cloned(),
            interlay_schema.settings().mirror().clone(),
        );
        interlay_state.update_listener_config(interlay_schema.listeners().to_vec(), interlay_schema.advertise_host().cloned());

        // Initialize the interlay and get signal_tx + abort_handle + shutdown_notify
        let (signal_tx, abort_handles, shutdown_notify) = init_interlay(
            engine_service,
            database_manager,
            organization_cache_uuid,
            interlay_schema.clone(),
            interlay_endpoints.clone(),
            proxy_runtime.get_ref(),
            Some(shard_router.into_inner()),
            telemetry_wrapper,
        )?;

        // Set the signal channel, abort handle, and shutdown notify
        interlay_state.set_signal_tx(signal_tx);
        interlay_state.set_abort_handles(abort_handles);
        interlay_state.set_shutdown_notify(shutdown_notify);

        // Insert the fully initialized state
        interlay_endpoints.insert(interlay_cache_uuid.clone(), interlay_state);
    }

    // `running`` is true if the interlay task is actively running
    let running = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.is_running()).unwrap_or(false);

    let response = CreateInterlayResponse::from_schema(interlay_schema, running);
    Ok(HttpResponse::Created().json(response))
}

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
#[named]
pub async fn reconnect_interlays(
    engine_service: web::Data<MyEngineService>,
    database_manager: web::Data<EdenDb>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    _eden_node_uuid: EdenNodeUuid,
    proxy_runtime: &web::Data<tokio::runtime::Handle>,
    shard_router: Option<Arc<crate::comm::interlays::shard::ShardRouter>>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    let mut span = telemetry_wrapper.client_tracer(format!("interlay.{}", function_name!()));

    for organization_schema in &database_manager.select_all_organizations().await? {
        let organization_cache_uuid = OrganizationCacheUuid::new(None, organization_schema.uuid());

        for interlay_schema in &database_manager.select_all_interlays(&organization_schema.uuid(), telemetry_wrapper).await? {
            // Resolve the endpoint schema for this interlay
            let endpoint_schema = match <EdenDb as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(
                &database_manager,
                &CacheObjectType::new(
                    Some(EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), interlay_schema.endpoint().clone())),
                    None,
                ),
                telemetry_wrapper,
            )
            .await
            {
                Ok(schema) => schema,
                Err(_) => continue, // Endpoint not found — skip this interlay
            };

            if let Err(error) = reconnect_interlay_runtime_endpoints(
                engine_service.get_ref(),
                database_manager.get_ref(),
                &organization_cache_uuid,
                &endpoint_schema,
                interlay_schema,
                telemetry_wrapper,
            )
            .await
            {
                error_handling(error, &mut span);
                continue;
            }

            // Create interlay state and initialize the interlay
            let interlay_cache_uuid = InterlayCacheUuid::new(Some(organization_cache_uuid.clone()), interlay_schema.uuid());

            match init_interlay(
                engine_service.clone(),
                database_manager.clone(),
                organization_cache_uuid.clone(),
                interlay_schema.to_owned(),
                interlay_endpoints.clone(),
                proxy_runtime.get_ref(),
                shard_router.clone(),
                telemetry_wrapper,
            ) {
                Ok((signal_tx, abort_handles, shutdown_notify)) => {
                    let mut interlay_state = InterlayState::new(
                        endpoint_schema.cache_key(organization_cache_uuid.clone()),
                        endpoint_schema.kind(),
                        endpoint_schema.routing(),
                        interlay_schema.settings().command_policy_value().cloned(),
                        interlay_schema.settings().audit_config_value().cloned(),
                        interlay_schema.settings().mirror().clone(),
                    );
                    interlay_state.update_listener_config(interlay_schema.listeners().to_vec(), interlay_schema.advertise_host().cloned());
                    interlay_state.set_signal_tx(signal_tx);
                    interlay_state.set_abort_handles(abort_handles);
                    interlay_state.set_shutdown_notify(shutdown_notify);
                    interlay_endpoints.insert(interlay_cache_uuid, interlay_state);
                }
                Err(e) => {
                    error_handling(e, &mut span);
                    // log errors, but try all interlays even if some fail
                }
            }
        }
    }
    Ok(())
}

type InterlayRuntimeHandles = (broadcast::Sender<InterlaySignal>, Vec<AbortHandle>, Arc<Notify>);

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub(crate) fn init_interlay(
    engine_service: web::Data<MyEngineService>,
    database_manager: web::Data<EdenDb>,
    organization_cache_uuid: OrganizationCacheUuid,
    interlay_schema: InterlaySchema,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    proxy_runtime: &tokio::runtime::Handle,
    shard_router: Option<Arc<crate::comm::interlays::shard::ShardRouter>>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<InterlayRuntimeHandles, EpError> {
    let listeners = interlay_schema
        .listeners()
        .iter()
        .cloned()
        .map(|listener| bind_interlay_listener(listener.bind_port()).map(|tcp_listener| (listener, tcp_listener)))
        .collect::<Result<Vec<_>, _>>()?;

    let (signal_tx, signal_rx) = broadcast::channel(16);

    // Notification fired when the spawned task exits (gracefully or via abort),
    // so callers can await actual port release before rebinding.
    let shutdown_notify = Arc::new(Notify::new());
    let remaining_tasks = Arc::new(AtomicUsize::new(listeners.len().max(1)));

    // Extract owned values before the async block to avoid lifetime issues
    let engine = engine_service.into_inner();
    let db = database_manager.into_inner();
    let eps = interlay_endpoints.into_inner();
    let tw = telemetry_wrapper.clone();

    let mut abort_handles = Vec::with_capacity(listeners.len());
    for (listener_cfg, listener) in listeners {
        let task_notify = shutdown_notify.clone();
        let task_remaining = remaining_tasks.clone();
        let engine = engine.clone();
        let db = db.clone();
        let org = organization_cache_uuid.clone();
        let schema = interlay_schema.clone();
        let eps = eps.clone();
        let tw = tw.clone();
        let signal_rx = signal_tx.subscribe();
        let shard_router = shard_router.clone();

        let interlay_task = proxy_runtime.spawn(async move {
            let _notify_guard = NotifyOnDrop::counted(task_notify, task_remaining);
            start_interlay(
                listener,
                listener_cfg.id().to_string(),
                listener_cfg.bind_port(),
                engine,
                db,
                org,
                schema,
                eps,
                signal_rx,
                tw,
                shard_router,
            )
            .await;
        });
        abort_handles.push(interlay_task.abort_handle());
    }

    drop(signal_rx);

    Ok((signal_tx, abort_handles, shutdown_notify))
}

/// Drop guard that fires a [`Notify`] when the interlay task exits,
/// whether by graceful shutdown or forced abort.
pub(crate) struct NotifyOnDrop {
    pub(crate) notify: Arc<Notify>,
    pub(crate) remaining: Option<Arc<AtomicUsize>>,
}

impl NotifyOnDrop {
    pub(crate) fn counted(notify: Arc<Notify>, remaining: Arc<AtomicUsize>) -> Self {
        Self { notify, remaining: Some(remaining) }
    }
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        if let Some(remaining) = &self.remaining {
            if remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.notify.notify_one();
            }
        } else {
            self.notify.notify_one();
        }
    }
}

#[cfg(any())]
mod tests {
    use super::*;
    use crate::test_utils::redis_migrate_test_utils::connect_to_multi_redis;
    use actix_web::web;
    use dashmap::DashMap;
    use database::methods::insert::InsertMethod;
    use database::methods::insert::interlay::InsertInterlay;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheUuid, OrganizationUuid, UserUuid};
    use endpoint_core::ep_core::GetPool;
    use endpoints::endpoint::ep_redis::ep::RedisEp;
    use std::time::Duration;

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
        let port = listener.local_addr().expect("listener addr").port();
        drop(listener);
        port
    }

    #[tokio::test]
    async fn reconnect_interlays_restores_existing_endpoint_pools() {
        let (endpoints, _engine_service, database_manager, organization_schema, mut telemetry) = connect_to_multi_redis(2).await;
        let origin_endpoint = endpoints[0].1.clone();
        let origin_schema = endpoints[0].2.clone();
        let mirror_endpoint = endpoints[1].1.clone();
        let mirror_schema = endpoints[1].2.clone();
        let mirror_settings: InterlaySettings = serde_json::from_value(serde_json::json!({
            "mirror": {
                "enabled": true,
                "mode": "mirror",
                "mirror_endpoint_uuids": [mirror_schema.uuid()],
                "mirror_reads": true,
                "mirror_writes": true,
                "sample_ratio": 1.0,
                "max_in_flight_per_mirror": 128
            }
        }))
        .expect("mirror settings should deserialize");

        let organization_cache_uuid = OrganizationCacheUuid::new(None, organization_schema.uuid());
        let interlay_schema = InterlaySchema::new(
            "startup-reconnect".into(),
            None,
            origin_schema.uuid().clone(),
            free_port(),
            None,
            Some(mirror_settings),
            None,
            UserUuid::new_uuid(),
        );

        let database_manager = web::Data::new(database_manager);
        <EdenDb as InsertMethod<InterlaySchema, InterlayCacheUuid, InterlayCacheId, InsertInterlay>>::insert(
            database_manager.get_ref(),
            InsertInterlay::new(organization_cache_uuid.eden_uuid::<OrganizationUuid>().clone(), interlay_schema.clone()),
            &mut telemetry,
        )
        .await
        .expect("insert interlay");

        let fresh_engine_service = web::Data::new(MyEngineService::default());
        let interlay_endpoints = web::Data::new(DashMap::new());
        let migration_states = web::Data::new(DashMap::new());
        let migration_lock = web::Data::new(DashMap::new());
        let proxy_runtime = web::Data::new(tokio::runtime::Handle::current());

        reconnect_interlays(
            fresh_engine_service.clone(),
            database_manager.clone(),
            interlay_endpoints.clone(),
            migration_states,
            migration_lock,
            organization_schema.eden_node_uuids()[0].clone(),
            &proxy_runtime,
            None,
            &mut telemetry,
        )
        .await
        .expect("reconnect interlays");

        let router = fresh_engine_service.router.read().await;
        let redis_ep = router.get(&EpKind::Redis).expect("redis router").as_any().downcast_ref::<RedisEp>().expect("redis router type");
        assert!(
            redis_ep.pool().pool().contains_key(&origin_endpoint),
            "startup reconnect should restore the persisted Redis endpoint pool"
        );
        assert!(
            redis_ep.pool().pool().contains_key(&mirror_endpoint),
            "startup reconnect should restore Redis mirror endpoint pools"
        );
        drop(router);

        let interlay_cache_uuid = InterlayCacheUuid::new(Some(organization_cache_uuid.clone()), interlay_schema.uuid());
        let state = interlay_endpoints.get(&interlay_cache_uuid).expect("interlay state");
        assert!(state.is_running(), "reconnect should restart the persisted interlay");

        let shutdown_notify = state.shutdown_notify().cloned().expect("shutdown notify");
        state.abort();
        drop(state);

        tokio::time::timeout(Duration::from_secs(5), shutdown_notify.notified()).await.expect("aborted interlay should exit");
    }
}
