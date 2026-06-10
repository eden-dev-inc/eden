use dashmap::DashMap;
use database::cache::CacheFunctions;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::format::cache_id::{EndpointCacheId, InterlayCacheId};
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::{CacheObjectType, CacheUuid, EndpointId, InterlayId, InterlayUuid, OrganizationUuid};
use eden_core::{
    format::{OrganizationCacheUuid, endpoint::EpKind},
    telemetry::{FastSpanAttribute, TelemetryWrapper},
};
use eden_gateway_core::traits::{BytesQueueSender, DatabaseProtocolProcessor, ProxyRequestChunk};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_info, log_trace};
use endpoint_schema::endpoint::EndpointSchema;
#[cfg(feature = "redis")]
use endpoints::endpoint::ep_redis::ep::RedisEp;
#[cfg(feature = "llm")]
use endpoints::endpoint::llm::ep::LlmEp;
#[cfg(feature = "mongo")]
use endpoints::endpoint::mongo::ep::MongoEp;
#[cfg(feature = "postgres")]
use endpoints::endpoint::postgres::ep::PostgresEp;
use ep_core::database::schema::interlay::{InterlaySchema, InterlayState};
use ep_core::database::schema::organization::EndpointUuid;
use ep_core::settings::EdenSettings;
use ep_runtime::comp::MyEngineService;
use function_name::named;
#[cfg(feature = "mongo")]
use gateway_mongo::MongoProtocolProcessor;
#[cfg(feature = "postgres")]
use gateway_postgres::PostgresProtocolProcessor;
use std::{net::SocketAddr, sync::Arc, time::Instant};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};

#[cfg(feature = "llm")]
use crate::llm::LlmProtocolProcessor;

/// Background task that processes incoming database protocol requests through
/// the proxy.
///
/// This is the core request processor for the migration-aware database proxy.
/// It receives raw protocol bytes from client connections, resolves the target
/// endpoint from the interlay state, routes through the appropriate protocol
/// processor, and returns response bytes back to the bridge.
pub(crate) struct GatewayProcessor {
    receiver: UnboundedReceiver<ProxyRequestChunk>,
    database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    sender: BytesQueueSender,
    engine_service: Arc<MyEngineService>,
    interlay_cache_uuid: InterlayCacheUuid,
    interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
    organization_cache_uuid: OrganizationCacheUuid,
    eden_settings: EdenSettings,
    telemetry_wrapper: TelemetryWrapper,
    client_addr: SocketAddr,
    listener_id: String,
}

impl GatewayProcessor {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        receiver: UnboundedReceiver<ProxyRequestChunk>,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        sender: BytesQueueSender,
        engine_service: Arc<MyEngineService>,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        organization_cache_uuid: OrganizationCacheUuid,
        eden_settings: EdenSettings,
        telemetry_wrapper: TelemetryWrapper,
        client_addr: SocketAddr,
        listener_id: String,
    ) -> JoinHandle<()> {
        Self {
            receiver,
            database_manager,
            sender,
            engine_service,
            interlay_cache_uuid,
            interlay_endpoints,
            organization_cache_uuid,
            eden_settings,
            telemetry_wrapper,
            client_addr,
            listener_id,
        }
        .spawn_on_runtime()
    }

    fn spawn_on_runtime(self) -> JoinHandle<()> {
        eden_gateway_core::runtime::spawn_on_current_runtime(async move {
            self.run().await;
        })
    }

    #[named]
    async fn run(mut self) {
        let organization_uuid = self.organization_cache_uuid.eden_uuid::<OrganizationUuid>();
        self.telemetry_wrapper.set_org_uuid(organization_uuid.clone());

        let ctx = ctx_with_trace!()
            .with_feature("gateway")
            .with_organization_uuid(organization_uuid.to_string())
            .with_additional("interlay_uuid", self.interlay_cache_uuid.uuid().to_string());

        let Some(interlay_state) = InterlayStateCache::new(
            &self.database_manager,
            &self.interlay_cache_uuid,
            &self.interlay_endpoints,
            &self.organization_cache_uuid,
            &mut self.telemetry_wrapper,
            &ctx,
        )
        .ensure_cached()
        .await
        else {
            return;
        };

        let kind = interlay_state.endpoint_kind();

        log_info!(
            ctx.clone(),
            "Processor started for interlay connection",
            audience = LogAudience::Internal,
            endpoint = format!("{:?}", interlay_state.endpoint_uuid()),
            kind = format!("{}", kind)
        );

        let processor = ProtocolProcessorFactory::new(&self.engine_service, &self.database_manager).get(kind).await;

        if let Some(proc) = processor {
            let response_policy = proc.response_policy_spec();
            debug_assert!(!response_policy.protocol().is_empty());

            proc.process(
                self.receiver,
                self.sender,
                self.eden_settings,
                self.interlay_cache_uuid,
                self.interlay_endpoints,
                self.telemetry_wrapper,
                ctx,
                self.client_addr,
                self.listener_id,
            )
            .await;
        }
    }
}

/// Hydrates an interlay state into the cache if it is not already present.
pub(crate) struct InterlayStateCache<'a> {
    database_manager: &'a Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    interlay_cache_uuid: &'a InterlayCacheUuid,
    interlay_endpoints: &'a Arc<DashMap<InterlayCacheUuid, InterlayState>>,
    organization_cache_uuid: &'a OrganizationCacheUuid,
    telemetry_wrapper: &'a mut TelemetryWrapper,
    ctx: &'a eden_logger_internal::LogContext,
}

impl<'a> InterlayStateCache<'a> {
    pub(crate) fn new(
        database_manager: &'a Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_cache_uuid: &'a InterlayCacheUuid,
        interlay_endpoints: &'a Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        organization_cache_uuid: &'a OrganizationCacheUuid,
        telemetry_wrapper: &'a mut TelemetryWrapper,
        ctx: &'a eden_logger_internal::LogContext,
    ) -> Self {
        Self {
            database_manager,
            interlay_cache_uuid,
            interlay_endpoints,
            organization_cache_uuid,
            telemetry_wrapper,
            ctx,
        }
    }

    /// Returns the resolved state, or `None` if hydration failed and the caller
    /// should abort the connection.
    pub(crate) async fn ensure_cached(self) -> Option<InterlayState> {
        if let Some(state) = self.interlay_endpoints.get(self.interlay_cache_uuid) {
            log_trace!(
                self.ctx.clone(),
                "Got interlay endpoint from cache",
                audience = LogAudience::Internal,
                endpoint = format!("{:?}", state.endpoint_uuid())
            );
            return Some(state.clone());
        }

        log_trace!(
            self.ctx.clone(),
            "Interlay endpoint not in cache, fetching from database",
            audience = LogAudience::Internal
        );

        let hydration_start = Instant::now();
        let mut hydration_span = self.telemetry_wrapper.client_tracer("gateway.interlay_state.hydrate");
        hydration_span.add_event(
            "interlay state cache miss",
            vec![FastSpanAttribute::new("interlay_uuid", self.interlay_cache_uuid.uuid().to_string())],
        );
        log_debug!(
            self.ctx.clone(),
            "Hydrating interlay state",
            audience = LogAudience::Internal,
            interlay_uuid = self.interlay_cache_uuid.uuid().to_string()
        );

        let interlay_schema_result = {
            let mut fetch_span = self.telemetry_wrapper.start_client_span("gateway.interlay_state.fetch_interlay_schema");
            fetch_span.add_event(
                "fetching interlay schema",
                vec![FastSpanAttribute::new("interlay_uuid", self.interlay_cache_uuid.uuid().to_string())],
            );
            let result = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                InterlaySchema,
                InterlayCacheUuid,
                InterlayUuid,
                InterlayCacheId,
                InterlayId,
            >>::get_from_cache(
                self.database_manager,
                &CacheObjectType::new(Some(self.interlay_cache_uuid.clone()), None),
                self.telemetry_wrapper,
            )
            .await;
            if let Err(err) = &result {
                fetch_span.add_event("interlay schema fetch failed", vec![FastSpanAttribute::new("error", err.to_string())]);
            }
            result
        };
        let interlay_schema = match interlay_schema_result {
            Ok(i) => i,
            Err(e) => {
                hydration_span.add_event("interlay state hydration failed", vec![FastSpanAttribute::new("error", e.to_string())]);
                log_error!(
                    self.ctx.clone(),
                    "Failed to fetch interlay schema from cache, closing connection",
                    audience = LogAudience::Internal,
                    interlay_uuid = self.interlay_cache_uuid.uuid().to_string(),
                    error = e.to_string()
                );
                return None;
            }
        };

        let endpoint_schema_result = {
            let mut fetch_span = self.telemetry_wrapper.start_client_span("gateway.interlay_state.fetch_endpoint_schema");
            fetch_span.add_event(
                "fetching endpoint schema",
                vec![FastSpanAttribute::new("endpoint_uuid", interlay_schema.endpoint().to_string())],
            );
            let result = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(
                self.database_manager,
                &CacheObjectType::new(
                    Some(EndpointCacheUuid::new(Some(self.organization_cache_uuid.clone()), interlay_schema.endpoint().clone()).clone()),
                    None,
                ),
                self.telemetry_wrapper,
            )
            .await;
            if let Err(err) = &result {
                fetch_span.add_event("endpoint schema fetch failed", vec![FastSpanAttribute::new("error", err.to_string())]);
            }
            result
        };
        let endpoint_schema = match endpoint_schema_result {
            Ok(e) => e,
            Err(e) => {
                hydration_span.add_event("interlay state hydration failed", vec![FastSpanAttribute::new("error", e.to_string())]);
                log_error!(
                    self.ctx.clone(),
                    "Failed to fetch endpoint schema from cache, closing connection",
                    audience = LogAudience::Internal,
                    interlay_uuid = self.interlay_cache_uuid.uuid().to_string(),
                    endpoint_uuid = interlay_schema.endpoint().to_string(),
                    error = e.to_string()
                );
                return None;
            }
        };

        log_trace!(
            self.ctx.clone(),
            "Adding endpoint to interlay endpoints cache",
            audience = LogAudience::Internal,
            endpoint_uuid = endpoint_schema.endpoint_uuid().to_string()
        );

        let mut state = InterlayState::new(
            endpoint_schema.cache_key(self.organization_cache_uuid.clone()),
            endpoint_schema.kind(),
            endpoint_schema.routing(),
            interlay_schema.settings().command_policy_value().cloned(),
            interlay_schema.settings().audit_config_value().cloned(),
            interlay_schema.settings().mirror().clone(),
        );
        state.update_listener_config(interlay_schema.listeners().to_vec(), interlay_schema.advertise_host().cloned());

        self.interlay_endpoints.entry(self.interlay_cache_uuid.clone()).or_insert(state.clone());
        let hydration_duration_us = hydration_start.elapsed().as_micros() as u64;
        hydration_span.add_event(
            "interlay state hydrated",
            vec![
                FastSpanAttribute::new("endpoint_uuid", endpoint_schema.endpoint_uuid().to_string()),
                FastSpanAttribute::new("endpoint_kind", endpoint_schema.kind().to_string()),
                FastSpanAttribute::new("duration_us", hydration_duration_us.to_string()),
            ],
        );
        log_debug!(
            self.ctx.clone(),
            "Hydrated interlay state",
            audience = LogAudience::Internal,
            endpoint_uuid = endpoint_schema.endpoint_uuid().to_string(),
            endpoint_kind = endpoint_schema.kind().to_string(),
            duration_us = hydration_duration_us
        );
        Some(state)
    }
}

struct ProtocolProcessorFactory<'a> {
    engine_service: &'a Arc<MyEngineService>,
    database_manager: &'a Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
}

impl<'a> ProtocolProcessorFactory<'a> {
    fn new(
        engine_service: &'a Arc<MyEngineService>,
        database_manager: &'a Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    ) -> Self {
        Self { engine_service, database_manager }
    }

    async fn get(&self, kind: EpKind) -> Option<Box<dyn DatabaseProtocolProcessor>> {
        #[cfg(any(feature = "redis", feature = "postgres", feature = "mongo", feature = "llm"))]
        {
            let lock = self.engine_service.router.read().await;

            match kind {
                #[cfg(feature = "redis")]
                EpKind::Redis => {
                    if let Some(route) = lock.get(&kind)
                        && let Some(ep) = route.as_any().downcast_ref::<RedisEp>()
                    {
                        return Some(Box::new(gateway_redis::RedisProtocolProcessor::new(ep.clone(), self.database_manager.clone())));
                    }
                    None
                }
                #[cfg(feature = "postgres")]
                EpKind::Postgres => {
                    if let Some(route) = lock.get(&kind)
                        && let Some(ep) = route.as_any().downcast_ref::<PostgresEp>()
                    {
                        let processor =
                            PostgresProtocolProcessor::new(ep.clone()).with_rbac_redis(self.database_manager.rbac_redis_pool().clone());
                        return Some(Box::new(processor));
                    }
                    None
                }
                #[cfg(feature = "mongo")]
                EpKind::Mongo => {
                    if let Some(route) = lock.get(&kind)
                        && let Some(ep) = route.as_any().downcast_ref::<MongoEp>()
                    {
                        return Some(Box::new(MongoProtocolProcessor::new(ep.clone())));
                    }
                    None
                }
                #[cfg(feature = "llm")]
                EpKind::Llm => {
                    if let Some(route) = lock.get(&kind)
                        && let Some(ep) = route.as_any().downcast_ref::<LlmEp>()
                    {
                        return Some(Box::new(LlmProtocolProcessor::new(ep.clone())));
                    }
                    None
                }
                _ => None,
            }
        }
        #[cfg(not(any(feature = "redis", feature = "postgres", feature = "mongo", feature = "llm")))]
        {
            let _ = self.engine_service;
            let _ = self.database_manager;
            let _ = kind;
            None
        }
    }
}
