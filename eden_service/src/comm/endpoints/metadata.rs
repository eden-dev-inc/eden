use crate::EdenDb;
use crate::comm::endpoints::hydrate_llm_endpoint_config;
use crate::comm::rbac::{AuthMode, verify_endpoint_access};

use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
#[cfg(all(feature = "poll-clickhouse", not(embedded_db)))]
use analytics_schema::insert_batch;
#[cfg(feature = "poll-clickhouse")]
use analytics_schema::poll::{
    CassandraPollMetricsRow, ClickhousePollMetricsRow, MongoPollMetricsRow, OraclePollMetricsRow, PostgresPollMetricsRow,
    RedisPollMetricsRow, tables as poll_tables,
};
use backon::{BackoffBuilder, ExponentialBuilder};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, MetadataError};
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use ep_runtime::comp::MyEngineService;
// Trait import in scope so `EndpointUuid::uuid()` resolves below; method use only.
#[allow(unused_imports)]
use eden_core::format::EdenUuid;
#[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
use eden_core::format::OrganizationUuid;
use eden_core::format::rbac::DataPerms;
use eden_core::format::{CacheObjectType, EdenNodeUuid, EndpointId, EndpointUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{AllMetrics, FastSpanAttribute, TelemetryDurations, TelemetryWrapper};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_trace};
#[cfg(feature = "all-endpoints")]
use endpoint_core::cassandra_core::CassandraAsync;
#[cfg(feature = "all-endpoints")]
use endpoint_core::clickhouse_core::ClickhouseAsync;
#[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
use endpoint_core::ep_core::GetPool;
use endpoint_core::ep_core::ep::EpRouter;
use endpoint_core::ep_core::settings::EdenSettings;
#[cfg(feature = "all-endpoints")]
use endpoint_core::http_core::HttpAsync;
#[cfg(feature = "all-endpoints")]
use endpoint_core::llm_core::LlmAsync;
#[cfg(feature = "mongo")]
use endpoint_core::mongo_core::MongoAsync;
#[cfg(feature = "all-endpoints")]
use endpoint_core::mssql_core::MssqlAsync;
#[cfg(feature = "all-endpoints")]
use endpoint_core::mysql_core::MysqlAsync;
#[cfg(feature = "all-endpoints")]
use endpoint_core::oracle_core::OracleAsync;
#[cfg(feature = "all-endpoints")]
use endpoint_core::pinecone_core::PineconeAsync;
#[cfg(feature = "postgres")]
use endpoint_core::postgres_core::PostgresAsync;
#[cfg(feature = "redis")]
use endpoint_core::redis_core::RedisAsync;
use endpoint_schema::endpoint::EndpointSchema;
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::cassandra::{ep::CassandraEp, metadata::CassandraMetadata, output::CassandraOutput};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::clickhouse::{ep::ClickhouseEp, metadata::ClickhouseMetadata};
#[cfg(feature = "redis")]
use endpoints::endpoint::ep_redis::output::RedisEndpointOutput;
#[cfg(feature = "redis")]
use endpoints::endpoint::ep_redis::{ep::RedisEp, metadata::RedisMetadata};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::http::{ep::HttpEp, metadata::HttpMetadata};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::llm::{ep::LlmEp, metadata::LlmMetadata};
use endpoints::endpoint::metadata::{
    BackoffConfig, JobErrorMode, MetadataBatch, MetadataConfig, MetadataOutputs, SchedulerIntervals, SyncFrequency,
    default_publisher_with_prefix,
};
#[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
use endpoints::endpoint::metadata::{EpMetadata, SyncMetadata, run_metadata_jobs_with_capabilities};
#[cfg(feature = "mongo")]
use endpoints::endpoint::mongo::{ep::MongoEp, metadata::MongoMetadata, output::MongoOutput};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::mssql::{ep::MssqlEp, metadata::MssqlMetadata};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::mysql::{ep::MysqlEp, metadata::MysqlMetadata};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::oracle::{ep::OracleEp, metadata::OracleMetadata};
#[cfg(feature = "all-endpoints")]
use endpoints::endpoint::pinecone::{ep::PineconeEp, metadata::PineconeMetadata};
#[cfg(feature = "postgres")]
use endpoints::endpoint::postgres::api::wrapper::output::PostgresOutput;
#[cfg(feature = "postgres")]
use endpoints::endpoint::postgres::{ep::PostgresEp, metadata::PostgresMetadata};
use function_name::named;
#[allow(unused_imports)]
use opentelemetry::trace::TraceContextExt;
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use telemetry_extensions_macro::with_telemetry;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval};
use utoipa::{IntoParams, ToSchema};

fn reject_metadata_when_els_active(auth_mode: AuthMode) -> Result<(), EpError> {
    if auth_mode == AuthMode::Els {
        return Err(EpError::auth(
            "Metadata introspection is not available when Endpoint-Level Security is active for this user".to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "poll-clickhouse")]
// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
enum PollMetricsRow {
    Redis(RedisPollMetricsRow),
    Postgres(PostgresPollMetricsRow),
    Mongo(MongoPollMetricsRow),
    Oracle(OraclePollMetricsRow),
    Cassandra(CassandraPollMetricsRow),
    Clickhouse(ClickhousePollMetricsRow),
}

#[derive(ToSchema)]
#[schema(title = "Endpoint response")]
// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
pub enum ReadResponse {
    #[cfg(feature = "mongo")]
    #[schema(title = "MongoDB")]
    MongoResponse(MongoOutput),
    #[cfg(feature = "all-endpoints")]
    #[schema(title = "Cassandra")]
    CassandraResponse(CassandraOutput),
    #[cfg(feature = "postgres")]
    #[schema(title = "PostgreSQL")]
    Postgres(PostgresOutput),
    #[cfg(feature = "redis")]
    #[schema(title = "Redis")]
    RedisResponse(RedisEndpointOutput),
}

/// Manual metadata data from an Endpoint with gRPC
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/metadata",
    operation_id = "get_endpoint_metadata",
    params(
        ("endpoint" = String, Path, description = "Endpoint identifier"),
        MetadataQuery
    ),
    responses(
        (status = OK, description = "Endpoint read response", body = serde_json::Value),
    )
)]
pub async fn metadata(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    query: Option<web::Query<MetadataQuery>>,
    engine_service: web::Data<MyEngineService>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let settings = EdenSettings::from(req.headers());
    let package = query.as_ref().and_then(|q| q.package.as_deref());
    let packages = package.map(|pkg| vec![pkg.to_string()]);

    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let mut endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let _attributes = [FastSpanAttribute::new("uuid", endpoint_schema.endpoint_uuid().to_string())];
    span.add_event(
        "collected `endpoint_uuid` from cache".to_string(),
        vec![FastSpanAttribute::new("uuid", endpoint_schema.endpoint_uuid().to_string())],
    );

    //TODO check if the endpoint is managed by the local engine
    let endpoint_cache_uuid = endpoint_schema.cache_key(organization_cache_uuid.clone());
    let auth_mode = verify_endpoint_access(
        &database,
        &auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .inspect(|_| span.add_event("Verified RBAC", vec![]))?;

    reject_metadata_when_els_active(auth_mode).map_err(|e| error_handling(e, &mut span))?;

    hydrate_llm_endpoint_config(&database, &mut endpoint_schema, auth.org_uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_event("`rbac` passed".to_string(), vec![]);

    let response = engine_service
        .metadata(&database, &endpoint_schema, organization_cache_uuid, settings, packages, None, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(Response(response)).into();

    response
}

/// List available collectors for an endpoint
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/metadata/collectors",
    operation_id = "get_endpoint_metadata_collectors",
    params(
        ("endpoint" = String, Path, description = "Endpoint identifier")
    ),
    responses(
        (status = OK, description = "Metadata collectors", body = CollectorsResponse)
    )
)]
pub async fn metadata_collectors(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    engine_service: web::Data<MyEngineService>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_cache_uuid = endpoint_schema.cache_key(organization_cache_uuid);
    let auth_mode = verify_endpoint_access(
        &database,
        &auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;
    reject_metadata_when_els_active(auth_mode).map_err(|e| error_handling(e, &mut span))?;

    let collectors = engine_service.metadata_collectors(&endpoint_schema).await.map_err(|e| error_handling(e, &mut span))?;

    let response = CollectorsResponse {
        collectors: collectors
            .into_iter()
            .map(|info| CollectorSummary {
                package: info.package().to_string(),
                priority: info.frequency().as_str().to_string(),
            })
            .collect(),
    };

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(response).into();

    response
}

/// Read the latest scheduled metadata batch for an endpoint from the internal cache.
/// **Permissions**: Same read checks as live metadata collection.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/metadata/cache",
    operation_id = "get_endpoint_cached_metadata",
    params(
        ("endpoint" = String, Path, description = "Endpoint identifier"),
        CachedMetadataQuery
    ),
    responses(
        (status = OK, description = "Cached metadata batch", body = serde_json::Value),
        (status = NOT_FOUND, description = "No cached metadata batch for endpoint/frequency")
    )
)]
pub async fn metadata_cache(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    query: web::Query<CachedMetadataQuery>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let frequency = match SyncFrequency::parse(query.frequency.trim()) {
        Some(frequency) => frequency,
        None => {
            return Err(error_handling(EpError::request("frequency must be one of: high, medium, low"), &mut span));
        }
    };
    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_cache_uuid = endpoint_schema.cache_key(organization_cache_uuid);
    let auth_mode = verify_endpoint_access(
        &database,
        &auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;
    reject_metadata_when_els_active(auth_mode).map_err(|e| error_handling(e, &mut span))?;

    let prefix = eden_config::analytics().metadata.redis_prefix.clone();
    let publisher = default_publisher_with_prefix(database.into_inner(), prefix);
    let Some(payload) = publisher.read(&endpoint_cache_uuid, frequency).await.map_err(|e| error_handling(e, &mut span))? else {
        return Ok(actix_web::HttpResponse::NotFound().json(serde_json::json!({
            "error": "cached_metadata_not_found",
            "frequency": frequency.as_str(),
        })));
    };
    let value: serde_json::Value = serde_json::from_str(&payload).map_err(|e| error_handling(EpError::serde(e), &mut span))?;

    Ok(actix_web::HttpResponse::Ok().json(value))
}

/// Trigger an immediate metadata collection for specific packages
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/metadata/collect",
    operation_id = "collect_endpoint_metadata",
    params(
        ("endpoint" = String, Path, description = "Endpoint identifier")
    ),
    request_body = MetadataCollectRequest,
    responses(
        (status = OK, description = "Collected metadata", body = serde_json::Value)
    )
)]
pub async fn metadata_collect(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    body: web::Json<MetadataCollectRequest>,
    engine_service: web::Data<MyEngineService>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let settings = EdenSettings::from(req.headers());

    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_cache_uuid = endpoint_schema.cache_key(organization_cache_uuid.clone());
    let auth_mode = verify_endpoint_access(
        &database,
        &auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    reject_metadata_when_els_active(auth_mode).map_err(|e| error_handling(e, &mut span))?;

    let request = body.into_inner();

    let mut seen = HashSet::new();
    let mut unique_packages = Vec::new();
    for package in request.packages.iter() {
        let trimmed = package.trim();
        if trimmed.is_empty() {
            continue;
        }
        let trimmed_owned = trimmed.to_string();
        if seen.insert(trimmed_owned.clone()) {
            unique_packages.push(trimmed_owned);
        }
    }

    let frequency = request.frequency;

    if frequency.is_some() && !unique_packages.is_empty() {
        return Err(error_handling(EpError::request("Specify either `frequency` or `packages`, not both"), &mut span));
    }

    let packages = if unique_packages.is_empty() { None } else { Some(unique_packages) };

    if frequency.is_none() && packages.is_none() {
        return Err(error_handling(
            EpError::request("Provide at least one package or a collection frequency"),
            &mut span,
        ));
    }

    let response = engine_service
        .metadata(
            &database,
            &endpoint_schema,
            organization_cache_uuid,
            settings,
            packages,
            frequency,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(Response(response)).into();

    response
}

#[derive(Debug, PartialEq, SerdeSerialize, ToSchema)]
pub struct Response(serde_json::Value);

impl Response {
    #[allow(dead_code)]
    fn new(value: serde_json::Value) -> Self {
        Self(value)
    }
}

#[derive(Debug, SerdeDeserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct MetadataQuery {
    pub package: Option<String>,
}

#[derive(Debug, SerdeDeserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct CachedMetadataQuery {
    /// Collection frequency: high, medium, or low.
    pub frequency: String,
}

#[derive(Debug, SerdeSerialize, ToSchema, PartialEq, Eq)]
pub struct CollectorSummary {
    pub package: String,
    pub priority: String,
}

#[derive(Debug, SerdeSerialize, ToSchema, PartialEq, Eq)]
pub struct CollectorsResponse {
    pub collectors: Vec<CollectorSummary>,
}

#[derive(Debug, SerdeDeserialize, ToSchema)]
pub struct MetadataCollectRequest {
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub frequency: Option<SyncFrequency>,
}

#[derive(Debug, Clone, Default)]
pub struct MetadataCollector {}

impl MetadataCollector {
    pub async fn sync_endpoints(
        eden_node_uuid: EdenNodeUuid,
        ep_manager: Arc<MyEngineService>,
        db_manager: Arc<EdenDb>,
        metrics: web::Data<AllMetrics>,
    ) {
        let analytics_cfg = eden_config::analytics();
        let md_cfg = &analytics_cfg.metadata;
        let config = MetadataConfig {
            intervals: SchedulerIntervals {
                high: Duration::from_secs(md_cfg.high_interval_secs),
                medium: Duration::from_secs(md_cfg.medium_interval_secs),
                low: Duration::from_secs(md_cfg.low_interval_secs),
            },
            job_timeout: Duration::from_secs(md_cfg.job_timeout_secs),
            endpoint_timeout: Duration::from_secs(md_cfg.endpoint_timeout_secs),
            max_concurrent_endpoints: md_cfg.max_concurrent_endpoints,
            backoff: BackoffConfig {
                base: Duration::from_secs(md_cfg.backoff_base_secs),
                factor: md_cfg.backoff_factor,
                max: Duration::from_secs(md_cfg.backoff_max_secs),
            },
            redis_prefix: md_cfg.redis_prefix.clone(),
            collector_query_timeout: Duration::from_secs(md_cfg.collector_query_timeout_secs),
        };
        let publisher = default_publisher_with_prefix(db_manager, config.redis_prefix.clone());

        #[cfg(feature = "poll-clickhouse")]
        let (poll_tx, poll_rx) = tokio::sync::mpsc::channel::<PollMetricsRow>(512);

        #[cfg(feature = "poll-clickhouse")]
        {
            let db_mgr = publisher.db_manager().clone();
            tokio::spawn(run_poll_ingestion(db_mgr, poll_rx));
        }

        let scheduler = MetadataScheduler::new(
            eden_node_uuid,
            ep_manager,
            publisher,
            metrics.into_inner(),
            config,
            #[cfg(feature = "poll-clickhouse")]
            poll_tx,
        );

        scheduler.spawn_worker(SyncFrequency::High, scheduler.config.intervals.high);
        scheduler.spawn_worker(SyncFrequency::Medium, scheduler.config.intervals.medium);
        scheduler.spawn_worker(SyncFrequency::Low, scheduler.config.intervals.low);
    }
}

#[derive(Clone)]
struct MetadataScheduler {
    eden_node_uuid: Arc<EdenNodeUuid>,
    ep_manager: Arc<MyEngineService>,
    publisher: Arc<MetadataOutputs>,
    metrics: Arc<AllMetrics>,
    config: MetadataConfig,
    failures: Arc<Mutex<HashMap<(EndpointCacheUuid, SyncFrequency), FailureState>>>,
    #[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
    metadata_cache: Arc<Mutex<HashMap<EndpointCacheUuid, Box<dyn std::any::Any + Send + Sync>>>>,
    /// Per-endpoint locks to serialize cache writes across tiers, preventing
    /// cross-tier race conditions where High/Medium/Low workers could overwrite
    /// each other's updates.
    endpoint_cache_locks: Arc<Mutex<HashMap<EndpointCacheUuid, Arc<Mutex<()>>>>>,
    /// Per-tier semaphores to bound the number of endpoints processed concurrently
    /// per tick. Each tier gets its own concurrency budget so High-priority work
    /// is never starved by Medium/Low tasks.
    tier_semaphores: Arc<HashMap<SyncFrequency, Arc<Semaphore>>>,
    #[cfg(feature = "poll-clickhouse")]
    poll_tx: tokio::sync::mpsc::Sender<PollMetricsRow>,
}

#[derive(Debug, Clone)]
struct FailureState {
    attempts: u32,
    next_allowed: Instant,
}

impl FailureState {
    fn new(now: Instant, cfg: &BackoffConfig) -> Self {
        Self { attempts: 1, next_allowed: now + cfg.base }
    }

    fn update_next(&mut self, now: Instant, cfg: &BackoffConfig) {
        self.attempts = self.attempts.saturating_add(1);
        let delay = backoff_delay(self.attempts, cfg);
        self.next_allowed = now + delay;
    }
}

fn backoff_delay(attempts: u32, cfg: &BackoffConfig) -> Duration {
    let max_times = attempts.max(1) as usize;
    ExponentialBuilder::default()
        .with_min_delay(cfg.base)
        .with_factor(cfg.factor.max(1) as f32)
        .with_max_delay(cfg.max)
        .with_max_times(max_times)
        .build()
        .last()
        .unwrap_or(cfg.base)
}

impl MetadataScheduler {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    fn new(
        eden_node_uuid: EdenNodeUuid,
        ep_manager: Arc<MyEngineService>,
        publisher: Arc<MetadataOutputs>,
        metrics: Arc<AllMetrics>,
        config: MetadataConfig,
        #[cfg(feature = "poll-clickhouse")] poll_tx: tokio::sync::mpsc::Sender<PollMetricsRow>,
    ) -> Self {
        let max_concurrent = config.max_concurrent_endpoints;
        let mut tier_semaphores = HashMap::new();
        tier_semaphores.insert(SyncFrequency::High, Arc::new(Semaphore::new(max_concurrent)));
        tier_semaphores.insert(SyncFrequency::Medium, Arc::new(Semaphore::new(max_concurrent)));
        tier_semaphores.insert(SyncFrequency::Low, Arc::new(Semaphore::new(max_concurrent)));
        Self {
            eden_node_uuid: Arc::new(eden_node_uuid),
            ep_manager,
            publisher,
            metrics,
            config,
            failures: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
            metadata_cache: Arc::new(Mutex::new(HashMap::new())),
            endpoint_cache_locks: Arc::new(Mutex::new(HashMap::new())),
            tier_semaphores: Arc::new(tier_semaphores),
            #[cfg(feature = "poll-clickhouse")]
            poll_tx,
        }
    }

    /// Get or create a per-endpoint lock for serializing cache writes across tiers.
    async fn endpoint_lock(&self, endpoint: &EndpointCacheUuid) -> Arc<Mutex<()>> {
        let mut locks = self.endpoint_cache_locks.lock().await;
        locks.entry(endpoint.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
    }

    /// Return the semaphore for the given tier, falling back to a fresh
    /// single-permit semaphore if the tier is somehow missing (defensive).
    fn tier_semaphore(&self, frequency: SyncFrequency) -> Arc<Semaphore> {
        self.tier_semaphores.get(&frequency).cloned().unwrap_or_else(|| Arc::new(Semaphore::new(1)))
    }

    #[named]
    fn spawn_worker(&self, frequency: SyncFrequency, interval_duration: Duration) {
        let scheduler = self.clone();
        let ctx = ctx_with_trace!();

        tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                ticker.tick().await;
                if let Err(err) = scheduler.run_tick(frequency).await {
                    log_error!(
                        ctx.clone(),
                        "metadata scheduler tick failed",
                        audience = LogAudience::Internal,
                        frequency = frequency.as_str(),
                        error = format!("{err:?}")
                    );
                }
            }
        });
    }

    /// Processes all registered endpoints concurrently for a given frequency,
    /// bounded by the per-tier semaphore. Each endpoint is wrapped with
    /// `endpoint_timeout` to prevent a single endpoint from blocking the tick.
    #[named]
    async fn run_tick(&self, frequency: SyncFrequency) -> Result<(), SchedulerError> {
        let _ctx = ctx_with_trace!();
        let targets = self.collect_snapshot().await;

        if targets.is_empty() {
            log_trace!(
                _ctx,
                "metadata scheduler tick: no endpoints registered",
                audience = LogAudience::Internal,
                frequency = frequency.as_str()
            );
            return Ok(());
        }

        // Prune stale endpoint cache locks for endpoints no longer registered.
        self.prune_stale_entries(&targets).await;

        // Filter to only endpoints not in backoff.
        let mut eligible = Vec::new();
        for (kind, endpoint) in targets {
            if self.should_skip(&endpoint, frequency).await {
                log_debug!(
                    _ctx.clone(),
                    "metadata scheduler skipping endpoint due to backoff",
                    audience = LogAudience::Internal,
                    endpoint = endpoint.to_string(),
                    kind = kind.to_string(),
                    frequency = frequency.as_str()
                );
                continue;
            }

            eligible.push((kind, endpoint));
        }

        self.spawn_endpoint_tasks(eligible, frequency).await
    }

    /// Spawn concurrent endpoint processing tasks for a list of eligible
    /// endpoints.
    ///
    /// Each task:
    /// 1. Acquires the per-tier semaphore permit.
    /// 2. Acquires the per-endpoint lock (serializes across tiers, no timeout).
    /// 3. Wraps `process_endpoint` in `endpoint_timeout`.
    /// 4. Publishes a synthetic failure batch on timeout so consumers are notified.
    /// 5. Logs any task panics from the JoinSet.
    #[named]
    async fn spawn_endpoint_tasks(
        &self,
        targets: Vec<(EpKind, EndpointCacheUuid)>,
        frequency: SyncFrequency,
    ) -> Result<(), SchedulerError> {
        let ctx = ctx_with_trace!();
        let endpoint_timeout = self.config.endpoint_timeout;
        let semaphore = self.tier_semaphore(frequency);
        let mut join_set = JoinSet::new();

        for (kind, endpoint) in targets {
            let scheduler = self.clone();
            let ctx = ctx.clone();
            let semaphore = semaphore.clone();
            join_set.spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return, // Semaphore closed
                };

                // Acquire per-endpoint lock OUTSIDE the timeout so that waiting
                // behind another tier does not count against our wall-clock budget.
                let ep_lock = scheduler.endpoint_lock(&endpoint).await;
                let _ep_guard = ep_lock.lock().await;

                let mut telemetry_wrapper = TelemetryWrapper::new(
                    scheduler.metrics.clone(),
                    TelemetryLabels::new(&scheduler.eden_node_uuid),
                    TelemetryDurations::default(),
                );
                telemetry_wrapper.mut_labels(|labels| {
                    labels.set_endpoint_uuid(endpoint.uuid().into());
                    labels.set_endpoint_kind(kind);
                });

                let result = tokio::time::timeout(
                    endpoint_timeout,
                    scheduler.process_endpoint(kind, endpoint.clone(), frequency, &mut telemetry_wrapper),
                )
                .await;

                match result {
                    Ok(Ok(())) => {
                        scheduler.clear_failure(&endpoint, frequency).await;
                    }
                    Ok(Err(err)) => {
                        log_error!(
                            ctx,
                            "metadata scheduler failed to process endpoint",
                            audience = LogAudience::Internal,
                            endpoint = endpoint.to_string(),
                            kind = kind.to_string(),
                            frequency = frequency.as_str(),
                            error = format!("{err:?}")
                        );
                        #[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
                        if matches!(err, SchedulerError::ConnectionFailed(_)) {
                            scheduler.record_failure(&endpoint, frequency).await;
                        }
                    }
                    Err(_elapsed) => {
                        log_error!(
                            ctx.clone(),
                            "metadata scheduler endpoint timed out",
                            audience = LogAudience::Internal,
                            endpoint = endpoint.to_string(),
                            kind = kind.to_string(),
                            frequency = frequency.as_str(),
                            timeout_secs = endpoint_timeout.as_secs()
                        );
                        scheduler.record_failure(&endpoint, frequency).await;
                        // Publish a synthetic failure batch so consumers see the
                        // timeout rather than silently missing an update.
                        scheduler.publish_timeout_failure(&endpoint, frequency, endpoint_timeout, &ctx).await;
                    }
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(join_err) = result {
                log_error!(ctx.clone(), "metadata scheduler task panicked", error = format!("{join_err:?}"));
            }
        }

        Ok(())
    }

    /// Publish a synthetic timeout failure batch so downstream consumers are
    /// informed when an endpoint times out (mirroring the connection-failure path
    /// in `collect_for`). Uses `MetadataBatch<serde_json::Value>` so the
    /// serialized payload matches the standard batch schema.
    async fn publish_timeout_failure(
        &self,
        endpoint: &EndpointCacheUuid,
        frequency: SyncFrequency,
        timeout_duration: Duration,
        ctx: &eden_logger_internal::LogContext,
    ) {
        let error_msg = format!("endpoint timed out after {}s", timeout_duration.as_secs());
        let ep_error = EpError::Metadata(MetadataError::QueryTimeout(error_msg));
        let batch: MetadataBatch<serde_json::Value> =
            MetadataBatch::failure(frequency, serde_json::Value::Null, "timeout", JobErrorMode::Fatal, ep_error);
        if let Err(err) = self.publish_batch(endpoint, batch).await {
            log_error!(
                ctx.clone(),
                "failed to publish timeout failure batch",
                audience = LogAudience::Internal,
                endpoint = endpoint.to_string(),
                frequency = frequency.as_str(),
                error = format!("{err:?}")
            );
        }
    }

    /// Prune per-endpoint lock entries for endpoints no longer in the active target set.
    async fn prune_stale_entries(&self, targets: &[(EpKind, EndpointCacheUuid)]) {
        let active: HashSet<&EndpointCacheUuid> = targets.iter().map(|(_, ep)| ep).collect();

        let mut locks = self.endpoint_cache_locks.lock().await;
        locks.retain(|ep, _| active.contains(ep));
    }

    async fn collect_snapshot(&self) -> Vec<(EpKind, EndpointCacheUuid)> {
        let mut targets = Vec::new();
        let guard = self.ep_manager.router.read().await;

        for (kind, router) in guard.iter() {
            Self::append_endpoints(*kind, router.as_ref(), &mut targets);
        }

        targets
    }

    fn append_endpoints(kind: EpKind, _router: &dyn EpRouter, _out: &mut Vec<(EpKind, EndpointCacheUuid)>) {
        match kind {
            #[cfg(feature = "all-endpoints")]
            EpKind::Cassandra => {
                if let Some(router) = _router.as_any().downcast_ref::<CassandraEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Clickhouse => {
                if let Some(router) = _router.as_any().downcast_ref::<ClickhouseEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Http => {
                if let Some(router) = _router.as_any().downcast_ref::<HttpEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }

            #[cfg(feature = "all-endpoints")]
            EpKind::Llm => {
                if let Some(router) = _router.as_any().downcast_ref::<LlmEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "mongo")]
            EpKind::Mongo => {
                if let Some(router) = _router.as_any().downcast_ref::<MongoEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Mssql => {
                if let Some(router) = _router.as_any().downcast_ref::<MssqlEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Mysql => {
                if let Some(router) = _router.as_any().downcast_ref::<MysqlEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Oracle => {
                if let Some(router) = _router.as_any().downcast_ref::<OracleEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Pinecone => {
                if let Some(router) = _router.as_any().downcast_ref::<PineconeEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "postgres")]
            EpKind::Postgres => {
                if let Some(router) = _router.as_any().downcast_ref::<PostgresEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            #[cfg(feature = "redis")]
            EpKind::Redis => {
                if let Some(router) = _router.as_any().downcast_ref::<RedisEp>() {
                    _out.extend(router.pool().endpoints().into_iter().map(|endpoint| (kind, endpoint)));
                }
            }
            _ => {}
        }
    }

    async fn process_endpoint(
        &self,
        kind: EpKind,
        _endpoint: EndpointCacheUuid,
        _frequency: SyncFrequency,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), SchedulerError> {
        match kind {
            #[cfg(feature = "all-endpoints")]
            EpKind::Cassandra => {
                self.collect_for::<CassandraEp, CassandraMetadata, CassandraAsync>(kind, _endpoint, _frequency, _telemetry_wrapper)
                    .await
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Clickhouse => {
                self.collect_for::<ClickhouseEp, ClickhouseMetadata, ClickhouseAsync>(kind, _endpoint, _frequency, _telemetry_wrapper)
                    .await
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Http => self.collect_for::<HttpEp, HttpMetadata, HttpAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await,

            #[cfg(feature = "all-endpoints")]
            EpKind::Llm => self.collect_for::<LlmEp, LlmMetadata, LlmAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await,
            #[cfg(feature = "mongo")]
            EpKind::Mongo => self.collect_for::<MongoEp, MongoMetadata, MongoAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await,
            #[cfg(feature = "all-endpoints")]
            EpKind::Mssql => self.collect_for::<MssqlEp, MssqlMetadata, MssqlAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await,
            #[cfg(feature = "all-endpoints")]
            EpKind::Mysql => self.collect_for::<MysqlEp, MysqlMetadata, MysqlAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await,
            #[cfg(feature = "all-endpoints")]
            EpKind::Oracle => {
                self.collect_for::<OracleEp, OracleMetadata, OracleAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Pinecone => {
                self.collect_for::<PineconeEp, PineconeMetadata, PineconeAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await
            }
            #[cfg(feature = "postgres")]
            EpKind::Postgres => {
                self.collect_for::<PostgresEp, PostgresMetadata, PostgresAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await
            }
            #[cfg(feature = "redis")]
            EpKind::Redis => self.collect_for::<RedisEp, RedisMetadata, RedisAsync>(kind, _endpoint, _frequency, _telemetry_wrapper).await,
            _ => Err(SchedulerError::UnsupportedKind(kind)),
        }
    }

    #[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
    #[named]
    async fn collect_for<E, M, C>(
        &self,
        kind: EpKind,
        endpoint: EndpointCacheUuid,
        frequency: SyncFrequency,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), SchedulerError>
    where
        E: GetPool<C> + 'static,
        C: Clone + Send + Sync + 'static,
        M: SyncMetadata<C> + EpMetadata + Clone + Default + SerdeSerialize + 'static,
    {
        let _ctx = ctx_with_trace!();

        if let Some(org_cache_uuid) = endpoint.org() {
            telemetry_wrapper.set_org_uuid(OrganizationUuid::from(org_cache_uuid.uuid()));
        }

        let connection = match self.fetch_connection::<E, C>(kind, &endpoint).await {
            Ok(conn) => conn,
            Err(err) => {
                let ep_error = err.to_ep_error();
                let batch = MetadataBatch::failure(frequency, M::default(), "connection", JobErrorMode::Fatal, ep_error.clone());
                #[cfg(feature = "poll-clickhouse")]
                self.send_poll_row(kind, frequency, &endpoint, &batch);
                self.publish_batch(&endpoint, batch).await.map_err(SchedulerError::PublishFailed)?;
                return Err(SchedulerError::ConnectionFailed(err));
            }
        };

        // NOTE: The per-endpoint lock is acquired by the caller (spawn_endpoint_tasks)
        // BEFORE invoking this method, so cache reads/writes here are already serialized
        // across tiers.

        // Get or create cached metadata to preserve state between syncs
        // This is important for tracking values like prev_total_commands in Redis
        let mut metadata = {
            let cache = self.metadata_cache.lock().await;
            if let Some(cached) = cache.get(&endpoint) {
                // Try to downcast to the expected type
                if let Some(m) = cached.downcast_ref::<M>() {
                    m.clone()
                } else {
                    // Type mismatch (shouldn't happen), create new
                    M::default()
                }
            } else {
                M::default()
            }
        };

        let jobs = metadata.jobs(frequency);
        if jobs.is_empty() {
            log_debug!(
                _ctx.clone(),
                "metadata scheduler found no jobs; skipping",
                audience = LogAudience::Internal,
                endpoint = endpoint.to_string(),
                kind = kind.to_string(),
                frequency = frequency.as_str()
            );
            return Ok(());
        }

        let capabilities = M::discover_capabilities(connection.clone(), telemetry_wrapper).await;
        let batch = run_metadata_jobs_with_capabilities(
            metadata,
            connection,
            jobs,
            telemetry_wrapper,
            frequency,
            self.config.job_timeout,
            &*capabilities,
        )
        .await;

        #[cfg(feature = "poll-clickhouse")]
        self.send_poll_row(kind, frequency, &endpoint, &batch);

        {
            let mut cache = self.metadata_cache.lock().await;
            cache.insert(endpoint.clone(), Box::new(batch.data.clone()));
        }

        self.publish_batch(&endpoint, batch).await.map_err(SchedulerError::PublishFailed)?;

        Ok(())
    }
    #[cfg(feature = "poll-clickhouse")]
    fn send_poll_row<M>(&self, kind: EpKind, frequency: SyncFrequency, endpoint: &EndpointCacheUuid, batch: &MetadataBatch<M>)
    where
        M: SerdeSerialize + 'static,
    {
        let endpoint_uuid = endpoint.eden_uuid::<EndpointUuid>().to_string();
        let organization_uuid = endpoint.org().map(|org| org.uuid().to_string()).unwrap_or_default();
        let freq_str = frequency.as_str().to_string();
        let collection_ms = batch.finished_at.signed_duration_since(batch.started_at).num_milliseconds().max(0) as u32;

        let row = match kind {
            #[cfg(feature = "redis")]
            EpKind::Redis => {
                let metadata = match (&batch.data as &dyn std::any::Any).downcast_ref::<RedisMetadata>() {
                    Some(m) => m,
                    None => return,
                };
                Some(PollMetricsRow::Redis(build_redis_poll_row(
                    metadata,
                    batch.finished_at,
                    organization_uuid,
                    endpoint_uuid,
                    freq_str,
                    collection_ms,
                    batch.had_fatal,
                )))
            }
            #[cfg(feature = "postgres")]
            EpKind::Postgres => {
                let metadata = match (&batch.data as &dyn std::any::Any).downcast_ref::<PostgresMetadata>() {
                    Some(m) => m,
                    None => return,
                };
                Some(PollMetricsRow::Postgres(build_postgres_poll_row(
                    metadata,
                    batch.finished_at,
                    organization_uuid,
                    endpoint_uuid,
                    freq_str,
                    collection_ms,
                    batch.had_fatal,
                )))
            }
            #[cfg(feature = "mongo")]
            EpKind::Mongo => {
                let metadata = match (&batch.data as &dyn std::any::Any).downcast_ref::<MongoMetadata>() {
                    Some(m) => m,
                    None => return,
                };
                Some(PollMetricsRow::Mongo(build_mongo_poll_row(
                    metadata,
                    batch.finished_at,
                    organization_uuid,
                    endpoint_uuid,
                    freq_str,
                    collection_ms,
                    batch.had_fatal,
                )))
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Oracle => {
                let metadata = match (&batch.data as &dyn std::any::Any).downcast_ref::<OracleMetadata>() {
                    Some(m) => m,
                    None => return,
                };
                Some(PollMetricsRow::Oracle(build_oracle_poll_row(
                    metadata,
                    batch.finished_at,
                    organization_uuid,
                    endpoint_uuid,
                    freq_str,
                    collection_ms,
                    batch.had_fatal,
                )))
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Cassandra => {
                let metadata = match (&batch.data as &dyn std::any::Any).downcast_ref::<CassandraMetadata>() {
                    Some(m) => m,
                    None => return,
                };
                Some(PollMetricsRow::Cassandra(build_cassandra_poll_row(
                    metadata,
                    batch.finished_at,
                    organization_uuid,
                    endpoint_uuid,
                    freq_str,
                    collection_ms,
                    batch.had_fatal,
                )))
            }
            #[cfg(feature = "all-endpoints")]
            EpKind::Clickhouse => {
                let metadata = match (&batch.data as &dyn std::any::Any).downcast_ref::<ClickhouseMetadata>() {
                    Some(m) => m,
                    None => return,
                };
                Some(PollMetricsRow::Clickhouse(build_clickhouse_poll_row(
                    metadata,
                    batch.finished_at,
                    organization_uuid,
                    endpoint_uuid,
                    freq_str,
                    collection_ms,
                    batch.had_fatal,
                )))
            }
            _ => None,
        };

        if let Some(row) = row
            && self.poll_tx.try_send(row).is_err()
        {
            tracing::warn!("poll metrics channel full; dropping row");
        }
    }

    #[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
    async fn fetch_connection<E, C>(&self, kind: EpKind, endpoint: &EndpointCacheUuid) -> Result<C, ConnectionError>
    where
        E: GetPool<C> + 'static,
        C: Clone + Send + Sync + 'static,
    {
        let guard = self.ep_manager.router.read().await;
        let Some(route) = guard.get(&kind) else {
            return Err(ConnectionError::RouterMissing(kind));
        };

        let Some(router) = route.as_any().downcast_ref::<E>() else {
            return Err(ConnectionError::RouterDowncast { kind, type_name: std::any::type_name::<E>().to_string() });
        };

        let Some(context) = router.pool().pool().get(endpoint) else {
            return Err(ConnectionError::EndpointMissing { kind, endpoint: endpoint.clone() });
        };

        context
            .conn()
            .system_conn()
            .cloned()
            .map_err(|error| ConnectionError::ConnUnavailable { kind, endpoint: endpoint.clone(), error })
    }

    async fn publish_batch<M>(&self, endpoint: &EndpointCacheUuid, batch: MetadataBatch<M>) -> Result<(), PublishError>
    where
        M: SerdeSerialize,
    {
        let frequency = batch.frequency;
        let payload =
            serde_json::to_string(&batch).map_err(|error| PublishError::Serialize { endpoint: endpoint.clone(), frequency, error })?;

        self.publisher.publish(endpoint, frequency, payload).await.map_err(|error| PublishError::Publish {
            endpoint: endpoint.clone(),
            frequency,
            error,
        })
    }

    async fn should_skip(&self, endpoint: &EndpointCacheUuid, frequency: SyncFrequency) -> bool {
        let now = Instant::now();
        let failures = self.failures.lock().await;
        failures.get(&(endpoint.clone(), frequency)).map(|state| state.next_allowed > now).unwrap_or(false)
    }

    async fn record_failure(&self, endpoint: &EndpointCacheUuid, frequency: SyncFrequency) {
        let now = Instant::now();
        let mut failures = self.failures.lock().await;
        let key = (endpoint.clone(), frequency);
        match failures.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().update_next(now, &self.config.backoff);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                // First failure: use base backoff, don't double-increment.
                entry.insert(FailureState::new(now, &self.config.backoff));
            }
        }
    }

    async fn clear_failure(&self, endpoint: &EndpointCacheUuid, frequency: SyncFrequency) {
        let mut failures = self.failures.lock().await;
        failures.remove(&(endpoint.clone(), frequency));
    }
}

#[cfg(all(feature = "poll-clickhouse", feature = "redis"))]
fn build_redis_poll_row(
    m: &RedisMetadata,
    snapshot_time: chrono::DateTime<chrono::Utc>,
    organization_uuid: String,
    endpoint_uuid: String,
    frequency: String,
    collection_ms: u32,
    had_fatal: bool,
) -> RedisPollMetricsRow {
    let mut row = RedisPollMetricsRow::common(snapshot_time, organization_uuid, endpoint_uuid, frequency, collection_ms, had_fatal);

    // Memory
    row.used_memory = Some(m.memory_info.used_memory);
    row.used_memory_rss = Some(m.memory_info.used_memory_rss);
    row.used_memory_peak = Some(m.memory_info.used_memory_peak);
    row.used_memory_overhead = Some(m.memory_info.used_memory_overhead);
    row.used_memory_startup = Some(m.memory_info.used_memory_startup);
    row.used_memory_dataset = Some(m.memory_info.used_memory_dataset);
    row.total_system_memory = Some(m.memory_info.total_system_memory);
    row.maxmemory = Some(m.memory_info.maxmemory);
    row.maxmemory_policy = Some(m.memory_info.maxmemory_policy.clone());
    row.mem_fragmentation_ratio = Some(m.memory_info.mem_fragmentation_ratio);
    row.mem_fragmentation_bytes = Some(m.memory_info.mem_fragmentation_bytes);
    row.allocator_frag_ratio = Some(m.memory_info.allocator_frag_ratio);
    row.allocator_frag_bytes = Some(m.memory_info.allocator_frag_bytes);
    row.allocator_rss_ratio = Some(m.memory_info.allocator_rss_ratio);
    row.allocator_allocated = Some(m.memory_info.allocator_allocated);
    row.allocator_active = Some(m.memory_info.allocator_active);
    row.allocator_resident = Some(m.memory_info.allocator_resident);
    row.used_memory_lua = Some(m.memory_info.used_memory_lua);
    row.used_memory_scripts = Some(m.memory_info.used_memory_scripts);
    row.used_memory_vm_total = Some(m.memory_info.used_memory_vm_total);
    row.mem_clients_normal = Some(m.memory_info.mem_clients_normal);
    row.mem_clients_slaves = Some(m.memory_info.mem_clients_slaves);
    row.mem_aof_buffer = Some(m.memory_info.mem_aof_buffer);
    row.mem_replication_backlog = Some(m.memory_info.mem_replication_backlog);
    row.active_defrag_running = Some(u8::from(m.memory_info.active_defrag_running));
    row.lazyfree_pending_objects = Some(m.memory_info.lazyfree_pending_objects);

    // CPU
    row.used_cpu_sys = Some(m.cpu_info.used_cpu_sys);
    row.used_cpu_user = Some(m.cpu_info.used_cpu_user);
    row.used_cpu_sys_children = Some(m.cpu_info.used_cpu_sys_children);
    row.used_cpu_user_children = Some(m.cpu_info.used_cpu_user_children);
    row.used_cpu_sys_main_thread = Some(m.cpu_info.used_cpu_sys_main_thread);
    row.used_cpu_user_main_thread = Some(m.cpu_info.used_cpu_user_main_thread);

    // Clients
    row.connected_clients = Some(m.client_info.connected_clients);
    row.blocked_clients = Some(m.client_info.blocked_clients);
    row.maxclients = Some(m.client_info.maxclients);
    row.cluster_connections = Some(m.client_info.cluster_connections);
    row.tracking_clients = Some(m.client_info.tracking_clients);
    row.pubsub_clients = Some(m.client_info.pubsub_clients);
    row.watching_clients = Some(m.client_info.watching_clients);
    row.clients_in_timeout_table = Some(m.client_info.clients_in_timeout_table);
    row.client_recent_max_input_buffer = Some(m.client_info.client_recent_max_input_buffer);
    row.client_recent_max_output_buffer = Some(m.client_info.client_recent_max_output_buffer);
    row.total_watched_keys = Some(m.client_info.total_watched_keys);
    row.total_blocking_keys = Some(m.client_info.total_blocking_keys);

    // Replication
    let role_str = if m.replication_info.is_master() { "master" } else { "slave" };
    row.replication_role = Some(role_str.to_string());
    row.connected_slaves = Some(m.replication_info.connected_slaves);
    row.master_repl_offset = m.replication_info.master_repl_offset;
    row.repl_backlog_active = m.replication_info.repl_backlog_active.map(u8::from);
    row.repl_backlog_size = m.replication_info.repl_backlog_size;
    row.repl_backlog_histlen = m.replication_info.repl_backlog_histlen;
    row.master_link_status = m.replication_info.master_link_status.clone();
    row.master_sync_in_progress = m.replication_info.master_sync_in_progress.map(u8::from);
    row.slave_repl_offset = m.replication_info.slave_repl_offset;
    row.master_link_down_since_seconds = m.replication_info.master_link_down_since_seconds;

    // Database (derived)
    let total_keys: u64 = m.database_stats.iter().map(|db| db.keys).sum();
    let total_expires: u64 = m.database_stats.iter().map(|db| db.expires).sum();
    row.total_keys = Some(total_keys);
    row.total_expires = Some(total_expires);
    row.database_count = Some(m.database_stats.len() as u32);

    // JSON blobs (high)
    row.client_details_json = serde_json::to_string(&m.client_info.client_details).unwrap_or_else(|_| "[]".to_string());
    row.slave_replicas_json = serde_json::to_string(&m.replication_info.slave_replicas).unwrap_or_else(|_| "[]".to_string());
    row.database_stats_json = serde_json::to_string(&m.database_stats).unwrap_or_else(|_| "[]".to_string());

    // Cluster (medium)
    if let Some(ref cluster) = m.cluster_info {
        row.cluster_enabled = Some(u8::from(cluster.cluster_enabled));
        row.cluster_state = Some(cluster.cluster_state.clone());
        row.cluster_known_nodes = Some(cluster.cluster_known_nodes);
        row.cluster_size = Some(cluster.cluster_size);
        row.cluster_info_json = serde_json::to_string(cluster).unwrap_or_else(|_| "{}".to_string());
    }

    // Persistence (medium)
    row.rdb_last_save_time = Some(m.persistence_info.rdb_last_save_time);
    row.rdb_changes_since_last_save = Some(m.persistence_info.rdb_changes_since_last_save);
    row.aof_enabled = Some(u8::from(m.persistence_info.aof_enabled));
    row.aof_rewrite_in_progress = Some(u8::from(m.persistence_info.aof_rewrite_in_progress));
    row.persistence_info_json = serde_json::to_string(&m.persistence_info).unwrap_or_else(|_| "{}".to_string());

    // Modules (medium)
    row.modules_info_json = serde_json::to_string(&m.modules_info).unwrap_or_else(|_| "{}".to_string());

    // Server (low)
    row.redis_version = Some(m.server_info.redis_version.clone());
    row.redis_mode = Some(format!("{:?}", m.server_info.redis_mode));
    row.os = Some(m.server_info.os.clone());
    row.uptime_in_seconds = Some(m.server_info.uptime_in_seconds);
    row.hz = Some(m.server_info.hz);

    // Config (low)
    row.config_json = serde_json::to_string(&m.configuration).unwrap_or_else(|_| "{}".to_string());

    // Security (low)
    row.security_info_json = serde_json::to_string(&m.security_info).unwrap_or_else(|_| "{}".to_string());

    row
}

#[cfg(all(feature = "poll-clickhouse", feature = "postgres"))]
fn build_postgres_poll_row(
    m: &PostgresMetadata,
    snapshot_time: chrono::DateTime<chrono::Utc>,
    organization_uuid: String,
    endpoint_uuid: String,
    frequency: String,
    collection_ms: u32,
    had_fatal: bool,
) -> PostgresPollMetricsRow {
    let mut row = PostgresPollMetricsRow::common(snapshot_time, organization_uuid, endpoint_uuid, frequency, collection_ms, had_fatal);

    // Activity (high)
    row.active_connections = Some(m.activity_info.active_connections);
    row.idle_connections = Some(m.activity_info.idle_connections);
    row.idle_in_transaction = Some(m.activity_info.idle_in_transaction);
    row.total_connections = Some(m.activity_info.total_connections);
    row.max_connections = Some(m.activity_info.max_connections);
    row.connection_utilization_pct = Some(m.activity_info.connection_utilization_pct);
    row.waiting_queries_count = Some(m.activity_info.waiting_queries_count);
    row.blocking_queries_count = Some(m.activity_info.blocking_queries_count);

    // Locks (high)
    row.total_locks = Some(m.lock_info.total_locks);
    row.granted_locks = Some(m.lock_info.granted_locks);
    row.waiting_locks = Some(m.lock_info.waiting_locks);
    row.deadlock_count = Some(m.lock_info.deadlock_count);
    row.max_lock_wait_time = Some(m.lock_info.max_lock_wait_time);

    // Performance (high)
    row.buffer_cache_hit_ratio = Some(m.performance_stats.buffer_cache_hit_ratio);
    row.index_hit_ratio = Some(m.performance_stats.index_hit_ratio);
    row.total_operations = Some(m.performance_stats.total_operations);
    row.total_transactions = Some(m.performance_stats.total_transactions);
    row.total_blocks_read = Some(m.performance_stats.total_blocks_read);
    row.total_blocks_hit = Some(m.performance_stats.total_blocks_hit);
    row.total_temp_files = Some(m.performance_stats.total_temp_files);
    row.total_temp_bytes = Some(m.performance_stats.total_temp_bytes);

    // Replication (high)
    row.is_primary = Some(u8::from(m.replication_info.is_primary));
    row.is_in_recovery = Some(u8::from(m.replication_info.is_in_recovery));
    row.active_replicas = Some(m.replication_info.active_replicas);
    row.max_replica_lag_seconds = Some(m.replication_info.max_replica_lag_seconds);
    row.synchronous_replicas = Some(m.replication_info.synchronous_replicas);

    // Transactions (high)
    row.xact_committed = Some(m.transaction_info.transactions_committed);
    row.xact_rolled_back = Some(m.transaction_info.transactions_rolled_back);
    row.commit_ratio = Some(m.transaction_info.commit_ratio);
    row.deadlocks_total = Some(m.transaction_info.deadlocks_total);

    // WAL (high)
    row.wal_bytes = Some(m.wal_info.wal_bytes);
    row.wal_records = Some(m.wal_info.wal_records);
    row.wal_fpi = Some(m.wal_info.wal_fpi);

    // JSON blobs (high)
    row.activity_info_json = serde_json::to_string(&m.activity_info).unwrap_or_else(|_| "{}".to_string());
    row.lock_info_json = serde_json::to_string(&m.lock_info).unwrap_or_else(|_| "{}".to_string());

    // BGWriter (medium)
    row.buffers_checkpoint = Some(m.bgwriter_info.buffers_checkpoint);
    row.buffers_clean = Some(m.bgwriter_info.buffers_clean);
    row.buffers_backend = Some(m.bgwriter_info.buffers_backend);

    // JSON (medium)
    row.database_stats_json = serde_json::to_string(&m.database_stats).unwrap_or_else(|_| "[]".to_string());
    row.table_info_json = serde_json::to_string(&m.table_info).unwrap_or_else(|_| "{}".to_string());
    row.index_info_json = serde_json::to_string(&m.index_info).unwrap_or_else(|_| "{}".to_string());
    row.vacuum_info_json = serde_json::to_string(&m.vacuum_info).unwrap_or_else(|_| "{}".to_string());

    // JSON (low)
    row.extensions_json = serde_json::to_string(&m.extension_info).unwrap_or_else(|_| "[]".to_string());
    row.settings_json = serde_json::to_string(&m.settings_info).unwrap_or_else(|_| "{}".to_string());

    row
}

#[cfg(all(feature = "poll-clickhouse", feature = "mongo"))]
fn build_mongo_poll_row(
    m: &MongoMetadata,
    snapshot_time: chrono::DateTime<chrono::Utc>,
    organization_uuid: String,
    endpoint_uuid: String,
    frequency: String,
    collection_ms: u32,
    had_fatal: bool,
) -> MongoPollMetricsRow {
    let mut row = MongoPollMetricsRow::common(snapshot_time, organization_uuid, endpoint_uuid, frequency, collection_ms, had_fatal);

    // Connections (high)
    if let Some(ref c) = m.connection_info {
        row.current_connections = Some(c.total_active_connections);
        row.available_connections = Some(c.available_connections);
        row.total_created = Some(c.total_connections_created);
    }

    // Locks (high)
    if let Some(ref l) = m.lock_info {
        row.current_queue_total = Some(l.operations_waiting);
        row.current_queue_readers = Some(l.global_read_locks);
        row.current_queue_writers = Some(l.global_write_locks);
        row.deadlocks_detected = Some(l.deadlocks_detected);
        row.lock_contention_ratio = Some(l.lock_contention_ratio);
    }

    // Network (high)
    if let Some(ref n) = m.network_info {
        row.bytes_in = Some(n.total_bytes_received);
        row.bytes_out = Some(n.total_bytes_sent);
        row.num_requests = Some(n.requests_per_second);
    }

    // Performance (high)
    if let Some(ref p) = m.performance_stats {
        row.overall_performance_score = Some(p.overall_performance_score);
    }

    // WiredTiger (high)
    if let Some(ref w) = m.wiredtiger_info {
        row.cache_bytes_currently_in_cache = Some(w.cache_used_bytes);
        row.cache_maximum_bytes_configured = Some(w.cache_size_bytes);
        row.cache_evictions = Some(w.cache_evictions);
        row.pages_read_into_cache = Some(w.cache_pages_read);
        row.pages_written_from_cache = Some(w.cache_pages_written);
        row.cache_hit_ratio = Some(w.cache_hit_ratio_percentage);
    }

    // Replication (high)
    if let Some(ref r) = m.replication_info {
        row.is_primary = r.primary_info.as_ref().map(|_| 1_u8);
        row.replication_lag_ms = Some(r.max_replication_lag_ms);
        row.member_count = Some(r.total_members);
    }

    // Transactions (high)
    if let Some(ref t) = m.transaction_info {
        row.total_started = Some(t.total_transactions);
        row.total_committed = Some(t.committed_transactions);
        row.total_aborted = Some(t.aborted_transactions);
    }

    // JSON blobs (high)
    if let Some(ref s) = m.server_info {
        row.server_info_json = serde_json::to_string(s).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref r) = m.replication_info {
        row.replication_info_json = serde_json::to_string(r).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref w) = m.wiredtiger_info {
        row.wiredtiger_info_json = serde_json::to_string(w).unwrap_or_else(|_| "{}".to_string());
    }

    // Oplog (medium)
    if let Some(ref o) = m.oplog_info {
        row.oplog_size_mb = Some(o.oplog_size_bytes as f64 / (1024.0 * 1024.0));
        row.oplog_used_mb = Some(o.oplog_used_bytes as f64 / (1024.0 * 1024.0));
    }

    // JSON (medium)
    if let Some(ref a) = m.aggregation_stats {
        row.aggregation_stats_json = serde_json::to_string(a).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref c) = m.collection_info {
        row.collection_info_json = serde_json::to_string(c).unwrap_or_else(|_| "[]".to_string());
    }
    if let Some(ref d) = m.database_stats {
        row.database_stats_json = serde_json::to_string(d).unwrap_or_else(|_| "[]".to_string());
    }
    if let Some(ref i) = m.index_info {
        row.index_info_json = serde_json::to_string(i).unwrap_or_else(|_| "[]".to_string());
    }
    if let Some(ref p) = m.profiler_info {
        row.profiler_info_json = serde_json::to_string(p).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref s) = m.sharding_info {
        row.sharding_info_json = serde_json::to_string(s).unwrap_or_else(|_| "{}".to_string());
    }

    // JSON (low)
    if let Some(ref b) = m.balancer_info {
        row.balancer_info_json = serde_json::to_string(b).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref mem) = m.memory_info {
        row.memory_info_json = serde_json::to_string(mem).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref sec) = m.security_info {
        row.security_info_json = serde_json::to_string(sec).unwrap_or_else(|_| "{}".to_string());
    }
    if let Some(ref u) = m.user_info {
        row.user_info_json = serde_json::to_string(u).unwrap_or_else(|_| "[]".to_string());
    }

    row
}

#[cfg(all(feature = "poll-clickhouse", feature = "all-endpoints"))]
fn build_oracle_poll_row(
    m: &OracleMetadata,
    snapshot_time: chrono::DateTime<chrono::Utc>,
    organization_uuid: String,
    endpoint_uuid: String,
    frequency: String,
    collection_ms: u32,
    had_fatal: bool,
) -> OraclePollMetricsRow {
    let mut row = OraclePollMetricsRow::common(snapshot_time, organization_uuid, endpoint_uuid, frequency, collection_ms, had_fatal);

    // Activity (high)
    row.active_sessions = Some(m.activity_info.active_sessions);
    row.total_sessions = Some(m.activity_info.total_sessions);
    row.max_sessions = Some(m.activity_info.max_sessions);
    row.session_utilization_pct = Some(m.activity_info.session_utilization_pct);
    row.waiting_sessions_count = Some(m.activity_info.waiting_sessions_count);
    row.blocking_sessions_count = Some(m.activity_info.blocking_sessions_count);
    row.sga_size = Some(m.activity_info.sga_size);
    row.current_pga_used = Some(m.activity_info.current_pga_used);

    // Connection (high)
    row.current_processes = Some(m.connection_info.current_processes);
    row.max_processes = Some(m.connection_info.max_processes);
    row.process_utilization_pct = Some(m.connection_info.process_utilization_pct);

    // Locks (high)
    row.total_active_locks = Some(m.lock_info.total_active_locks);
    row.blocking_locks = Some(m.lock_info.blocking_locks);
    row.blocked_sessions = Some(m.lock_info.blocked_sessions);
    row.total_deadlocks = Some(m.lock_info.total_deadlocks);
    row.max_lock_wait_time = Some(m.lock_info.max_lock_wait_time);

    // Performance (high)
    row.health_score = Some(m.performance_stats.health_score);

    // Transactions (high)
    row.active_transactions = Some(m.transaction_info.active_transactions);
    row.user_commits = Some(m.transaction_info.user_commits);
    row.user_rollbacks = Some(m.transaction_info.user_rollbacks);
    row.rollback_ratio = Some(m.transaction_info.rollback_ratio);
    row.transaction_health_score = Some(m.transaction_info.transaction_health_score);

    // Wait events (high)
    row.cpu_time_percent = Some(m.wait_events.cpu_time_percent);
    row.wait_time_percent = Some(m.wait_events.wait_time_percent);
    row.wait_health_score = Some(m.wait_events.wait_health_score);

    // JSON blobs (high)
    row.activity_info_json = serde_json::to_string(&m.activity_info).unwrap_or_else(|_| "{}".to_string());
    row.connection_info_json = serde_json::to_string(&m.connection_info).unwrap_or_else(|_| "{}".to_string());
    row.lock_info_json = serde_json::to_string(&m.lock_info).unwrap_or_else(|_| "{}".to_string());
    row.performance_stats_json = serde_json::to_string(&m.performance_stats).unwrap_or_else(|_| "{}".to_string());
    row.session_info_json = serde_json::to_string(&m.session_info).unwrap_or_else(|_| "{}".to_string());
    row.transaction_info_json = serde_json::to_string(&m.transaction_info).unwrap_or_else(|_| "{}".to_string());
    row.wait_events_json = serde_json::to_string(&m.wait_events).unwrap_or_else(|_| "{}".to_string());

    // Database stats scalars (medium, first element)
    if let Some(db) = m.database_stats.first() {
        row.buffer_cache_hit_ratio = Some(db.buffer_cache_hit_ratio);
        row.transactions_per_sec = Some(db.transactions_per_sec);
        row.physical_reads_per_sec = Some(db.physical_reads_per_sec);
        row.database_size = Some(db.database_size);
        row.used_space = Some(db.used_space);
        row.uptime_seconds = Some(db.uptime_seconds);
    }

    // JSON blobs (medium)
    row.database_stats_json = serde_json::to_string(&m.database_stats).unwrap_or_else(|_| "[]".to_string());
    row.index_info_json = serde_json::to_string(&m.index_info).unwrap_or_else(|_| "[]".to_string());
    row.redolog_info_json = serde_json::to_string(&m.redolog_info).unwrap_or_else(|_| "{}".to_string());
    row.segment_info_json = serde_json::to_string(&m.segment_info).unwrap_or_else(|_| "[]".to_string());
    row.storage_info_json = serde_json::to_string(&m.storage_info).unwrap_or_else(|_| "{}".to_string());
    row.table_info_json = serde_json::to_string(&m.table_info).unwrap_or_else(|_| "[]".to_string());
    row.tablespace_info_json = serde_json::to_string(&m.tablespace_info).unwrap_or_else(|_| "[]".to_string());

    // JSON blobs (low)
    row.parameter_info_json = serde_json::to_string(&m.parameter_info).unwrap_or_else(|_| "{}".to_string());

    row
}

#[cfg(all(feature = "poll-clickhouse", feature = "all-endpoints"))]
fn build_cassandra_poll_row(
    m: &CassandraMetadata,
    snapshot_time: chrono::DateTime<chrono::Utc>,
    organization_uuid: String,
    endpoint_uuid: String,
    frequency: String,
    collection_ms: u32,
    had_fatal: bool,
) -> CassandraPollMetricsRow {
    let mut row = CassandraPollMetricsRow::common(snapshot_time, organization_uuid, endpoint_uuid, frequency, collection_ms, had_fatal);

    // Cluster (high)
    row.total_nodes = Some(m.cluster_info.total_nodes);
    row.up_nodes = Some(m.cluster_info.up_nodes);
    row.down_nodes = Some(m.cluster_info.down_nodes);
    row.cluster_health_pct = Some(m.cluster_info.cluster_health_pct);
    row.schema_agreement = Some(u8::from(m.cluster_info.schema_agreement));
    row.total_client_connections = Some(m.cluster_info.total_client_connections);
    row.pending_compactions = Some(m.cluster_info.pending_compactions);
    row.active_repairs = Some(m.cluster_info.active_repairs);

    // Node resource/performance metrics (high, first node)
    if let Some(node) = m.node_info.first() {
        row.heap_memory_used_mb = Some(node.resource_metrics.heap_memory_used_mb);
        row.heap_memory_max_mb = Some(node.resource_metrics.heap_memory_max_mb);
        row.heap_memory_utilization_pct = Some(node.resource_metrics.heap_memory_utilization_pct);
        row.cpu_utilization_pct = Some(node.resource_metrics.cpu_utilization_pct);
        row.disk_used_gb = Some(node.resource_metrics.disk_used_gb);
        row.disk_utilization_pct = Some(node.resource_metrics.disk_utilization_pct);
        row.read_requests_per_sec = Some(node.performance_metrics.read_requests_per_sec);
        row.write_requests_per_sec = Some(node.performance_metrics.write_requests_per_sec);
        row.avg_read_latency_ms = Some(node.performance_metrics.avg_read_latency_ms);
        row.avg_write_latency_ms = Some(node.performance_metrics.avg_write_latency_ms);
        row.cache_hit_ratio_pct = Some(node.performance_metrics.cache_hit_ratio_pct);
    }

    // Thread pools (high)
    row.threadpool_active_threads = Some(m.threadpool_info.total_active_threads);
    row.threadpool_pending_tasks = Some(m.threadpool_info.total_pending_tasks);
    row.threadpool_dropped_tasks = Some(m.threadpool_info.total_dropped_tasks);
    row.threadpool_health_score = Some(m.threadpool_info.overall_health_score);

    // JSON blobs (high)
    row.cluster_info_json = serde_json::to_string(&m.cluster_info).unwrap_or_else(|_| "{}".to_string());
    row.node_info_json = serde_json::to_string(&m.node_info).unwrap_or_else(|_| "[]".to_string());
    row.threadpool_info_json = serde_json::to_string(&m.threadpool_info).unwrap_or_else(|_| "{}".to_string());

    // Compaction (medium)
    row.compaction_pending = Some(m.compaction_info.total_pending_compactions);
    row.compaction_active = Some(m.compaction_info.active_compactions);
    row.compaction_rate_mb_per_sec = Some(m.compaction_info.avg_compaction_rate_mb_per_sec);

    // Repair (medium)
    row.repair_success_rate_pct = Some(m.repair_info.repair_success_rate_pct);
    row.keyspaces_needing_repair = Some(m.repair_info.keyspaces_needing_repair);

    // Tombstone (medium)
    row.tombstone_health_score = Some(m.tombstone_info.overall_health_score);
    row.high_tombstone_ratio_tables = Some(m.tombstone_info.high_tombstone_ratio_tables);

    // JSON blobs (medium)
    row.compaction_info_json = serde_json::to_string(&m.compaction_info).unwrap_or_else(|_| "{}".to_string());
    row.repair_info_json = serde_json::to_string(&m.repair_info).unwrap_or_else(|_| "{}".to_string());
    row.tombstone_info_json = serde_json::to_string(&m.tombstone_info).unwrap_or_else(|_| "{}".to_string());
    row.keyspace_info_json = serde_json::to_string(&m.keyspace_info).unwrap_or_else(|_| "[]".to_string());
    row.table_info_json = serde_json::to_string(&m.table_info).unwrap_or_else(|_| "[]".to_string());
    row.snapshot_info_json = serde_json::to_string(&m.snapshot_info).unwrap_or_else(|_| "{}".to_string());

    // JSON blobs (low)
    row.schema_info_json = serde_json::to_string(&m.schema_info).unwrap_or_else(|_| "{}".to_string());

    row
}

#[cfg(all(feature = "poll-clickhouse", feature = "all-endpoints"))]
fn build_clickhouse_poll_row(
    m: &ClickhouseMetadata,
    snapshot_time: chrono::DateTime<chrono::Utc>,
    organization_uuid: String,
    endpoint_uuid: String,
    frequency: String,
    collection_ms: u32,
    had_fatal: bool,
) -> ClickhousePollMetricsRow {
    let mut row = ClickhousePollMetricsRow::common(snapshot_time, organization_uuid, endpoint_uuid, frequency, collection_ms, had_fatal);

    // Activity (high)
    row.running_queries = Some(m.activity_info.running_queries);
    row.queued_queries = Some(m.activity_info.queued_queries);
    row.longest_query_duration = Some(m.activity_info.longest_query_duration);
    row.queries_per_second = Some(m.activity_info.queries_per_second);
    row.query_memory_usage = Some(m.activity_info.query_memory_usage);

    // Connections (high)
    row.total_connections = Some(m.connection_info.total_connections);
    row.max_connections = Some(m.connection_info.max_connections);
    row.connection_utilization_pct = Some(m.connection_info.connection_utilization_pct);
    row.active_users_count = Some(m.connection_info.active_users_count);

    // Queries (high)
    row.slow_queries = Some(m.query_info.slow_queries);
    row.high_memory_queries = Some(m.query_info.high_memory_queries);
    row.avg_query_execution_time = Some(m.query_info.avg_query_execution_time);
    row.total_bytes_read = Some(m.query_info.total_bytes_read);
    row.total_rows_processed = Some(m.query_info.total_rows_processed);

    // Cluster (high)
    row.cluster_health_pct = Some(m.cluster_info.cluster_health_pct);
    row.total_shards = Some(m.cluster_info.total_shards);
    row.total_replicas = Some(m.cluster_info.total_replicas);

    // Replication (high)
    row.avg_replication_lag = Some(m.replication_info.avg_replication_lag);
    row.max_replication_lag = Some(m.replication_info.max_replication_lag);
    row.lagging_tables = Some(m.replication_info.lagging_tables);
    row.readonly_tables = Some(m.replication_info.readonly_tables);
    row.total_queue_size = Some(m.replication_info.total_queue_size);

    // Storage (high)
    row.total_disk_usage = Some(m.storage_info.total_disk_usage);
    row.total_rows_stored = Some(m.storage_info.total_rows);
    row.avg_compression_ratio = Some(m.storage_info.avg_compression_ratio);
    row.fragmented_tables = Some(m.storage_info.fragmented_tables);
    row.reclaimable_space = Some(m.storage_info.reclaimable_space);

    // ZooKeeper (high)
    row.zk_active_connections = Some(m.zookeeper_info.active_connections);
    row.zk_max_replication_lag_seconds = Some(m.zookeeper_info.max_replication_lag_seconds);
    row.zk_detached_replicas = Some(m.zookeeper_info.detached_replicas);
    row.zk_readonly_replicas = Some(m.zookeeper_info.readonly_replicas);

    // JSON blobs (high)
    row.activity_info_json = serde_json::to_string(&m.activity_info).unwrap_or_else(|_| "{}".to_string());
    row.connection_info_json = serde_json::to_string(&m.connection_info).unwrap_or_else(|_| "{}".to_string());
    row.query_info_json = serde_json::to_string(&m.query_info).unwrap_or_else(|_| "{}".to_string());
    row.cluster_info_json = serde_json::to_string(&m.cluster_info).unwrap_or_else(|_| "{}".to_string());
    row.replication_info_json = serde_json::to_string(&m.replication_info).unwrap_or_else(|_| "{}".to_string());
    row.storage_info_json = serde_json::to_string(&m.storage_info).unwrap_or_else(|_| "{}".to_string());
    row.zookeeper_info_json = serde_json::to_string(&m.zookeeper_info).unwrap_or_else(|_| "{}".to_string());

    // Merges (medium)
    row.running_merges = Some(m.merge_info.running_merges);
    row.queued_merges = Some(m.merge_info.queued_merges);
    row.avg_merge_throughput = Some(m.merge_info.avg_merge_throughput);

    // Mutations (medium)
    row.active_mutations = Some(m.mutation_info.active_mutations);
    row.failed_mutations = Some(m.mutation_info.failed_mutations);
    row.stuck_mutations = Some(m.mutation_info.stuck_mutations);

    // JSON blobs (medium)
    row.merge_info_json = serde_json::to_string(&m.merge_info).unwrap_or_else(|_| "{}".to_string());
    row.mutation_info_json = serde_json::to_string(&m.mutation_info).unwrap_or_else(|_| "{}".to_string());
    row.part_info_json = serde_json::to_string(&m.part_info).unwrap_or_else(|_| "[]".to_string());
    row.database_stats_json = serde_json::to_string(&m.database_stats).unwrap_or_else(|_| "[]".to_string());
    row.table_info_json = serde_json::to_string(&m.table_info).unwrap_or_else(|_| "[]".to_string());

    // JSON blobs (low)
    row.dictionary_info_json = serde_json::to_string(&m.dictionary_info).unwrap_or_else(|_| "[]".to_string());
    row.settings_info_json = serde_json::to_string(&m.settings_info).unwrap_or_else(|_| "{}".to_string());

    row
}

#[cfg(feature = "poll-clickhouse")]
async fn run_poll_ingestion(db_manager: Arc<EdenDb>, mut rx: tokio::sync::mpsc::Receiver<PollMetricsRow>) {
    // Ensure poll metrics tables exist (idempotent; also called by analytics.rs
    // Poll-to-ClickHouse can run standalone.
    match db_manager.clickhouse_connection().await {
        Ok(client) => {
            #[cfg(not(embedded_db))]
            if let Err(err) = analytics_schema::ddl::ensure_poll_tables(&client).await {
                tracing::error!(error = %err, "poll metrics: failed to ensure ClickHouse tables");
            }

            #[cfg(embedded_db)]
            if let Err(err) = client.ensure_schema().await {
                tracing::error!(error = %err, "poll metrics: failed to ensure DuckDB analytics schema");
            }
        }
        Err(err) => {
            tracing::error!(error = %err, "poll metrics: failed to get analytics connection for DDL init");
        }
    }

    let mut redis_batch: Vec<RedisPollMetricsRow> = Vec::with_capacity(64);
    let mut pg_batch: Vec<PostgresPollMetricsRow> = Vec::with_capacity(64);
    let mut mongo_batch: Vec<MongoPollMetricsRow> = Vec::with_capacity(64);
    let mut oracle_batch: Vec<OraclePollMetricsRow> = Vec::with_capacity(64);
    let mut cassandra_batch: Vec<CassandraPollMetricsRow> = Vec::with_capacity(64);
    let mut ch_batch: Vec<ClickhousePollMetricsRow> = Vec::with_capacity(64);

    let flush_interval = Duration::from_secs(30);
    let max_batch = 128;
    let mut ticker = interval(flush_interval);
    ticker.tick().await; // consume immediate tick

    let stats = Arc::new(ch_push::DeadLetterStats::default());
    let mut breaker = ch_push::CircuitBreaker::new(stats, 5, Duration::from_secs(60));

    loop {
        tokio::select! {
            biased;
            _ = ticker.tick() => {
                if !breaker.allow() {
                    continue;
                }
                let ok = flush_poll_batches(
                    &db_manager,
                     &mut redis_batch,
                     &mut pg_batch,
                     &mut mongo_batch,
                     &mut oracle_batch,
                    &mut cassandra_batch,
                     &mut ch_batch,
                ).await;
                if ok {
                    breaker.on_success();
                } else {
                    breaker.on_failure();
                }
            }
            maybe_row = rx.recv() => {
                match maybe_row {
                    Some(PollMetricsRow::Redis(row)) => {
                        redis_batch.push(row);
                        if redis_batch.len() >= max_batch {
                            flush_single_poll_batch(&db_manager, &mut redis_batch, poll_tables::REDIS_POLL_METRICS, "redis-poll").await;
                        }
                    }
                    Some(PollMetricsRow::Postgres(row)) => {
                        pg_batch.push(row);
                        if pg_batch.len() >= max_batch {
                            flush_single_poll_batch(&db_manager, &mut pg_batch, poll_tables::POSTGRES_POLL_METRICS, "postgres-poll").await;
                        }
                    }
                    Some(PollMetricsRow::Mongo(row)) => {
                        mongo_batch.push(row);
                        if mongo_batch.len() >= max_batch {
                            flush_single_poll_batch(&db_manager, &mut mongo_batch, poll_tables::MONGO_POLL_METRICS, "mongo-poll").await;
                        }
                    }
                    Some(PollMetricsRow::Oracle(row)) => {
                        oracle_batch.push(row);
                        if oracle_batch.len() >= max_batch {
                            flush_single_poll_batch(&db_manager, &mut oracle_batch, poll_tables::ORACLE_POLL_METRICS, "oracle-poll").await;
                        }
                    }
                    Some(PollMetricsRow::Cassandra(row)) => {
                        cassandra_batch.push(row);
                        if cassandra_batch.len() >= max_batch {
                            flush_single_poll_batch(&db_manager, &mut cassandra_batch, poll_tables::CASSANDRA_POLL_METRICS, "cassandra-poll").await;
                        }
                    }
                    Some(PollMetricsRow::Clickhouse(row)) => {
                        ch_batch.push(row);
                        if ch_batch.len() >= max_batch {
                            flush_single_poll_batch(&db_manager, &mut ch_batch, poll_tables::CLICKHOUSE_POLL_METRICS, "clickhouse-poll").await;
                        }
                    }
                    None => {
                        let _ = flush_poll_batches(
                            &db_manager,
                             &mut redis_batch,
                             &mut pg_batch,
                             &mut mongo_batch,
                             &mut oracle_batch,
                            &mut cassandra_batch,
                             &mut ch_batch,
                        ).await;
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "poll-clickhouse")]
async fn flush_poll_batches(
    db_manager: &Arc<EdenDb>,
    redis_batch: &mut Vec<RedisPollMetricsRow>,
    pg_batch: &mut Vec<PostgresPollMetricsRow>,
    mongo_batch: &mut Vec<MongoPollMetricsRow>,
    oracle_batch: &mut Vec<OraclePollMetricsRow>,
    cassandra_batch: &mut Vec<CassandraPollMetricsRow>,
    ch_batch: &mut Vec<ClickhousePollMetricsRow>,
) -> bool {
    let mut all_ok = true;
    if !redis_batch.is_empty() {
        all_ok &= flush_single_poll_batch(db_manager, redis_batch, poll_tables::REDIS_POLL_METRICS, "redis-poll").await;
    }
    if !pg_batch.is_empty() {
        all_ok &= flush_single_poll_batch(db_manager, pg_batch, poll_tables::POSTGRES_POLL_METRICS, "postgres-poll").await;
    }
    if !mongo_batch.is_empty() {
        all_ok &= flush_single_poll_batch(db_manager, mongo_batch, poll_tables::MONGO_POLL_METRICS, "mongo-poll").await;
    }
    if !oracle_batch.is_empty() {
        all_ok &= flush_single_poll_batch(db_manager, oracle_batch, poll_tables::ORACLE_POLL_METRICS, "oracle-poll").await;
    }
    if !cassandra_batch.is_empty() {
        all_ok &= flush_single_poll_batch(db_manager, cassandra_batch, poll_tables::CASSANDRA_POLL_METRICS, "cassandra-poll").await;
    }
    if !ch_batch.is_empty() {
        all_ok &= flush_single_poll_batch(db_manager, ch_batch, poll_tables::CLICKHOUSE_POLL_METRICS, "clickhouse-poll").await;
    }
    all_ok
}

#[cfg(feature = "poll-clickhouse")]
async fn flush_single_poll_batch<T: clickhouse::Row + serde::Serialize + Sync>(
    db_manager: &Arc<EdenDb>,
    batch: &mut Vec<T>,
    table: &str,
    label: &str,
) -> bool {
    if batch.is_empty() {
        return true;
    }
    let client = match db_manager.clickhouse_connection().await {
        Ok(c) => c,
        Err(err) => {
            tracing::error!(label, error = %err, "poll metrics flush: failed to get CH connection");
            return false;
        }
    };
    let rows = std::mem::take(batch);
    let count = rows.len();
    let retry_cfg = ch_push::RetryConfig { max_retries: 2, initial_backoff: Duration::from_millis(250) };

    #[cfg(not(embedded_db))]
    let result = ch_push::with_retry(retry_cfg, None, || insert_batch(&client, table, &rows)).await;

    #[cfg(embedded_db)]
    let result = ch_push::with_retry(retry_cfg, None, || client.insert_rows(table, &rows)).await;

    match result {
        Ok(()) => true,
        Err(err) => {
            tracing::error!(label, count, error = %err, "poll metrics flush: insert failed after retries");
            // Re-queue for retry, but cap at 512 to bound memory
            if rows.len() < 512 {
                *batch = rows;
            }
            false
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum SchedulerError {
    UnsupportedKind(EpKind),
    #[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
    ConnectionFailed(ConnectionError),
    PublishFailed(PublishError),
}

#[derive(Debug)]
#[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
enum ConnectionError {
    RouterMissing(EpKind),
    RouterDowncast {
        kind: EpKind,
        type_name: String,
    },
    EndpointMissing {
        kind: EpKind,
        endpoint: EndpointCacheUuid,
    },
    ConnUnavailable {
        kind: EpKind,
        endpoint: EndpointCacheUuid,
        error: EpError,
    },
}

#[cfg(any(feature = "all-endpoints", feature = "mongo", feature = "postgres", feature = "redis"))]
impl ConnectionError {
    fn to_ep_error(&self) -> EpError {
        match self {
            ConnectionError::RouterMissing(kind) => EpError::Metadata(MetadataError::RouterMissing { kind: kind.to_string() }),
            ConnectionError::RouterDowncast { kind, type_name } => {
                EpError::Metadata(MetadataError::RouterTypeMismatch { kind: kind.to_string(), expected: type_name.clone() })
            }
            ConnectionError::EndpointMissing { kind, endpoint } => {
                EpError::Metadata(MetadataError::EndpointMissing { kind: kind.to_string(), endpoint: endpoint.to_string() })
            }
            ConnectionError::ConnUnavailable { kind, endpoint, error } => {
                let details = format!("{} ({})", error.error_hex(), error);
                EpError::Metadata(MetadataError::ConnectionUnavailable {
                    kind: kind.to_string(),
                    endpoint: endpoint.to_string(),
                    details,
                })
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum PublishError {
    Serialize {
        endpoint: EndpointCacheUuid,
        frequency: SyncFrequency,
        error: serde_json::Error,
    },
    Publish {
        endpoint: EndpointCacheUuid,
        frequency: SyncFrequency,
        error: EpError,
    },
}

#[derive(SerdeDeserialize, IntoParams)]
pub struct MetadataHistoryQuery {
    /// Time range, e.g. 30m, 6h, 2h30m, 7d (default: 24h, max 365d).
    pub range: Option<String>,
    /// Filter by collection frequency: high, medium, low.
    pub frequency: Option<String>,
    /// Maximum number of data points to return (default: 200).
    pub limit: Option<u64>,
}

/// GET /api/v1/endpoints/{endpoint}/metadata/history
///
/// Returns historical poll metrics for the endpoint from ClickHouse.
/// The response includes universal fields (snapshot_time, frequency,
/// collection_ms, had_fatal) plus a `db_specific` JSON object containing
/// all DB-kind-specific columns.
/// **Permissions**: See exact permission-bit checks in the handler body.
#[cfg(feature = "poll-clickhouse")]
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
pub async fn metadata_history(
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    query: web::Query<MetadataHistoryQuery>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_cache_uuid = endpoint_schema.cache_key(organization_cache_uuid.clone());
    let auth_mode = verify_endpoint_access(
        &database,
        &auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    reject_metadata_when_els_active(auth_mode).map_err(|e| error_handling(e, &mut span))?;

    let _ = query;

    Ok(actix_web::HttpResponse::Gone().json(serde_json::json!({
        "error": "metadata_history_unavailable",
        "message": "Metadata poll history is not included in this build."
    })))
}

#[cfg(not(feature = "poll-clickhouse"))]
pub async fn metadata_history() -> impl Responder {
    actix_web::HttpResponse::ServiceUnavailable().body("poll-clickhouse feature disabled")
}

#[cfg(test)]
mod tests {
    use super::*;
    use eden_core::format::cache_uuid::CacheUuid;
    use std::collections::HashMap;

    fn assert_duration_close(actual: Duration, expected: Duration) {
        assert!(actual.abs_diff(expected) <= Duration::from_micros(1), "expected {expected:?}, got {actual:?}");
    }

    fn test_backoff_config() -> BackoffConfig {
        BackoffConfig {
            base: Duration::from_millis(100),
            factor: 2,
            max: Duration::from_millis(500),
        }
    }

    #[test]
    fn failure_state_new_schedules_base_delay() {
        let cfg = test_backoff_config();
        let now = Instant::now();

        let state = FailureState::new(now, &cfg);

        assert_eq!(state.attempts, 1);
        assert_eq!(state.next_allowed.duration_since(now), cfg.base);
    }

    #[test]
    fn failure_state_updates_multiply_delay_and_cap_at_max() {
        let cfg = test_backoff_config();
        let now = Instant::now();
        let mut state = FailureState::new(now, &cfg);

        state.update_next(now, &cfg);
        assert_eq!(state.attempts, 2);
        assert_duration_close(state.next_allowed.duration_since(now), Duration::from_millis(200));

        state.update_next(now, &cfg);
        assert_eq!(state.attempts, 3);
        assert_duration_close(state.next_allowed.duration_since(now), Duration::from_millis(400));

        state.update_next(now, &cfg);
        assert_eq!(state.attempts, 4);
        assert_duration_close(state.next_allowed.duration_since(now), Duration::from_millis(500));

        state.update_next(now, &cfg);
        assert_eq!(state.attempts, 5);
        assert_duration_close(state.next_allowed.duration_since(now), Duration::from_millis(500));
    }

    #[test]
    fn backoff_delay_sequence_starts_at_base_and_caps() {
        let cfg = test_backoff_config();

        assert_duration_close(backoff_delay(1, &cfg), Duration::from_millis(100));
        assert_duration_close(backoff_delay(2, &cfg), Duration::from_millis(200));
        assert_duration_close(backoff_delay(3, &cfg), Duration::from_millis(400));
        assert_duration_close(backoff_delay(4, &cfg), Duration::from_millis(500));
        assert_duration_close(backoff_delay(5, &cfg), Duration::from_millis(500));
    }

    #[test]
    fn clearing_failure_state_resets_backoff_attempts() {
        let cfg = test_backoff_config();
        let now = Instant::now();
        let endpoint = EndpointCacheUuid::new(None, EndpointUuid::new_uuid());
        let key = (endpoint, SyncFrequency::High);
        let mut failures: HashMap<(EndpointCacheUuid, SyncFrequency), FailureState> = HashMap::new();

        failures.insert(key.clone(), FailureState::new(now, &cfg));
        if let Some(state) = failures.get_mut(&key) {
            state.update_next(now, &cfg);
        } else {
            panic!("state must exist");
        }
        let attempts_before_clear = failures.get(&key).map(|state| state.attempts).unwrap_or_default();
        assert_eq!(attempts_before_clear, 2);

        failures.remove(&key);
        failures.insert(key.clone(), FailureState::new(now, &cfg));
        let attempts_after_clear = failures.get(&key).map(|state| state.attempts).unwrap_or_default();
        assert_eq!(attempts_after_clear, 1);
    }

    #[test]
    fn reject_metadata_when_els_active_blocks_metadata_routes() {
        assert!(reject_metadata_when_els_active(AuthMode::Rbac).is_ok());

        let err = reject_metadata_when_els_active(AuthMode::Els).expect_err("ELS mode should reject metadata");
        assert!(err.to_string().contains("Metadata introspection is not available"));
    }
}
