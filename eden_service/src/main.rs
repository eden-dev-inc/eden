use actix_governor::GovernorConfigBuilder;
use actix_web::web::Data;
use database::db::els::ElsCommands;
#[cfg(not(embedded_db))]
use database::db::lib::ClickhouseDbConfig;
#[cfg(not(embedded_db))]
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::methods::insert::InsertMethod;
use database::db::methods::insert::eden_node::InsertEdenNode;
use eden_core::auth::Jwt;
use eden_core::comm::NodeData;
use eden_core::format::cache_id::EdenNodeCacheId;
use eden_core::format::cache_uuid::EdenNodeCacheUuid;
use eden_core::request::ServerData;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{MetricsMiddleware, TelemetryDurations, TelemetryWrapper, setup_metrics};
#[cfg(any(feature = "llm", feature = "telemetry-export"))]
use eden_logger_internal::log_warn;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_info};
use eden_service::EdenDb;
#[cfg(not(embedded_db))]
use eden_service::backup_restore::maybe_restore_backup;
#[cfg(not(embedded_db))]
use eden_service::config::{ClickhouseConfig, DbConfig, PostgresConfig, RedisConfig};
use eden_service::config::{ContainerConfig, EdenAppConfig, EngineConfig};
use eden_service::runtime_affinity;
use ep_runtime::comp::MyEngineService;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use eden_service::data::LicenseRsaPublicKey;
use eden_service::http_server;
use endpoint_core::ep_core::database::schema::{Table, eden_node::EdenNodeSchema};
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use function_name::named;
#[cfg(feature = "telemetry-export")]
use opentelemetry::trace::TracerProvider;
use serde_json::json;
use std::env;
#[cfg(feature = "telemetry-export")]
use std::time::Duration;
#[cfg(feature = "telemetry-export")]
use telemetry_exporters::dogstatsd::DogStatsDConfig;
#[cfg(feature = "telemetry-export")]
use telemetry_exporters::initialize_tracer;
#[cfg(feature = "telemetry-export")]
use telemetry_exporters::spans::SpanExportConfig;
#[cfg(feature = "telemetry-export")]
use telemetry_exporters::sweeper::SweepConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var("RUST_LOG").is_err() {
        unsafe {
            env::set_var("RUST_LOG", "debug");
        }
    }
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }

    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all();
    let gateway_cpu_affinity = eden_config::services().eden.gateway_cpu_affinity;
    let runtime_affinity =
        runtime_affinity::configure_tokio_worker_affinity(&mut runtime_builder, gateway_cpu_affinity).map_err(std::io::Error::other)?;
    let runtime = runtime_builder.build()?;

    runtime.block_on(async_main(runtime_affinity))
}

#[named]
async fn async_main(runtime_affinity: runtime_affinity::RuntimeAffinityPlan) -> Result<(), Box<dyn std::error::Error>> {
    // First, get the Eden config (needed for tracer initialization)
    let eden_config = EdenAppConfig::new()?;

    #[cfg(embedded_db)]
    if eden_config::encryption().enabled {
        return Err(std::io::Error::other(
            "Refusing to start with `embedded-db` while encryption.enabled=true: ELS cache encryption is disabled in embedded-db builds",
        )
        .into());
    }

    // Initialize eden_logger_internal runtime filter from centralised config so that
    // EDEN_LOG_LEVEL no longer needs to be read directly from the environment.
    eden_logger_internal::init_from_value(&eden_config::telemetry().log_level);

    let tracing_default_level = eden_config::telemetry().log_level.clone();

    #[cfg(feature = "telemetry-export")]
    {
        if eden_config::telemetry().otlp_export_enabled {
            let provider = initialize_tracer("eden", eden_config.otlp_collector(), eden_config.otlp_db_collector()).await;
            let tracer = provider.tracer("eden");

            tracing_subscriber::registry()
                .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_default_level.into()))
                .with(tracing_subscriber::fmt::layer())
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .init();
        } else {
            tracing_subscriber::registry()
                .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_default_level.into()))
                .with(tracing_subscriber::fmt::layer())
                .init();
        }
    }

    #[cfg(not(feature = "telemetry-export"))]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_default_level.into()))
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    let _ctx = ctx_with_trace!().with_feature("main");

    log_debug!(_ctx.clone(), "Starting eden-service", audience = eden_logger_internal::LogAudience::Internal);

    match &runtime_affinity {
        runtime_affinity::RuntimeAffinityPlan::PerformanceCores(selection) => {
            log_info!(
                _ctx.clone(),
                "Pinned main tokio runtime workers to performance cores for interlay proxy work",
                audience = eden_logger_internal::LogAudience::Internal,
                source = selection.source,
                threshold = selection.threshold,
                min_metric = selection.min_metric,
                max_metric = selection.max_metric,
                logical_processor_ids = format!("{:?}", selection.logical_processor_ids)
            );
        }
        runtime_affinity::RuntimeAffinityPlan::Unpinned => {
            log_debug!(
                _ctx.clone(),
                "Leaving main tokio runtime workers unpinned",
                audience = eden_logger_internal::LogAudience::Internal
            );
        }
    }

    log_debug!(
        _ctx.clone(),
        "Using eden configuration",
        audience = eden_logger_internal::LogAudience::Internal,
        config = format!("{:?}", eden_config)
    );

    log_debug!(
        _ctx.clone(),
        "Starting REST server for eden-service",
        audience = eden_logger_internal::LogAudience::Internal,
        url = hex::encode(eden_config.url())
    );

    // Base URL used for tools relay URLs (served by this HTTP service).
    let mut engine_url = eden_config.url();
    if !engine_url.starts_with("http://") && !engine_url.starts_with("https://") {
        engine_url = format!("http://{engine_url}");
    }

    let _engine_config = EngineConfig::new()?;

    log_debug!(
        _ctx.clone(),
        "Starting REST server for engine-service",
        audience = eden_logger_internal::LogAudience::Internal,
        url = hex::encode(_engine_config.url())
    );

    #[cfg(not(embedded_db))]
    let analytics_db_config = {
        let clickhouse_config = ClickhouseConfig::new()?;
        ClickhouseDbConfig::new(
            clickhouse_config.url(),
            clickhouse_config.username_opt().map(str::to_string),
            clickhouse_config.password_opt().map(str::to_string),
            clickhouse_config.database().map(str::to_string),
            clickhouse_config.pool_size(),
        )?
    };

    #[cfg(embedded_db)]
    let analytics_db_config = database::db::duckdb_analytics::DuckDbAnalyticsConfig::from_telemetry(&eden_config::telemetry().duckdb)?;

    let jwt = Jwt::new(eden_config.jwt_secret(), eden_config.jwt_expiry_s());

    #[cfg(not(embedded_db))]
    let database_manager: Data<EdenDb> = {
        let redis_config = RedisConfig::new()?;
        let postgres_config = PostgresConfig::new()?;
        let db = Data::new(
            DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(
                &redis_config.url(),
                &postgres_config.url(),
                analytics_db_config,
                database::db::lib::CacheTtl::from_secs(redis_config.cache_ttl()),
                Some(jwt),
            )
            .await?,
        );

        // Attempt backup restoration if configured
        let pg_password = postgres_config.password();
        match maybe_restore_backup(&db, &pg_password).await {
            Ok(true) => {
                log_info!(
                    _ctx.clone(),
                    "Database restored from backup",
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
            Ok(false) => {
                // No restoration requested, continue normally
            }
            Err(e) => {
                log_error!(
                    _ctx.clone(),
                    "Backup restoration failed, aborting startup",
                    audience = eden_logger_internal::LogAudience::Internal,
                    error = e.to_string()
                );
                return Err(e.into());
            }
        }
        db
    };

    #[cfg(embedded_db)]
    let database_manager: Data<EdenDb> = {
        let turso_path = std::env::var("EDEN_TURSO_PATH").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            format!("{home}/.eden/local.db")
        });
        let db_encryption_key = std::env::var("EDEN_DB_ENCRYPTION_KEY").ok();
        Data::new(
            EdenDb::new_local(
                &turso_path,
                analytics_db_config,
                database::db::lib::CacheTtl::from_secs(eden_config::limits().redis_cache_ttl_secs),
                Some(jwt),
                db_encryption_key,
            )
            .await?,
        )
    };

    let template_registry = Data::new(TemplateRegistry::new());

    let server_data = ServerData {
        engine_url,
        public_key: eden_config.eden_node_uuid().clone(),
        new_org_token: eden_config.relay_new_org_token().map(str::to_string),
        tools_service_timeout_secs: eden_config.tools_service_timeout_secs(),
        internal_llm: eden_config.internal_llm().cloned(),
    };

    let engine_service = Data::new(MyEngineService::with_database_manager(database_manager.clone().into_inner()));

    let server_data_ref = Data::new(server_data);

    log_debug!(_ctx.clone(), "Engine client config", audience = eden_logger_internal::LogAudience::Internal);

    // Set up metrics
    let all_metrics = setup_metrics(eden_config.otlp_collector(), eden_config.otlp_db_collector())?;
    let all_metrics_data = Data::new(all_metrics);
    let metrics_middleware = MetricsMiddleware::default(); //new(all_metrics_data.clone());
    let cancellation_token = tokio_util::sync::CancellationToken::new();

    #[cfg(feature = "telemetry-export")]
    {
        let telemetry_config = eden_config::telemetry();
        if telemetry_config.dogstatsd_enabled {
            let dogstatsd_endpoint = telemetry_config.dogstatsd_endpoint.trim();
            if dogstatsd_endpoint.is_empty() {
                log_warn!(
                    _ctx.clone(),
                    "DogStatsD exporter enabled without an endpoint; exporter disabled",
                    audience = LogAudience::Internal
                );
            } else {
                let dogstatsd_config =
                    DogStatsDConfig::new(dogstatsd_endpoint.to_string()).with_interval(Duration::from_secs(10)).with_max_packet_size(1432);

                let metrics_for_dogstatsd = all_metrics_data.clone().into_inner();
                let tags: Vec<(String, String)> = vec![
                    ("service".to_string(), "eden".to_string()),
                    ("node_uuid".to_string(), eden_config.eden_node_uuid().to_string()),
                ];
                let mut export_state = eden_core::telemetry::metrics::FastMetricsExportState::new();
                tokio::spawn(telemetry_exporters::dogstatsd::run(dogstatsd_config, cancellation_token.clone(), move |output| {
                    let tag_refs: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    metrics_for_dogstatsd.export_dogstatsd_delta(output, &tag_refs, &mut export_state);
                }));
                log_info!(
                    _ctx.clone(),
                    "DogStatsD telemetry export enabled",
                    audience = LogAudience::Internal,
                    endpoint = dogstatsd_endpoint
                );
            }
        } else {
            log_info!(_ctx.clone(), "DogStatsD telemetry export disabled", audience = LogAudience::Internal);
        }

        let clickhouse_telemetry_enabled = telemetry_config.clickhouse_enabled;
        let clickhouse_telemetry_config =
            telemetry_exporters::clickhouse::ClickHouseTelemetryConfig::new("eden", eden_config.eden_node_uuid().to_string())
                .with_interval(Duration::from_secs(2));
        let clickhouse_pool = database_manager.clickhouse_pool().clone();

        if clickhouse_telemetry_enabled {
            let (log_tx, log_rx) = tokio::sync::mpsc::channel(8192);
            if let Err(err) = eden_logger_internal::install_sink(move |log| {
                let _ = log_tx.try_send(log);
            }) {
                log_warn!(_ctx.clone(), "ClickHouse log sink already installed", audience = LogAudience::Internal, error = err);
            }

            let metrics_for_clickhouse = all_metrics_data.clone().into_inner();
            tokio::spawn(telemetry_exporters::clickhouse::run_metrics(
                clickhouse_pool.clone(),
                clickhouse_telemetry_config.clone(),
                cancellation_token.clone(),
                move |batches, time_unix_nano| metrics_for_clickhouse.export_clickhouse(batches, time_unix_nano),
            ));

            let metrics_for_clickhouse_live = all_metrics_data.clone().into_inner();
            let live_telemetry_config = clickhouse_telemetry_config.clone().with_interval(Duration::from_secs(1));
            tokio::spawn(telemetry_exporters::clickhouse::run_metrics(
                clickhouse_pool.clone(),
                live_telemetry_config,
                cancellation_token.clone(),
                move |batches, time_unix_nano| metrics_for_clickhouse_live.export_clickhouse_live(batches, time_unix_nano),
            ));

            tokio::spawn(telemetry_exporters::clickhouse::run_logs(
                clickhouse_pool.clone(),
                clickhouse_telemetry_config.clone(),
                cancellation_token.clone(),
                log_rx,
            ));

            log_info!(_ctx.clone(), "ClickHouse telemetry sync enabled", audience = LogAudience::Internal);
        } else {
            log_info!(_ctx.clone(), "ClickHouse telemetry sync disabled", audience = LogAudience::Internal);
        }

        let otlp_span_config = telemetry_config
            .otlp_export_enabled
            .then(|| SpanExportConfig::new(telemetry_config.traces_endpoint()).with_service_name("eden"));
        let clickhouse_span_export = clickhouse_telemetry_enabled.then_some((clickhouse_pool, clickhouse_telemetry_config));
        if clickhouse_span_export.is_some() || otlp_span_config.is_some() {
            tokio::spawn(telemetry_exporters::clickhouse::run_span_fanout(
                all_metrics_data.span_collector().clone(),
                otlp_span_config,
                clickhouse_span_export,
                cancellation_token.clone(),
            ));
        }
        // Spawn background sweeper to evict stale dynamic metric series
        let metrics_for_sweeper = all_metrics_data.clone().into_inner();
        tokio::spawn(telemetry_exporters::sweeper::run(
            SweepConfig::new(),
            cancellation_token.clone(),
            move |threshold| metrics_for_sweeper.evict_stale_series(threshold),
        ));
    }

    // Seed builtin skills (idempotent).
    #[cfg(feature = "llm")]
    {
        let mut seed_telemetry = TelemetryWrapper::new(
            all_metrics_data.clone().into_inner(),
            TelemetryLabels::new(eden_config.eden_node_uuid()),
            TelemetryDurations::default(),
        );
        match eden_service::seed_skills::seed_builtin_skills(&database_manager, &mut seed_telemetry).await {
            Ok(()) => {
                log_debug!(
                    _ctx.clone(),
                    "Builtin skills seeded into database",
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
            Err(e) => {
                log_warn!(
                    _ctx.clone(),
                    "Failed to seed builtin skills (non-fatal)",
                    audience = eden_logger_internal::LogAudience::Internal,
                    error = e.to_string()
                );
            }
        }
    }

    match database_manager.els_warm_all_caches().await {
        Ok(warmed) => {
            log_info!(
                _ctx.clone(),
                "ELS cache warmup completed",
                audience = LogAudience::Internal,
                warmed_assignments = warmed.to_string()
            );
        }
        Err(e) => {
            log_error!(
                _ctx.clone(),
                "ELS cache warmup failed; service will continue with database fallback",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
        }
    }

    let mut governor_config_builder = GovernorConfigBuilder::default();
    if eden_config.rate_limit() == 0 {
        log_info!(_ctx.clone(), "Rate limiting turned off", audience = eden_logger_internal::LogAudience::Internal);
        governor_config_builder.permissive(true);
    } else {
        log_info!(
            _ctx.clone(),
            "Rate limiting configured",
            audience = eden_logger_internal::LogAudience::Internal,
            milliseconds_per_request = eden_config.rate_limit()
        );
        let _ = governor_config_builder.milliseconds_per_request(eden_config.rate_limit()).burst_size(100).use_headers();
    }
    let governor_conf = governor_config_builder.finish().unwrap_or_default();

    let eden_node_schema = EdenNodeSchema::new("node_0".to_string(), eden_config.eden_node_uuid().clone(), vec![], json!(""));

    let node_data = Data::new(NodeData::new(eden_node_schema.id(), eden_node_schema.uuid()));

    match <EdenDb as InsertMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeCacheId, InsertEdenNode>>::insert(
        &database_manager,
        InsertEdenNode::new(eden_node_schema),
        &mut TelemetryWrapper::new(
            all_metrics_data.clone().into_inner(),
            TelemetryLabels::new(eden_config.eden_node_uuid()),
            TelemetryDurations::default(),
        ),
    )
    .await
    {
        Ok(_) => {
            log_debug!(
                _ctx.clone(),
                "Adding Eden Node to database",
                audience = eden_logger_internal::LogAudience::Internal,
                node_uuid = node_data.uuid().to_string()
            );
        }
        Err(e) => {
            log_error!(
                _ctx,
                "Failed to add Eden Node to database",
                audience = eden_logger_internal::LogAudience::Internal,
                error = e.to_string()
            );
        }
    }

    let license_rsa_key = Data::new(LicenseRsaPublicKey(None));

    http_server(
        governor_conf,
        metrics_middleware,
        all_metrics_data,
        node_data,
        server_data_ref,
        engine_service,
        template_registry,
        database_manager,
        license_rsa_key,
        eden_config.port(),
    )
    .await?;

    // Gracefully stop the stale-series sweeper and RBAC Redis-PG sync worker
    cancellation_token.cancel();
    Ok(())
}
