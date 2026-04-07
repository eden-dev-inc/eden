mod config;
mod eden_client;
mod metrics;
mod queries;
mod workers;

use axum::{Router, extract::State, routing::get};
use clap::Parser;
use config::Config;
use eden_client::EdenClient;
use metrics::AppMetrics;
use regex::Regex;
use serde_json::json;
use std::net::IpAddr;
use std::sync::Arc;
use tracing::{error, info, warn};
use workers::{CrossDbWorker, EndpointDef, Endpoints, MetricsReporter, SingleDbWorker};

#[derive(Clone)]
struct AppState {
    metrics: Arc<AppMetrics>,
    config: Arc<Config>,
    endpoints: Endpoints,
}

const ADAM_RUNTIME_USERNAME: &str = "adam-demo-runner";
const ADAM_RUNTIME_PASSWORD: &str = "adam-demo-runner-pass";
const ADAM_RUNTIME_USER_ACCESS_LEVEL: &str = "Read";
const ADAM_RUNTIME_CONTROL_ACCESS_LEVEL: &str = "Read";
const ADAM_RUNTIME_DATA_ACCESS_LEVEL: &str = "Admin";

fn clickhouse_endpoints_disabled() -> bool {
    let value = std::env::var("DISABLE_CLICKHOUSE_ENDPOINTS").unwrap_or_else(|_| "1".to_string());
    value.trim().eq_ignore_ascii_case("1") || value.trim().eq_ignore_ascii_case("true")
}

fn trim_bracketed_host(host: &str) -> &str {
    host.trim_start_matches('[').trim_end_matches(']')
}

fn is_loopback_host(host: &str) -> bool {
    let host = trim_bracketed_host(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

fn is_private_or_local_host(host: &str) -> bool {
    let host = trim_bracketed_host(host);
    if host.eq_ignore_ascii_case("localhost") || host.eq_ignore_ascii_case("host.docker.internal") {
        return true;
    }

    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        Ok(IpAddr::V6(ip)) => {
            ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local()
        }
        Err(_) => false,
    }
}

fn normalize_loopback_url_host(url: &str) -> String {
    let mut parsed = match reqwest::Url::parse(url) {
        Ok(parsed) => parsed,
        Err(_) => return url.to_string(),
    };

    let Some(host) = parsed.host_str() else {
        return url.to_string();
    };

    if !is_loopback_host(host) {
        return url.to_string();
    }

    if parsed.set_host(Some("host.docker.internal")).is_ok() {
        parsed.to_string()
    } else {
        url.to_string()
    }
}

fn validate_public_http_base_url(label: &str, url: &str) -> Option<String> {
    let parsed = match reqwest::Url::parse(url) {
        Ok(parsed) => parsed,
        Err(err) => {
            warn!(
                "Skipping {} endpoint registration because '{}' is not a valid URL: {}",
                label, url, err
            );
            return None;
        }
    };

    let Some(host) = parsed.host_str() else {
        warn!(
            "Skipping {} endpoint registration because '{}' does not include a hostname",
            label, url
        );
        return None;
    };

    if is_private_or_local_host(host) {
        warn!(
            "Skipping {} endpoint registration because '{}' points at a local/private host ('{}'), which Eden will reject during tool initialization",
            label, url, host
        );
        return None;
    }

    Some(parsed.to_string())
}

fn azure_endpoint_description(config: &Config) -> String {
    let display_name = config.azure_display_name.trim();
    let subscription_id = config.azure_subscription_id.trim();

    if !display_name.is_empty() {
        format!(
            "Azure Resource Manager APIs via HTTP for service principal '{}'",
            display_name
        )
    } else if !subscription_id.is_empty() {
        format!(
            "Azure Resource Manager APIs via HTTP for subscription '{}'",
            subscription_id
        )
    } else {
        "Azure Resource Manager APIs via HTTP".to_string()
    }
}

fn azure_refresh_interval(expires_in_secs: u64) -> std::time::Duration {
    let safety_buffer_secs = if expires_in_secs > 600 { 300 } else { 60 };
    std::time::Duration::from_secs(expires_in_secs.saturating_sub(safety_buffer_secs).max(60))
}

async fn azure_management_connection(
    config: &Config,
) -> Result<(serde_json::Value, std::time::Duration), Box<dyn std::error::Error + Send + Sync>> {
    let subscription_base_url = format!(
        "{}/subscriptions/{}",
        config.azure_api_base_url.trim_end_matches('/'),
        config.azure_subscription_id.trim()
    );

    let base_url = validate_public_http_base_url("Azure Resource Manager", &subscription_base_url)
        .ok_or_else(|| {
            format!(
                "Azure Resource Manager base URL '{}' is not public or valid",
                subscription_base_url
            )
        })?;

    let token_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        config.azure_tenant.trim()
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.http_timeout))
        .build()?;

    let response = client
        .post(&token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", config.azure_app_id.as_str()),
            ("client_secret", config.azure_password.as_str()),
            ("scope", "https://management.azure.com/.default"),
        ])
        .send()
        .await?;

    let status = response.status();
    let body: serde_json::Value = response.json().await?;

    if !status.is_success() {
        let detail = body
            .get("error_description")
            .and_then(|value| value.as_str())
            .or_else(|| body.get("error").and_then(|value| value.as_str()))
            .unwrap_or("unknown Azure auth error");
        return Err(format!("Azure token request failed ({}): {}", status, detail).into());
    }

    let access_token = body
        .get("access_token")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            format!(
                "Azure token response did not include access_token: {}",
                body
            )
        })?;

    let expires_in_secs = body
        .get("expires_in")
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
        })
        .unwrap_or(3600);

    Ok((
        json!({
            "base_url": base_url,
            "headers": {
                "Authorization": format!("Bearer {}", access_token),
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        }),
        azure_refresh_interval(expires_in_secs),
    ))
}

fn spawn_azure_endpoint_refresh(
    eden: EdenClient,
    config: Config,
    initial_refresh_interval: std::time::Duration,
) {
    tokio::spawn(async move {
        let mut refresh_interval = initial_refresh_interval;

        loop {
            tokio::time::sleep(refresh_interval).await;

            match azure_management_connection(&config).await {
                Ok((conn, next_refresh_interval)) => {
                    refresh_interval = next_refresh_interval;
                    let description = azure_endpoint_description(&config);

                    match eden
                        .create_endpoint("adam_azure", "http", conn, &description)
                        .await
                    {
                        Ok(_) => info!(
                            "Refreshed Azure endpoint token; next refresh in {:?}",
                            refresh_interval
                        ),
                        Err(err) => warn!("Failed to refresh Azure endpoint registration: {}", err),
                    }
                }
                Err(err) => {
                    warn!(
                        "Failed to refresh Azure endpoint token: {}. Retrying in 60s...",
                        err
                    );
                    refresh_interval = std::time::Duration::from_secs(60);
                }
            }
        }
    });
}

async fn cleanup_clickhouse_endpoints(eden: &EdenClient) {
    const CLICKHOUSE_ENDPOINTS: &[&str] = &[
        "adam_clickhouse",
        "stone_clickhouse_analytics",
        "tech_user_events",
        "fin_trading",
        "hc_billing_analytics",
        "ins_claims_analytics",
    ];

    for endpoint_id in CLICKHOUSE_ENDPOINTS {
        match eden.delete_endpoint(endpoint_id).await {
            Ok(()) => info!(
                "ClickHouse cleanup ensured endpoint '{}' is absent",
                endpoint_id
            ),
            Err(err) => warn!(
                "Failed to remove stale ClickHouse endpoint '{}': {}",
                endpoint_id, err
            ),
        }
    }
}

async fn provision_runtime_client(
    admin_eden: &EdenClient,
    config: &Config,
    endpoints: &Endpoints,
) -> Result<EdenClient, Box<dyn std::error::Error + Send + Sync>> {
    info!("\nProvisioning endpoint-scoped runtime user...");

    admin_eden
        .create_user(
            ADAM_RUNTIME_USERNAME,
            ADAM_RUNTIME_PASSWORD,
            "adam-demo-runner@local.eden",
            "ADAM Demo Runner",
            "Scoped runtime user for ADAM demo query traffic",
            ADAM_RUNTIME_USER_ACCESS_LEVEL,
        )
        .await?;

    admin_eden
        .set_org_control_access(&[(ADAM_RUNTIME_USERNAME, ADAM_RUNTIME_CONTROL_ACCESS_LEVEL)])
        .await?;

    for endpoint in &endpoints.silos {
        let endpoint_info = match admin_eden.get_endpoint(&endpoint.endpoint_id).await {
            Ok(info) => info,
            Err(err) => {
                warn!(
                    "Failed to resolve endpoint UUID for '{}' while granting runtime RBAC: {}. Falling back to endpoint id only.",
                    endpoint.endpoint_id, err
                );
                eden_client::EndpointInfo {
                    id: endpoint.endpoint_id.clone(),
                    uuid: endpoint.endpoint_id.clone(),
                    kind: "unknown".to_string(),
                }
            }
        };

        admin_eden
            .set_endpoint_access_aliases(
                &endpoint_info.id,
                Some(endpoint_info.uuid.as_str()),
                &[(ADAM_RUNTIME_USERNAME, ADAM_RUNTIME_CONTROL_ACCESS_LEVEL)],
                true,
                false,
            )
            .await?;
        admin_eden
            .set_endpoint_access_aliases(
                &endpoint_info.id,
                Some(endpoint_info.uuid.as_str()),
                &[(ADAM_RUNTIME_USERNAME, ADAM_RUNTIME_DATA_ACCESS_LEVEL)],
                false,
                true,
            )
            .await?;
    }

    EdenClient::login(
        &config.eden_api_url,
        &config.eden_org_id,
        ADAM_RUNTIME_USERNAME,
        ADAM_RUNTIME_PASSWORD,
        config.http_timeout,
    )
    .await
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "adam_demo=info".into()),
        )
        .init();

    let config = Arc::new(Config::parse());
    let metrics = AppMetrics::new();

    info!("════════════════════════════════════════");
    info!("  ADAM Demo — Cross-Database Query Engine");
    info!("════════════════════════════════════════");
    info!("Vertical:      {}", config.vertical);
    info!("Eden API:      {}", config.eden_api_url);
    info!("Org ID:        {}", config.eden_org_id);
    info!(
        "Target QPS:    {} (total across all DBs)",
        config.queries_per_second
    );
    info!("HTTP timeout:  {}s", config.http_timeout);

    // ── Connect to Eden API ──
    let admin_eden = if config.eden_jwt_token.is_empty() {
        info!("\nSetting up Eden organization and auth...");
        match EdenClient::setup(
            &config.eden_api_url,
            &config.eden_org_id,
            &config.eden_new_org_secret,
            config.http_timeout,
            config.setup_retries,
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to connect to Eden API: {}", e);
                error!("Starting in metrics-only mode");
                start_server_only(config, metrics).await;
                return;
            }
        }
    } else {
        EdenClient::new(
            &config.eden_api_url,
            &config.eden_org_id,
            &config.eden_jwt_token,
        )
    };

    if clickhouse_endpoints_disabled() {
        warn!("DISABLE_CLICKHOUSE_ENDPOINTS is enabled; removing stale ClickHouse endpoints");
        cleanup_clickhouse_endpoints(&admin_eden).await;
    }

    // ── Register endpoints based on vertical ──
    let endpoints = match config.vertical.as_str() {
        "retail" => register_retail_endpoints(&admin_eden, &config).await,
        "stonebreaker" => register_stonebreaker_endpoints(&admin_eden, &config).await,
        "bird" => register_bird_endpoints(&admin_eden, &config).await,
        "tech" => register_tech_endpoints(&admin_eden, &config).await,
        "finance" => register_finance_endpoints(&admin_eden, &config).await,
        "healthcare" => register_healthcare_endpoints(&admin_eden, &config).await,
        "insurance" => register_insurance_endpoints(&admin_eden, &config).await,
        "migration" => register_migration_endpoints(&admin_eden, &config).await,
        _ => {
            warn!(
                "Vertical '{}' not yet supported, falling back to retail",
                config.vertical
            );
            register_retail_endpoints(&admin_eden, &config).await
        }
    };

    let runtime_eden = match provision_runtime_client(&admin_eden, &config, &endpoints).await {
        Ok(client) => client,
        Err(e) => {
            error!(
                "Failed to provision endpoint-scoped runtime user for ADAM demo: {}",
                e
            );
            error!("Starting in metrics-only mode");
            start_server_only(config, metrics).await;
            return;
        }
    };

    // Log endpoint summary
    info!("\nEndpoint registration summary:");
    for def in &endpoints.silos {
        metrics
            .endpoint_healthy
            .with_label_values(&[&def.metrics_label])
            .set(1.0);
        info!(
            "  {:>20}  →  {} ({} QPS)",
            def.metrics_label, def.endpoint_id, def.qps
        );
    }

    // ── Spawn query workers ──
    info!("\nSpawning {} query workers...", endpoints.silos.len());
    for def in &endpoints.silos {
        let worker =
            SingleDbWorker::new(runtime_eden.clone(), metrics.clone(), &config.vertical, def);
        tokio::spawn(worker.run());
    }

    // Cross-database worker (runs every 5 seconds)
    let cross_worker = CrossDbWorker::new(runtime_eden.clone(), metrics.clone(), &endpoints, 5);
    tokio::spawn(cross_worker.run());

    // Metrics reporter
    let reporter = MetricsReporter::new(metrics.clone(), &endpoints, config.metrics_interval);
    tokio::spawn(reporter.run());

    // ── Start HTTP server ──
    let state = AppState {
        metrics,
        config: config.clone(),
        endpoints,
    };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .with_state(state);

    let bind = &config.bind_address;
    info!("\nHTTP server listening on {}", bind);
    info!("  /metrics  — Prometheus metrics");
    info!("  /health   — Health check");
    info!("  /status   — JSON status summary");
    info!("\n════════════════════════════════════════");
    info!("  ADAM Demo running. Ctrl+C to stop.");
    info!("════════════════════════════════════════\n");

    let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ═══════════════════════════════════════════════════════════════
// Vertical: BIRD (single Postgres benchmark database)
// ═══════════════════════════════════════════════════════════════

async fn register_bird_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering BIRD benchmark endpoint with Eden...");

    let pg_bird_url = std::env::var("EDEN_PG_BIRD_URL")
        .or_else(|_| std::env::var("PG_BIRD_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());

    let db_id = std::env::var("BIRD_DB_ID").unwrap_or_else(|_| "auto-selected".to_string());
    let (pg_read_url, pg_write_url, pg_admin_url, pg_system_url) = access_tier_urls(
        &["EDEN_PG_BIRD_READ_URL", "PG_BIRD_READ_URL"],
        &["EDEN_PG_BIRD_WRITE_URL", "PG_BIRD_WRITE_URL"],
        &["EDEN_PG_BIRD_URL", "PG_BIRD_URL"],
        pg_bird_url.clone(),
    );

    let pg_ep = eden
        .create_endpoint(
            "bird_postgres",
            "postgres",
            postgres_endpoint_config(&pg_read_url, &pg_write_url, &pg_admin_url, &pg_system_url),
            &format!(
                "BIRD benchmark database imported from SQLite into Postgres (db_id: {})",
                db_id
            ),
        )
        .await;

    let silos = vec![EndpointDef {
        silo_name: "pg_bird".into(),
        endpoint_id: ep_id(&pg_ep, "bird_postgres"),
        metrics_label: "pg_bird".into(),
        qps: config.queries_per_second,
    }];

    register_external_endpoints(eden, config).await;

    Endpoints {
        vertical: "bird".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Retail (backward compatible)
// ═══════════════════════════════════════════════════════════════

async fn register_retail_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering retail database endpoints with Eden...");

    let eden_pg_url = config
        .eden_postgres_url
        .as_deref()
        .unwrap_or(&config.postgres_url);
    let eden_mongo_url = config
        .eden_mongo_url
        .as_deref()
        .unwrap_or(&config.mongo_url);
    let eden_redis_url = config
        .eden_redis_url
        .as_deref()
        .unwrap_or(&config.redis_url);
    let eden_ch_url = config
        .eden_clickhouse_url
        .as_deref()
        .unwrap_or(&config.clickhouse_url);
    let eden_weaviate_url = config
        .eden_weaviate_url
        .as_deref()
        .unwrap_or(&config.weaviate_url);
    let (pg_read_url, pg_write_url, pg_admin_url, pg_system_url) = access_tier_urls(
        &["EDEN_PG_READ_URL", "PG_READ_URL"],
        &["EDEN_PG_WRITE_URL", "PG_WRITE_URL"],
        &["EDEN_POSTGRES_URL", "POSTGRES_URL"],
        eden_pg_url.to_string(),
    );
    let (mongo_read_url, mongo_write_url, mongo_admin_url, mongo_system_url) = access_tier_urls(
        &["EDEN_MONGO_READ_URL", "MONGO_READ_URL"],
        &["EDEN_MONGO_WRITE_URL", "MONGO_WRITE_URL"],
        &["EDEN_MONGO_URL", "MONGO_URL"],
        eden_mongo_url.to_string(),
    );
    let (redis_read_url, redis_write_url, redis_admin_url, redis_system_url) = access_tier_urls(
        &["EDEN_REDIS_READ_URL", "REDIS_READ_URL"],
        &["EDEN_REDIS_WRITE_URL", "REDIS_WRITE_URL"],
        &["EDEN_REDIS_URL", "REDIS_URL"],
        eden_redis_url.to_string(),
    );
    let (ch_read_url, ch_write_url, ch_admin_url, ch_system_url) = access_tier_urls(
        &["EDEN_CLICKHOUSE_READ_URL", "CLICKHOUSE_READ_URL"],
        &["EDEN_CLICKHOUSE_WRITE_URL", "CLICKHOUSE_WRITE_URL"],
        &["EDEN_CLICKHOUSE_URL", "CLICKHOUSE_URL"],
        eden_ch_url.to_string(),
    );

    // ── Postgres (4 connection tiers in 1 endpoint) ──
    let pg_ep = eden
        .create_endpoint(
            "adam_postgres",
            "postgres",
            postgres_endpoint_config(&pg_read_url, &pg_write_url, &pg_admin_url, &pg_system_url),
            "Marketplace OLTP — users, brands, and marketplace events",
        )
        .await;

    // ── MongoDB (4 connection tiers in 1 endpoint) ──
    let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "ecommerce".to_string());
    let mongo_ep = eden
        .create_endpoint(
            "adam_mongodb",
            "mongo",
            mongo_endpoint_config(
                &mongo_read_url,
                &mongo_write_url,
                &mongo_admin_url,
                &mongo_system_url,
                &mongo_db,
            ),
            "Retail domain — order and cart events as documents",
        )
        .await;

    // ── Redis (4 connection tiers in 1 endpoint) ──
    let redis_read_info = parse_redis_url(&redis_read_url);
    let redis_write_info = parse_redis_url(&redis_write_url);
    let redis_admin_info = parse_redis_url(&redis_admin_url);
    let redis_system_info = parse_redis_url(&redis_system_url);
    let redis_read = json!({"host": redis_read_info.host, "port": redis_read_info.port,
        "tls": if redis_read_info.tls { json!(true) } else { json!(null) },
        "username": redis_read_info.username.as_deref().unwrap_or("reader"),
        "password": redis_read_info.password});
    let redis_write = json!({"host": redis_write_info.host, "port": redis_write_info.port,
        "tls": if redis_write_info.tls { json!(true) } else { json!(null) },
        "username": redis_write_info.username.as_deref().unwrap_or("writer"),
        "password": redis_write_info.password});
    let redis_admin = json!({"host": redis_admin_info.host, "port": redis_admin_info.port,
        "tls": if redis_admin_info.tls { json!(true) } else { json!(null) },
        "username": redis_admin_info.username.as_deref().unwrap_or("default"),
        "password": redis_admin_info.password});
    let redis_system = json!({"host": redis_system_info.host, "port": redis_system_info.port,
        "tls": if redis_system_info.tls { json!(true) } else { json!(null) },
        "username": redis_system_info.username.as_deref().unwrap_or("default"),
        "password": redis_system_info.password});
    let redis_ep = eden.create_endpoint("adam_redis", "redis",
        json!({"read_conn": redis_read, "write_conn": redis_write, "admin_conn": redis_admin, "system_conn": redis_system}),
        "Offers domain — real-time engagement cache and leaderboards").await;

    let clickhouse_disabled = clickhouse_endpoints_disabled();
    if clickhouse_disabled {
        warn!("ClickHouse endpoint registration disabled for retail");
    }
    let ch_ep = if clickhouse_disabled {
        None
    } else {
        let ch_read_info = parse_clickhouse_url(&ch_read_url);
        let ch_write_info = parse_clickhouse_url(&ch_write_url);
        let ch_admin_info = parse_clickhouse_url(&ch_admin_url);
        let ch_system_info = parse_clickhouse_url(&ch_system_url);
        let ch_read = json!({"url": ch_read_info.0, "user": ch_read_info.1, "password": ch_read_info.2, "database": ch_read_info.3});
        let ch_write = json!({"url": ch_write_info.0, "user": ch_write_info.1, "password": ch_write_info.2, "database": ch_write_info.3});
        let ch_admin = json!({"url": ch_admin_info.0, "user": ch_admin_info.1, "password": ch_admin_info.2, "database": ch_admin_info.3});
        let ch_system = json!({"url": ch_system_info.0, "user": ch_system_info.1, "password": ch_system_info.2, "database": ch_system_info.3});
        Some(
            eden.create_endpoint("adam_clickhouse", "clickhouse",
                json!({"read_conn": ch_read, "write_conn": ch_write, "admin_conn": ch_admin, "system_conn": ch_system}),
                "Marketplace OLAP — analytics on marketplace events").await,
        )
    };

    // ── Weaviate (no RBAC, same connection for all tiers) ──
    let weav_conn = json!({ "url": eden_weaviate_url, "token": "" });
    let weav_ep = eden.create_endpoint("adam_weaviate", "weaviate",
        json!({"read_conn": weav_conn.clone(), "write_conn": weav_conn.clone(), "admin_conn": weav_conn.clone(), "system_conn": weav_conn}),
        "Reviews domain — vector search on review embeddings (T-ECD)").await;

    // Distribute QPS: PG 30%, Mongo 20%, Redis 25%, CH 15%, Weaviate 10%
    let total_qps = config.queries_per_second;

    let mut silos = vec![
        EndpointDef {
            silo_name: "postgres".into(),
            endpoint_id: ep_id(&pg_ep, "adam_postgres"),
            metrics_label: "postgres".into(),
            qps: total_qps * if clickhouse_disabled { 45 } else { 30 } / 100,
        },
        EndpointDef {
            silo_name: "mongodb".into(),
            endpoint_id: ep_id(&mongo_ep, "adam_mongodb"),
            metrics_label: "mongodb".into(),
            qps: total_qps * 20 / 100,
        },
        EndpointDef {
            silo_name: "redis".into(),
            endpoint_id: ep_id(&redis_ep, "adam_redis"),
            metrics_label: "redis".into(),
            qps: total_qps * 25 / 100,
        },
        EndpointDef {
            silo_name: "weaviate".into(),
            endpoint_id: ep_id(&weav_ep, "adam_weaviate"),
            metrics_label: "weaviate".into(),
            qps: total_qps * 10 / 100,
        },
    ];
    if let Some(ch_ep) = ch_ep {
        silos.insert(
            3,
            EndpointDef {
                silo_name: "clickhouse".into(),
                endpoint_id: ep_id(&ch_ep, "adam_clickhouse"),
                metrics_label: "clickhouse".into(),
                qps: total_qps * 15 / 100,
            },
        );
    }

    // Register optional external service endpoints
    register_external_endpoints(eden, config).await;

    Endpoints {
        vertical: "retail".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Stonebreaker (5-source retail benchmark)
// ═══════════════════════════════════════════════════════════════

async fn register_stonebreaker_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering stonebreaker benchmark endpoints with Eden...");

    let eden_pg_url = config
        .eden_postgres_url
        .as_deref()
        .unwrap_or(&config.postgres_url);
    let eden_mongo_url = config
        .eden_mongo_url
        .as_deref()
        .unwrap_or(&config.mongo_url);
    let eden_redis_url = config
        .eden_redis_url
        .as_deref()
        .unwrap_or(&config.redis_url);
    let eden_ch_url = config
        .eden_clickhouse_url
        .as_deref()
        .unwrap_or(&config.clickhouse_url);
    let eden_weaviate_url = config
        .eden_weaviate_url
        .as_deref()
        .unwrap_or(&config.weaviate_url);
    let (pg_read_url, pg_write_url, pg_admin_url, pg_system_url) = access_tier_urls(
        &["EDEN_PG_READ_URL", "PG_READ_URL"],
        &["EDEN_PG_WRITE_URL", "PG_WRITE_URL"],
        &["EDEN_POSTGRES_URL", "POSTGRES_URL"],
        eden_pg_url.to_string(),
    );
    let (mongo_read_url, mongo_write_url, mongo_admin_url, mongo_system_url) = access_tier_urls(
        &["EDEN_MONGO_READ_URL", "MONGO_READ_URL"],
        &["EDEN_MONGO_WRITE_URL", "MONGO_WRITE_URL"],
        &["EDEN_MONGO_URL", "MONGO_URL"],
        eden_mongo_url.to_string(),
    );
    let (redis_read_url, redis_write_url, redis_admin_url, redis_system_url) = access_tier_urls(
        &["EDEN_REDIS_READ_URL", "REDIS_READ_URL"],
        &["EDEN_REDIS_WRITE_URL", "REDIS_WRITE_URL"],
        &["EDEN_REDIS_URL", "REDIS_URL"],
        eden_redis_url.to_string(),
    );
    let (ch_read_url, ch_write_url, ch_admin_url, ch_system_url) = access_tier_urls(
        &["EDEN_CLICKHOUSE_READ_URL", "CLICKHOUSE_READ_URL"],
        &["EDEN_CLICKHOUSE_WRITE_URL", "CLICKHOUSE_WRITE_URL"],
        &["EDEN_CLICKHOUSE_URL", "CLICKHOUSE_URL"],
        eden_ch_url.to_string(),
    );

    let pg_ep = eden
        .create_endpoint(
            "stone_pg_marketplace",
            "postgres",
            postgres_endpoint_config(&pg_read_url, &pg_write_url, &pg_admin_url, &pg_system_url),
            "Stonebreaker benchmark — marketplace OLTP evidence source",
        )
        .await;

    let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "ecommerce".to_string());
    let mongo_ep = eden
        .create_endpoint(
            "stone_mongo_catalog",
            "mongo",
            mongo_endpoint_config(
                &mongo_read_url,
                &mongo_write_url,
                &mongo_admin_url,
                &mongo_system_url,
                &mongo_db,
            ),
            "Stonebreaker benchmark — retail catalog document evidence source",
        )
        .await;

    let redis_read_info = parse_redis_url(&redis_read_url);
    let redis_write_info = parse_redis_url(&redis_write_url);
    let redis_admin_info = parse_redis_url(&redis_admin_url);
    let redis_system_info = parse_redis_url(&redis_system_url);
    let redis_read = json!({"host": redis_read_info.host, "port": redis_read_info.port,
        "tls": if redis_read_info.tls { json!(true) } else { json!(null) },
        "username": redis_read_info.username.as_deref().unwrap_or("reader"),
        "password": redis_read_info.password});
    let redis_write = json!({"host": redis_write_info.host, "port": redis_write_info.port,
        "tls": if redis_write_info.tls { json!(true) } else { json!(null) },
        "username": redis_write_info.username.as_deref().unwrap_or("writer"),
        "password": redis_write_info.password});
    let redis_admin = json!({"host": redis_admin_info.host, "port": redis_admin_info.port,
        "tls": if redis_admin_info.tls { json!(true) } else { json!(null) },
        "username": redis_admin_info.username.as_deref().unwrap_or("default"),
        "password": redis_admin_info.password});
    let redis_system = json!({"host": redis_system_info.host, "port": redis_system_info.port,
        "tls": if redis_system_info.tls { json!(true) } else { json!(null) },
        "username": redis_system_info.username.as_deref().unwrap_or("default"),
        "password": redis_system_info.password});
    let redis_ep = eden.create_endpoint("stone_redis_offers", "redis",
        json!({"read_conn": redis_read, "write_conn": redis_write, "admin_conn": redis_admin, "system_conn": redis_system}),
        "Stonebreaker benchmark — real-time leaderboard evidence source").await;

    let ch_read_info = parse_clickhouse_url(&ch_read_url);
    let ch_write_info = parse_clickhouse_url(&ch_write_url);
    let ch_admin_info = parse_clickhouse_url(&ch_admin_url);
    let ch_system_info = parse_clickhouse_url(&ch_system_url);
    let ch_read = json!({"url": ch_read_info.0, "user": ch_read_info.1, "password": ch_read_info.2, "database": ch_read_info.3});
    let ch_write = json!({"url": ch_write_info.0, "user": ch_write_info.1, "password": ch_write_info.2, "database": ch_write_info.3});
    let ch_admin = json!({"url": ch_admin_info.0, "user": ch_admin_info.1, "password": ch_admin_info.2, "database": ch_admin_info.3});
    let ch_system = json!({"url": ch_system_info.0, "user": ch_system_info.1, "password": ch_system_info.2, "database": ch_system_info.3});
    let ch_ep = eden
        .create_endpoint(
            "stone_clickhouse_analytics",
            "clickhouse",
            json!({"read_conn": ch_read, "write_conn": ch_write, "admin_conn": ch_admin, "system_conn": ch_system}),
            "Stonebreaker benchmark — analytics evidence source",
        )
        .await;

    let weav_conn = json!({ "url": eden_weaviate_url, "token": "" });
    let weav_ep = eden.create_endpoint("stone_weaviate_reviews", "weaviate",
        json!({"read_conn": weav_conn.clone(), "write_conn": weav_conn.clone(), "admin_conn": weav_conn.clone(), "system_conn": weav_conn}),
        "Stonebreaker benchmark — vector review evidence source").await;

    if !config.stonebreaker_localfs_root.is_empty() {
        let localfs_ep = eden
            .create_endpoint(
                "stone_localfs_docs",
                "localfs",
                localfs_endpoint_config(&config.stonebreaker_localfs_root),
                "Stonebreaker auxiliary local filesystem corpus of raw benchmark documents",
            )
            .await;
        info!(
            "Stonebreaker auxiliary localfs endpoint: {}",
            ep_id(&localfs_ep, "stone_localfs_docs")
        );
    }

    let total_qps = config.queries_per_second;
    let silos = vec![
        EndpointDef {
            silo_name: "postgres".into(),
            endpoint_id: ep_id(&pg_ep, "stone_pg_marketplace"),
            metrics_label: "stone_pg".into(),
            qps: total_qps / 5,
        },
        EndpointDef {
            silo_name: "mongodb".into(),
            endpoint_id: ep_id(&mongo_ep, "stone_mongo_catalog"),
            metrics_label: "stone_mongo".into(),
            qps: total_qps / 5,
        },
        EndpointDef {
            silo_name: "redis".into(),
            endpoint_id: ep_id(&redis_ep, "stone_redis_offers"),
            metrics_label: "stone_redis".into(),
            qps: total_qps / 5,
        },
        EndpointDef {
            silo_name: "clickhouse".into(),
            endpoint_id: ep_id(&ch_ep, "stone_clickhouse_analytics"),
            metrics_label: "stone_clickhouse".into(),
            qps: total_qps / 5,
        },
        EndpointDef {
            silo_name: "weaviate".into(),
            endpoint_id: ep_id(&weav_ep, "stone_weaviate_reviews"),
            metrics_label: "stone_weaviate".into(),
            qps: total_qps / 5,
        },
    ];

    register_external_endpoints(eden, config).await;

    Endpoints {
        vertical: "stonebreaker".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Tech (2x Postgres + ClickHouse + Mongo + Redis + Weaviate)
// ═══════════════════════════════════════════════════════════════

async fn register_tech_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering tech/SaaS database endpoints with Eden...");

    // Read silo-specific URLs from env (set by docker-compose.tech.yml)
    let pg_netsec_url = std::env::var("EDEN_PG_NETWORK_SECURITY_URL")
        .or_else(|_| std::env::var("PG_NETWORK_SECURITY_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let pg_billing_url = std::env::var("EDEN_PG_SAAS_BILLING_URL")
        .or_else(|_| std::env::var("PG_SAAS_BILLING_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let ch_url = config
        .eden_clickhouse_url
        .as_deref()
        .unwrap_or(&config.clickhouse_url);
    let mongo_url = config
        .eden_mongo_url
        .as_deref()
        .unwrap_or(&config.mongo_url);
    let redis_url = config
        .eden_redis_url
        .as_deref()
        .unwrap_or(&config.redis_url);
    let weaviate_url = config
        .eden_weaviate_url
        .as_deref()
        .unwrap_or(&config.weaviate_url);
    let (pg1_read_url, pg1_write_url, pg1_admin_url, pg1_system_url) = access_tier_urls(
        &[
            "EDEN_PG_NETWORK_SECURITY_READ_URL",
            "PG_NETWORK_SECURITY_READ_URL",
        ],
        &[
            "EDEN_PG_NETWORK_SECURITY_WRITE_URL",
            "PG_NETWORK_SECURITY_WRITE_URL",
        ],
        &["EDEN_PG_NETWORK_SECURITY_URL", "PG_NETWORK_SECURITY_URL"],
        pg_netsec_url.clone(),
    );
    let (pg2_read_url, pg2_write_url, pg2_admin_url, pg2_system_url) = access_tier_urls(
        &["EDEN_PG_SAAS_BILLING_READ_URL", "PG_SAAS_BILLING_READ_URL"],
        &[
            "EDEN_PG_SAAS_BILLING_WRITE_URL",
            "PG_SAAS_BILLING_WRITE_URL",
        ],
        &["EDEN_PG_SAAS_BILLING_URL", "PG_SAAS_BILLING_URL"],
        pg_billing_url.clone(),
    );
    let (mongo_read_url, mongo_write_url, mongo_admin_url, mongo_system_url) = access_tier_urls(
        &["EDEN_MONGO_READ_URL", "MONGO_READ_URL"],
        &["EDEN_MONGO_WRITE_URL", "MONGO_WRITE_URL"],
        &["EDEN_MONGO_URL", "MONGO_URL"],
        mongo_url.to_string(),
    );
    let (redis_read_url, redis_write_url, redis_admin_url, redis_system_url) = access_tier_urls(
        &["EDEN_REDIS_READ_URL", "REDIS_READ_URL"],
        &["EDEN_REDIS_WRITE_URL", "REDIS_WRITE_URL"],
        &["EDEN_REDIS_URL", "REDIS_URL"],
        redis_url.to_string(),
    );
    let (ch_read_url, ch_write_url, ch_admin_url, ch_system_url) = access_tier_urls(
        &["EDEN_CLICKHOUSE_READ_URL", "CLICKHOUSE_READ_URL"],
        &["EDEN_CLICKHOUSE_WRITE_URL", "CLICKHOUSE_WRITE_URL"],
        &["EDEN_CLICKHOUSE_URL", "CLICKHOUSE_URL"],
        ch_url.to_string(),
    );

    // ── Postgres #1: Network Security (4 connection tiers) ──
    let pg1_ep = eden
        .create_endpoint(
            "tech_network_security",
            "postgres",
            postgres_endpoint_config(
                &pg1_read_url,
                &pg1_write_url,
                &pg1_admin_url,
                &pg1_system_url,
            ),
            "SecOps — Network intrusion detection (UNSW-NB15, 2.5M flows)",
        )
        .await;

    // ── Postgres #2: SaaS Billing (4 connection tiers) ──
    let pg2_ep = eden
        .create_endpoint(
            "tech_saas_billing",
            "postgres",
            postgres_endpoint_config(
                &pg2_read_url,
                &pg2_write_url,
                &pg2_admin_url,
                &pg2_system_url,
            ),
            "Finance — SaaS subscriptions, invoices, API usage",
        )
        .await;

    let clickhouse_disabled = clickhouse_endpoints_disabled();
    if clickhouse_disabled {
        warn!("ClickHouse endpoint registration disabled for tech");
    }
    let ch_ep = if clickhouse_disabled {
        None
    } else {
        let ch_read_info = parse_clickhouse_url(&ch_read_url);
        let ch_write_info = parse_clickhouse_url(&ch_write_url);
        let ch_admin_info = parse_clickhouse_url(&ch_admin_url);
        let ch_system_info = parse_clickhouse_url(&ch_system_url);
        let ch_read = json!({"url": ch_read_info.0, "user": ch_read_info.1, "password": ch_read_info.2, "database": ch_read_info.3});
        let ch_write = json!({"url": ch_write_info.0, "user": ch_write_info.1, "password": ch_write_info.2, "database": ch_write_info.3});
        let ch_admin = json!({"url": ch_admin_info.0, "user": ch_admin_info.1, "password": ch_admin_info.2, "database": ch_admin_info.3});
        let ch_system = json!({"url": ch_system_info.0, "user": ch_system_info.1, "password": ch_system_info.2, "database": ch_system_info.3});
        Some(
            eden.create_endpoint("tech_user_events", "clickhouse",
                json!({"read_conn": ch_read, "write_conn": ch_write, "admin_conn": ch_admin, "system_conn": ch_system}),
                "Product Analytics — User behavior events").await,
        )
    };

    // ── MongoDB: CVE Vulnerabilities (4 connection tiers) ──
    let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "issues".to_string());
    let mongo_ep = eden
        .create_endpoint(
            "tech_cve",
            "mongo",
            mongo_endpoint_config(
                &mongo_read_url,
                &mongo_write_url,
                &mongo_admin_url,
                &mongo_system_url,
                &mongo_db,
            ),
            "CVE vulnerability database",
        )
        .await;

    // ── Redis: Sessions (4 connection tiers) ──
    let redis_read_info = parse_redis_url(&redis_read_url);
    let redis_write_info = parse_redis_url(&redis_write_url);
    let redis_admin_info = parse_redis_url(&redis_admin_url);
    let redis_system_info = parse_redis_url(&redis_system_url);
    let redis_read = json!({"host": redis_read_info.host, "port": redis_read_info.port,
        "tls": if redis_read_info.tls { json!(true) } else { json!(null) },
        "username": redis_read_info.username.as_deref().unwrap_or("reader"),
        "password": redis_read_info.password});
    let redis_write = json!({"host": redis_write_info.host, "port": redis_write_info.port,
        "tls": if redis_write_info.tls { json!(true) } else { json!(null) },
        "username": redis_write_info.username.as_deref().unwrap_or("writer"),
        "password": redis_write_info.password});
    let redis_admin = json!({"host": redis_admin_info.host, "port": redis_admin_info.port,
        "tls": if redis_admin_info.tls { json!(true) } else { json!(null) },
        "username": redis_admin_info.username.as_deref().unwrap_or("default"),
        "password": redis_admin_info.password});
    let redis_system = json!({"host": redis_system_info.host, "port": redis_system_info.port,
        "tls": if redis_system_info.tls { json!(true) } else { json!(null) },
        "username": redis_system_info.username.as_deref().unwrap_or("default"),
        "password": redis_system_info.password});
    let redis_ep = eden.create_endpoint("tech_sessions", "redis",
        json!({"read_conn": redis_read, "write_conn": redis_write, "admin_conn": redis_admin, "system_conn": redis_system}),
        "Sessions and rate limits").await;

    // ── Weaviate: Vulnerability Search (no RBAC, same connection for all) ──
    let weav_conn = json!({ "url": weaviate_url, "token": "" });
    let weav_ep = eden.create_endpoint("tech_logs", "weaviate",
        json!({"read_conn": weav_conn.clone(), "write_conn": weav_conn.clone(), "admin_conn": weav_conn.clone(), "system_conn": weav_conn}),
        "Semantic search").await;

    // Distribute QPS across 6 silos
    let total_qps = config.queries_per_second;
    let mut silos = vec![
        EndpointDef {
            silo_name: "pg_network_security".into(),
            endpoint_id: ep_id(&pg1_ep, "tech_network_security"),
            metrics_label: "pg_netsec".into(),
            qps: total_qps * if clickhouse_disabled { 45 } else { 20 } / 100,
        },
        EndpointDef {
            silo_name: "pg_saas_billing".into(),
            endpoint_id: ep_id(&pg2_ep, "tech_saas_billing"),
            metrics_label: "pg_billing".into(),
            qps: total_qps * 20 / 100,
        },
        EndpointDef {
            silo_name: "mongo_cve".into(),
            endpoint_id: ep_id(&mongo_ep, "tech_cve"),
            metrics_label: "mongo_cve".into(),
            qps: total_qps * 15 / 100,
        },
        EndpointDef {
            silo_name: "redis_sessions".into(),
            endpoint_id: ep_id(&redis_ep, "tech_sessions"),
            metrics_label: "redis_sessions".into(),
            qps: total_qps * 10 / 100,
        },
        EndpointDef {
            silo_name: "weaviate_logs".into(),
            endpoint_id: ep_id(&weav_ep, "tech_logs"),
            metrics_label: "weaviate_logs".into(),
            qps: total_qps * 10 / 100,
        },
    ];
    if let Some(ch_ep) = ch_ep {
        silos.insert(
            2,
            EndpointDef {
                silo_name: "ch_user_events".into(),
                endpoint_id: ep_id(&ch_ep, "tech_user_events"),
                metrics_label: "ch_events".into(),
                qps: total_qps * 25 / 100,
            },
        );
    }

    register_external_endpoints(eden, config).await;

    Endpoints {
        vertical: "tech".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Finance (2x Postgres + ClickHouse + Mongo + Redis + Weaviate)
// ═══════════════════════════════════════════════════════════════

async fn register_finance_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering finance/banking database endpoints with Eden...");

    let pg_core_url = std::env::var("EDEN_PG_CORE_BANKING_URL")
        .or_else(|_| std::env::var("PG_CORE_BANKING_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let pg_credit_url = std::env::var("EDEN_PG_CREDIT_SCORING_URL")
        .or_else(|_| std::env::var("PG_CREDIT_SCORING_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let ch_url = config
        .eden_clickhouse_url
        .as_deref()
        .unwrap_or(&config.clickhouse_url);
    let mongo_url = config
        .eden_mongo_url
        .as_deref()
        .unwrap_or(&config.mongo_url);
    let redis_url = config
        .eden_redis_url
        .as_deref()
        .unwrap_or(&config.redis_url);
    let weaviate_url = config
        .eden_weaviate_url
        .as_deref()
        .unwrap_or(&config.weaviate_url);
    let (pg1_read_url, pg1_write_url, pg1_admin_url, pg1_system_url) = access_tier_urls(
        &["EDEN_PG_CORE_BANKING_READ_URL", "PG_CORE_BANKING_READ_URL"],
        &[
            "EDEN_PG_CORE_BANKING_WRITE_URL",
            "PG_CORE_BANKING_WRITE_URL",
        ],
        &["EDEN_PG_CORE_BANKING_URL", "PG_CORE_BANKING_URL"],
        pg_core_url.clone(),
    );
    let (pg2_read_url, pg2_write_url, pg2_admin_url, pg2_system_url) = access_tier_urls(
        &[
            "EDEN_PG_CREDIT_SCORING_READ_URL",
            "PG_CREDIT_SCORING_READ_URL",
        ],
        &[
            "EDEN_PG_CREDIT_SCORING_WRITE_URL",
            "PG_CREDIT_SCORING_WRITE_URL",
        ],
        &["EDEN_PG_CREDIT_SCORING_URL", "PG_CREDIT_SCORING_URL"],
        pg_credit_url.clone(),
    );
    let (mongo_read_url, mongo_write_url, mongo_admin_url, mongo_system_url) = access_tier_urls(
        &["EDEN_MONGO_READ_URL", "MONGO_READ_URL"],
        &["EDEN_MONGO_WRITE_URL", "MONGO_WRITE_URL"],
        &["EDEN_MONGO_URL", "MONGO_URL"],
        mongo_url.to_string(),
    );
    let (redis_read_url, redis_write_url, redis_admin_url, redis_system_url) = access_tier_urls(
        &["EDEN_REDIS_READ_URL", "REDIS_READ_URL"],
        &["EDEN_REDIS_WRITE_URL", "REDIS_WRITE_URL"],
        &["EDEN_REDIS_URL", "REDIS_URL"],
        redis_url.to_string(),
    );
    let (ch_read_url, ch_write_url, ch_admin_url, ch_system_url) = access_tier_urls(
        &["EDEN_CLICKHOUSE_READ_URL", "CLICKHOUSE_READ_URL"],
        &["EDEN_CLICKHOUSE_WRITE_URL", "CLICKHOUSE_WRITE_URL"],
        &["EDEN_CLICKHOUSE_URL", "CLICKHOUSE_URL"],
        ch_url.to_string(),
    );

    // ── Postgres #1: Core Banking (4 connection tiers) ──
    let pg1_ep = eden
        .create_endpoint(
            "fin_core_banking",
            "postgres",
            postgres_endpoint_config(
                &pg1_read_url,
                &pg1_write_url,
                &pg1_admin_url,
                &pg1_system_url,
            ),
            "Core Banking — Fraud detection transactions",
        )
        .await;

    // ── Postgres #2: Credit Scoring (4 connection tiers) ──
    let pg2_ep = eden
        .create_endpoint(
            "fin_credit_scoring",
            "postgres",
            postgres_endpoint_config(
                &pg2_read_url,
                &pg2_write_url,
                &pg2_admin_url,
                &pg2_system_url,
            ),
            "Credit Dept — Credit card transactions",
        )
        .await;

    let clickhouse_disabled = clickhouse_endpoints_disabled();
    if clickhouse_disabled {
        warn!("ClickHouse endpoint registration disabled for finance");
    }
    let ch_ep = if clickhouse_disabled {
        None
    } else {
        let ch_read_info = parse_clickhouse_url(&ch_read_url);
        let ch_write_info = parse_clickhouse_url(&ch_write_url);
        let ch_admin_info = parse_clickhouse_url(&ch_admin_url);
        let ch_system_info = parse_clickhouse_url(&ch_system_url);
        let ch_read = json!({"url": ch_read_info.0, "user": ch_read_info.1, "password": ch_read_info.2, "database": ch_read_info.3});
        let ch_write = json!({"url": ch_write_info.0, "user": ch_write_info.1, "password": ch_write_info.2, "database": ch_write_info.3});
        let ch_admin = json!({"url": ch_admin_info.0, "user": ch_admin_info.1, "password": ch_admin_info.2, "database": ch_admin_info.3});
        let ch_system = json!({"url": ch_system_info.0, "user": ch_system_info.1, "password": ch_system_info.2, "database": ch_system_info.3});
        Some(
            eden.create_endpoint("fin_trading", "clickhouse",
                json!({"read_conn": ch_read, "write_conn": ch_write, "admin_conn": ch_admin, "system_conn": ch_system}),
                "Trading Desk — S&P 500 1-minute stock bars").await,
        )
    };

    // ── MongoDB: Compliance (4 connection tiers) ──
    let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "compliance".to_string());
    let mongo_ep = eden
        .create_endpoint(
            "fin_compliance",
            "mongo",
            mongo_endpoint_config(
                &mongo_read_url,
                &mongo_write_url,
                &mongo_admin_url,
                &mongo_system_url,
                &mongo_db,
            ),
            "Compliance — SEC 10-K annual filings",
        )
        .await;

    // ── Redis: Fraud (4 connection tiers) ──
    let redis_read_info = parse_redis_url(&redis_read_url);
    let redis_write_info = parse_redis_url(&redis_write_url);
    let redis_admin_info = parse_redis_url(&redis_admin_url);
    let redis_system_info = parse_redis_url(&redis_system_url);
    let redis_read = json!({"host": redis_read_info.host, "port": redis_read_info.port,
        "tls": if redis_read_info.tls { json!(true) } else { json!(null) },
        "username": redis_read_info.username.as_deref().unwrap_or("reader"),
        "password": redis_read_info.password});
    let redis_write = json!({"host": redis_write_info.host, "port": redis_write_info.port,
        "tls": if redis_write_info.tls { json!(true) } else { json!(null) },
        "username": redis_write_info.username.as_deref().unwrap_or("writer"),
        "password": redis_write_info.password});
    let redis_admin = json!({"host": redis_admin_info.host, "port": redis_admin_info.port,
        "tls": if redis_admin_info.tls { json!(true) } else { json!(null) },
        "username": redis_admin_info.username.as_deref().unwrap_or("default"),
        "password": redis_admin_info.password});
    let redis_system = json!({"host": redis_system_info.host, "port": redis_system_info.port,
        "tls": if redis_system_info.tls { json!(true) } else { json!(null) },
        "username": redis_system_info.username.as_deref().unwrap_or("default"),
        "password": redis_system_info.password});
    let redis_ep = eden.create_endpoint("fin_fraud", "redis",
        json!({"read_conn": redis_read, "write_conn": redis_write, "admin_conn": redis_admin, "system_conn": redis_system}),
        "Real-time — Fraud scores, account balances, rate limits").await;

    // ── Weaviate: Risk (no RBAC, same connection for all) ──
    let weav_conn = json!({ "url": weaviate_url, "token": "" });
    let weav_ep = eden.create_endpoint("fin_risk", "weaviate",
        json!({"read_conn": weav_conn.clone(), "write_conn": weav_conn.clone(), "admin_conn": weav_conn.clone(), "system_conn": weav_conn}),
        "Risk — Semantic search on SEC filings and transactions").await;

    let total_qps = config.queries_per_second;
    let mut silos = vec![
        EndpointDef {
            silo_name: "pg_core_banking".into(),
            endpoint_id: ep_id(&pg1_ep, "fin_core_banking"),
            metrics_label: "pg_core".into(),
            qps: total_qps * if clickhouse_disabled { 45 } else { 25 } / 100,
        },
        EndpointDef {
            silo_name: "pg_credit_scoring".into(),
            endpoint_id: ep_id(&pg2_ep, "fin_credit_scoring"),
            metrics_label: "pg_credit".into(),
            qps: total_qps * 20 / 100,
        },
        EndpointDef {
            silo_name: "mongo_compliance".into(),
            endpoint_id: ep_id(&mongo_ep, "fin_compliance"),
            metrics_label: "mongo_compliance".into(),
            qps: total_qps * 15 / 100,
        },
        EndpointDef {
            silo_name: "redis_fraud".into(),
            endpoint_id: ep_id(&redis_ep, "fin_fraud"),
            metrics_label: "redis_fraud".into(),
            qps: total_qps * 10 / 100,
        },
        EndpointDef {
            silo_name: "weaviate_risk".into(),
            endpoint_id: ep_id(&weav_ep, "fin_risk"),
            metrics_label: "weaviate_risk".into(),
            qps: total_qps * 10 / 100,
        },
    ];
    if let Some(ch_ep) = ch_ep {
        silos.insert(
            2,
            EndpointDef {
                silo_name: "ch_trading".into(),
                endpoint_id: ep_id(&ch_ep, "fin_trading"),
                metrics_label: "ch_trading".into(),
                qps: total_qps * 20 / 100,
            },
        );
    }

    register_external_endpoints(eden, config).await;
    Endpoints {
        vertical: "finance".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Healthcare (2x Postgres + 2x Mongo + ClickHouse + Redis + Weaviate)
// ═══════════════════════════════════════════════════════════════

async fn register_healthcare_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering healthcare/clinical database endpoints with Eden...");

    let pg_patients_url = std::env::var("EDEN_PG_PATIENT_RECORDS_URL")
        .or_else(|_| std::env::var("PG_PATIENT_RECORDS_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let pg_billing_url = std::env::var("EDEN_PG_BILLING_URL")
        .or_else(|_| std::env::var("PG_BILLING_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let pg_cms_url = std::env::var("EDEN_PG_CMS_CLAIMS_URL")
        .or_else(|_| std::env::var("PG_CMS_CLAIMS_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let mongo_clinical_url = std::env::var("EDEN_MONGO_CLINICAL_URL")
        .or_else(|_| std::env::var("MONGO_CLINICAL_URL"))
        .unwrap_or_else(|_| config.mongo_url.clone());
    let mongo_lab_url = std::env::var("EDEN_MONGO_LAB_URL")
        .or_else(|_| std::env::var("MONGO_LAB_URL"))
        .unwrap_or_else(|_| config.mongo_url.clone());
    let ch_url = config
        .eden_clickhouse_url
        .as_deref()
        .unwrap_or(&config.clickhouse_url);
    let redis_url = config
        .eden_redis_url
        .as_deref()
        .unwrap_or(&config.redis_url);
    let weaviate_url = config
        .eden_weaviate_url
        .as_deref()
        .unwrap_or(&config.weaviate_url);
    let (pg1_read_url, pg1_write_url, pg1_admin_url, pg1_system_url) = access_tier_urls(
        &[
            "EDEN_PG_PATIENT_RECORDS_READ_URL",
            "PG_PATIENT_RECORDS_READ_URL",
        ],
        &[
            "EDEN_PG_PATIENT_RECORDS_WRITE_URL",
            "PG_PATIENT_RECORDS_WRITE_URL",
        ],
        &["EDEN_PG_PATIENT_RECORDS_URL", "PG_PATIENT_RECORDS_URL"],
        pg_patients_url.clone(),
    );
    let (pg2_read_url, pg2_write_url, pg2_admin_url, pg2_system_url) = access_tier_urls(
        &["EDEN_PG_BILLING_READ_URL", "PG_BILLING_READ_URL"],
        &["EDEN_PG_BILLING_WRITE_URL", "PG_BILLING_WRITE_URL"],
        &["EDEN_PG_BILLING_URL", "PG_BILLING_URL"],
        pg_billing_url.clone(),
    );
    let (pg3_read_url, pg3_write_url, pg3_admin_url, pg3_system_url) = access_tier_urls(
        &["EDEN_PG_CMS_CLAIMS_READ_URL", "PG_CMS_CLAIMS_READ_URL"],
        &["EDEN_PG_CMS_CLAIMS_WRITE_URL", "PG_CMS_CLAIMS_WRITE_URL"],
        &["EDEN_PG_CMS_CLAIMS_URL", "PG_CMS_CLAIMS_URL"],
        pg_cms_url.clone(),
    );
    let (
        mongo_clinical_read_url,
        mongo_clinical_write_url,
        mongo_clinical_admin_url,
        mongo_clinical_system_url,
    ) = access_tier_urls(
        &["EDEN_MONGO_CLINICAL_READ_URL", "MONGO_CLINICAL_READ_URL"],
        &["EDEN_MONGO_CLINICAL_WRITE_URL", "MONGO_CLINICAL_WRITE_URL"],
        &["EDEN_MONGO_CLINICAL_URL", "MONGO_CLINICAL_URL"],
        mongo_clinical_url.clone(),
    );
    let (mongo_lab_read_url, mongo_lab_write_url, mongo_lab_admin_url, mongo_lab_system_url) =
        access_tier_urls(
            &["EDEN_MONGO_LAB_READ_URL", "MONGO_LAB_READ_URL"],
            &["EDEN_MONGO_LAB_WRITE_URL", "MONGO_LAB_WRITE_URL"],
            &["EDEN_MONGO_LAB_URL", "MONGO_LAB_URL"],
            mongo_lab_url.clone(),
        );
    let (redis_read_url, redis_write_url, redis_admin_url, redis_system_url) = access_tier_urls(
        &["EDEN_REDIS_READ_URL", "REDIS_READ_URL"],
        &["EDEN_REDIS_WRITE_URL", "REDIS_WRITE_URL"],
        &["EDEN_REDIS_URL", "REDIS_URL"],
        redis_url.to_string(),
    );
    let (ch_read_url, ch_write_url, ch_admin_url, ch_system_url) = access_tier_urls(
        &["EDEN_CLICKHOUSE_READ_URL", "CLICKHOUSE_READ_URL"],
        &["EDEN_CLICKHOUSE_WRITE_URL", "CLICKHOUSE_WRITE_URL"],
        &["EDEN_CLICKHOUSE_URL", "CLICKHOUSE_URL"],
        ch_url.to_string(),
    );

    // ── Postgres #1: Patient Records (4 connection tiers) ──
    let pg1_ep = eden
        .create_endpoint(
            "hc_patient_records",
            "postgres",
            postgres_endpoint_config(
                &pg1_read_url,
                &pg1_write_url,
                &pg1_admin_url,
                &pg1_system_url,
            ),
            "EHR — Patient demographics & encounters",
        )
        .await;

    // ── Postgres #2: Billing (4 connection tiers) ──
    let pg2_ep = eden
        .create_endpoint(
            "hc_billing",
            "postgres",
            postgres_endpoint_config(
                &pg2_read_url,
                &pg2_write_url,
                &pg2_admin_url,
                &pg2_system_url,
            ),
            "Billing — Insurance claims, payer coverage, costs",
        )
        .await;

    // ── Postgres #3: CMS Medicare Claims (4 connection tiers) ──
    let pg3_ep = eden
        .create_endpoint(
            "hc_cms_claims",
            "postgres",
            postgres_endpoint_config(
                &pg3_read_url,
                &pg3_write_url,
                &pg3_admin_url,
                &pg3_system_url,
            ),
            "CMS Medicare Claims — Legacy claims warehouse (DE-SynPUF)",
        )
        .await;

    // ── MongoDB #1: Clinical Docs (4 connection tiers) ──
    let mongo_clinical_db =
        std::env::var("MONGO_CLINICAL_DB").unwrap_or_else(|_| "clinical".to_string());
    let m1_ep = eden
        .create_endpoint(
            "hc_clinical_docs",
            "mongo",
            mongo_endpoint_config(
                &mongo_clinical_read_url,
                &mongo_clinical_write_url,
                &mongo_clinical_admin_url,
                &mongo_clinical_system_url,
                &mongo_clinical_db,
            ),
            "Clinical Docs — Conditions, procedures, medications",
        )
        .await;

    // ── MongoDB #2: Lab Results (4 connection tiers) ──
    let mongo_lab_db = std::env::var("MONGO_LAB_DB").unwrap_or_else(|_| "laboratory".to_string());
    let m2_ep = eden
        .create_endpoint(
            "hc_lab_results",
            "mongo",
            mongo_endpoint_config(
                &mongo_lab_read_url,
                &mongo_lab_write_url,
                &mongo_lab_admin_url,
                &mongo_lab_system_url,
                &mongo_lab_db,
            ),
            "Lab System — Observations, vitals, lab results",
        )
        .await;

    let clickhouse_disabled = clickhouse_endpoints_disabled();
    if clickhouse_disabled {
        warn!("ClickHouse endpoint registration disabled for healthcare");
    }
    let ch_ep = if clickhouse_disabled {
        None
    } else {
        let ch_read_info = parse_clickhouse_url(&ch_read_url);
        let ch_write_info = parse_clickhouse_url(&ch_write_url);
        let ch_admin_info = parse_clickhouse_url(&ch_admin_url);
        let ch_system_info = parse_clickhouse_url(&ch_system_url);
        let ch_read = json!({"url": ch_read_info.0, "user": ch_read_info.1, "password": ch_read_info.2, "database": ch_read_info.3});
        let ch_write = json!({"url": ch_write_info.0, "user": ch_write_info.1, "password": ch_write_info.2, "database": ch_write_info.3});
        let ch_admin = json!({"url": ch_admin_info.0, "user": ch_admin_info.1, "password": ch_admin_info.2, "database": ch_admin_info.3});
        let ch_system = json!({"url": ch_system_info.0, "user": ch_system_info.1, "password": ch_system_info.2, "database": ch_system_info.3});
        Some(
            eden.create_endpoint("hc_billing_analytics", "clickhouse",
                json!({"read_conn": ch_read, "write_conn": ch_write, "admin_conn": ch_admin, "system_conn": ch_system}),
                "Billing Analytics — Claims aggregates, cost trends").await,
        )
    };

    // ── Redis: Alerts (4 connection tiers) ──
    let redis_read_info = parse_redis_url(&redis_read_url);
    let redis_write_info = parse_redis_url(&redis_write_url);
    let redis_admin_info = parse_redis_url(&redis_admin_url);
    let redis_system_info = parse_redis_url(&redis_system_url);
    let redis_read = json!({"host": redis_read_info.host, "port": redis_read_info.port,
        "tls": if redis_read_info.tls { json!(true) } else { json!(null) },
        "username": redis_read_info.username.as_deref().unwrap_or("reader"),
        "password": redis_read_info.password});
    let redis_write = json!({"host": redis_write_info.host, "port": redis_write_info.port,
        "tls": if redis_write_info.tls { json!(true) } else { json!(null) },
        "username": redis_write_info.username.as_deref().unwrap_or("writer"),
        "password": redis_write_info.password});
    let redis_admin = json!({"host": redis_admin_info.host, "port": redis_admin_info.port,
        "tls": if redis_admin_info.tls { json!(true) } else { json!(null) },
        "username": redis_admin_info.username.as_deref().unwrap_or("default"),
        "password": redis_admin_info.password});
    let redis_system = json!({"host": redis_system_info.host, "port": redis_system_info.port,
        "tls": if redis_system_info.tls { json!(true) } else { json!(null) },
        "username": redis_system_info.username.as_deref().unwrap_or("default"),
        "password": redis_system_info.password});
    let redis_ep = eden.create_endpoint("hc_alerts", "redis",
        json!({"read_conn": redis_read, "write_conn": redis_write, "admin_conn": redis_admin, "system_conn": redis_system}),
        "Real-time — Bed availability, patient alerts").await;

    // ── Weaviate: Clinical Search (no RBAC, same connection for all) ──
    let weav_conn = json!({ "url": weaviate_url, "token": "" });
    let weav_ep = eden.create_endpoint("hc_clinical_search", "weaviate",
        json!({"read_conn": weav_conn.clone(), "write_conn": weav_conn.clone(), "admin_conn": weav_conn.clone(), "system_conn": weav_conn}),
        "Clinical Search — Condition & procedure description embeddings").await;

    let total_qps = config.queries_per_second;
    let mut silos = vec![
        EndpointDef {
            silo_name: "pg_patient_records".into(),
            endpoint_id: ep_id(&pg1_ep, "hc_patient_records"),
            metrics_label: "pg_patients".into(),
            qps: total_qps * if clickhouse_disabled { 30 } else { 15 } / 100,
        },
        EndpointDef {
            silo_name: "pg_billing".into(),
            endpoint_id: ep_id(&pg2_ep, "hc_billing"),
            metrics_label: "pg_billing".into(),
            qps: total_qps * 12 / 100,
        },
        EndpointDef {
            silo_name: "pg_cms_claims".into(),
            endpoint_id: ep_id(&pg3_ep, "hc_cms_claims"),
            metrics_label: "pg_cms".into(),
            qps: total_qps * 12 / 100,
        },
        EndpointDef {
            silo_name: "mongo_clinical_docs".into(),
            endpoint_id: ep_id(&m1_ep, "hc_clinical_docs"),
            metrics_label: "mongo_clinical".into(),
            qps: total_qps * 15 / 100,
        },
        EndpointDef {
            silo_name: "mongo_lab_results".into(),
            endpoint_id: ep_id(&m2_ep, "hc_lab_results"),
            metrics_label: "mongo_lab".into(),
            qps: total_qps * 13 / 100,
        },
        EndpointDef {
            silo_name: "redis_alerts".into(),
            endpoint_id: ep_id(&redis_ep, "hc_alerts"),
            metrics_label: "redis_alerts".into(),
            qps: total_qps * 8 / 100,
        },
        EndpointDef {
            silo_name: "weaviate_clinical".into(),
            endpoint_id: ep_id(&weav_ep, "hc_clinical_search"),
            metrics_label: "weaviate_clinical".into(),
            qps: total_qps * 10 / 100,
        },
    ];
    if let Some(ch_ep) = ch_ep {
        silos.insert(
            5,
            EndpointDef {
                silo_name: "ch_billing_analytics".into(),
                endpoint_id: ep_id(&ch_ep, "hc_billing_analytics"),
                metrics_label: "ch_billing".into(),
                qps: total_qps * 15 / 100,
            },
        );
    }

    register_external_endpoints(eden, config).await;
    Endpoints {
        vertical: "healthcare".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Insurance (2x Postgres + ClickHouse + Mongo + Redis + Weaviate)
// ═══════════════════════════════════════════════════════════════

async fn register_insurance_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering insurance/risk database endpoints with Eden...");

    let pg_policy_url = std::env::var("EDEN_PG_POLICY_ADMIN_URL")
        .or_else(|_| std::env::var("PG_POLICY_ADMIN_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let pg_risk_url = std::env::var("EDEN_PG_RISK_SCORING_URL")
        .or_else(|_| std::env::var("PG_RISK_SCORING_URL"))
        .unwrap_or_else(|_| config.postgres_url.clone());
    let ch_url = config
        .eden_clickhouse_url
        .as_deref()
        .unwrap_or(&config.clickhouse_url);
    let mongo_url = config
        .eden_mongo_url
        .as_deref()
        .unwrap_or(&config.mongo_url);
    let redis_url = config
        .eden_redis_url
        .as_deref()
        .unwrap_or(&config.redis_url);
    let weaviate_url = config
        .eden_weaviate_url
        .as_deref()
        .unwrap_or(&config.weaviate_url);
    let (pg1_read_url, pg1_write_url, pg1_admin_url, pg1_system_url) = access_tier_urls(
        &["EDEN_PG_POLICY_ADMIN_READ_URL", "PG_POLICY_ADMIN_READ_URL"],
        &[
            "EDEN_PG_POLICY_ADMIN_WRITE_URL",
            "PG_POLICY_ADMIN_WRITE_URL",
        ],
        &["EDEN_PG_POLICY_ADMIN_URL", "PG_POLICY_ADMIN_URL"],
        pg_policy_url.clone(),
    );
    let (pg2_read_url, pg2_write_url, pg2_admin_url, pg2_system_url) = access_tier_urls(
        &["EDEN_PG_RISK_SCORING_READ_URL", "PG_RISK_SCORING_READ_URL"],
        &[
            "EDEN_PG_RISK_SCORING_WRITE_URL",
            "PG_RISK_SCORING_WRITE_URL",
        ],
        &["EDEN_PG_RISK_SCORING_URL", "PG_RISK_SCORING_URL"],
        pg_risk_url.clone(),
    );
    let (mongo_read_url, mongo_write_url, mongo_admin_url, mongo_system_url) = access_tier_urls(
        &["EDEN_MONGO_READ_URL", "MONGO_READ_URL"],
        &["EDEN_MONGO_WRITE_URL", "MONGO_WRITE_URL"],
        &["EDEN_MONGO_URL", "MONGO_URL"],
        mongo_url.to_string(),
    );
    let (redis_read_url, redis_write_url, redis_admin_url, redis_system_url) = access_tier_urls(
        &["EDEN_REDIS_READ_URL", "REDIS_READ_URL"],
        &["EDEN_REDIS_WRITE_URL", "REDIS_WRITE_URL"],
        &["EDEN_REDIS_URL", "REDIS_URL"],
        redis_url.to_string(),
    );
    let (ch_read_url, ch_write_url, ch_admin_url, ch_system_url) = access_tier_urls(
        &["EDEN_CLICKHOUSE_READ_URL", "CLICKHOUSE_READ_URL"],
        &["EDEN_CLICKHOUSE_WRITE_URL", "CLICKHOUSE_WRITE_URL"],
        &["EDEN_CLICKHOUSE_URL", "CLICKHOUSE_URL"],
        ch_url.to_string(),
    );

    // ── Postgres #1: Policy Admin (4 connection tiers) ──
    let pg1_ep = eden
        .create_endpoint(
            "ins_policy_admin",
            "postgres",
            postgres_endpoint_config(
                &pg1_read_url,
                &pg1_write_url,
                &pg1_admin_url,
                &pg1_system_url,
            ),
            "Policy Admin — French motor liability policies & claims",
        )
        .await;

    // ── Postgres #2: Risk Scoring (4 connection tiers) ──
    let pg2_ep = eden
        .create_endpoint(
            "ins_risk_scoring",
            "postgres",
            postgres_endpoint_config(
                &pg2_read_url,
                &pg2_write_url,
                &pg2_admin_url,
                &pg2_system_url,
            ),
            "Underwriting — Driver risk prediction",
        )
        .await;

    let clickhouse_disabled = clickhouse_endpoints_disabled();
    if clickhouse_disabled {
        warn!("ClickHouse endpoint registration disabled for insurance");
    }
    let ch_ep = if clickhouse_disabled {
        None
    } else {
        let ch_read_info = parse_clickhouse_url(&ch_read_url);
        let ch_write_info = parse_clickhouse_url(&ch_write_url);
        let ch_admin_info = parse_clickhouse_url(&ch_admin_url);
        let ch_system_info = parse_clickhouse_url(&ch_system_url);
        let ch_read = json!({"url": ch_read_info.0, "user": ch_read_info.1, "password": ch_read_info.2, "database": ch_read_info.3});
        let ch_write = json!({"url": ch_write_info.0, "user": ch_write_info.1, "password": ch_write_info.2, "database": ch_write_info.3});
        let ch_admin = json!({"url": ch_admin_info.0, "user": ch_admin_info.1, "password": ch_admin_info.2, "database": ch_admin_info.3});
        let ch_system = json!({"url": ch_system_info.0, "user": ch_system_info.1, "password": ch_system_info.2, "database": ch_system_info.3});
        Some(
            eden.create_endpoint("ins_claims_analytics", "clickhouse",
                json!({"read_conn": ch_read, "write_conn": ch_write, "admin_conn": ch_admin, "system_conn": ch_system}),
                "Claims Analytics — US accident severity, geographic risk").await,
        )
    };

    // ── MongoDB: Incidents (4 connection tiers) ──
    let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "incidents".to_string());
    let mongo_ep = eden
        .create_endpoint(
            "ins_incidents",
            "mongo",
            mongo_endpoint_config(
                &mongo_read_url,
                &mongo_write_url,
                &mongo_admin_url,
                &mongo_system_url,
                &mongo_db,
            ),
            "Incident Reports — US traffic accidents",
        )
        .await;

    // ── Redis: Claims (4 connection tiers) ──
    let redis_read_info = parse_redis_url(&redis_read_url);
    let redis_write_info = parse_redis_url(&redis_write_url);
    let redis_admin_info = parse_redis_url(&redis_admin_url);
    let redis_system_info = parse_redis_url(&redis_system_url);
    let redis_read = json!({"host": redis_read_info.host, "port": redis_read_info.port,
        "tls": if redis_read_info.tls { json!(true) } else { json!(null) },
        "username": redis_read_info.username.as_deref().unwrap_or("reader"),
        "password": redis_read_info.password});
    let redis_write = json!({"host": redis_write_info.host, "port": redis_write_info.port,
        "tls": if redis_write_info.tls { json!(true) } else { json!(null) },
        "username": redis_write_info.username.as_deref().unwrap_or("writer"),
        "password": redis_write_info.password});
    let redis_admin = json!({"host": redis_admin_info.host, "port": redis_admin_info.port,
        "tls": if redis_admin_info.tls { json!(true) } else { json!(null) },
        "username": redis_admin_info.username.as_deref().unwrap_or("default"),
        "password": redis_admin_info.password});
    let redis_system = json!({"host": redis_system_info.host, "port": redis_system_info.port,
        "tls": if redis_system_info.tls { json!(true) } else { json!(null) },
        "username": redis_system_info.username.as_deref().unwrap_or("default"),
        "password": redis_system_info.password});
    let redis_ep = eden.create_endpoint("ins_claims_cache", "redis",
        json!({"read_conn": redis_read, "write_conn": redis_write, "admin_conn": redis_admin, "system_conn": redis_system}),
        "Real-time — Claim status tracker, active policies cache").await;

    // ── Weaviate: Claims Search (no RBAC, same connection for all) ──
    let weav_conn = json!({ "url": weaviate_url, "token": "" });
    let weav_ep = eden.create_endpoint("ins_claims_search", "weaviate",
        json!({"read_conn": weav_conn.clone(), "write_conn": weav_conn.clone(), "admin_conn": weav_conn.clone(), "system_conn": weav_conn}),
        "Claims Search — Accident description & similarity search").await;

    let total_qps = config.queries_per_second;
    let mut silos = vec![
        EndpointDef {
            silo_name: "pg_policy_admin".into(),
            endpoint_id: ep_id(&pg1_ep, "ins_policy_admin"),
            metrics_label: "pg_policy".into(),
            qps: total_qps * if clickhouse_disabled { 45 } else { 20 } / 100,
        },
        EndpointDef {
            silo_name: "pg_risk_scoring".into(),
            endpoint_id: ep_id(&pg2_ep, "ins_risk_scoring"),
            metrics_label: "pg_risk".into(),
            qps: total_qps * 15 / 100,
        },
        EndpointDef {
            silo_name: "mongo_incidents".into(),
            endpoint_id: ep_id(&mongo_ep, "ins_incidents"),
            metrics_label: "mongo_incidents".into(),
            qps: total_qps * 15 / 100,
        },
        EndpointDef {
            silo_name: "redis_claims".into(),
            endpoint_id: ep_id(&redis_ep, "ins_claims_cache"),
            metrics_label: "redis_claims".into(),
            qps: total_qps * 15 / 100,
        },
        EndpointDef {
            silo_name: "weaviate_claims".into(),
            endpoint_id: ep_id(&weav_ep, "ins_claims_search"),
            metrics_label: "weaviate_claims".into(),
            qps: total_qps * 10 / 100,
        },
    ];
    if let Some(ch_ep) = ch_ep {
        silos.insert(
            2,
            EndpointDef {
                silo_name: "ch_claims_analytics".into(),
                endpoint_id: ep_id(&ch_ep, "ins_claims_analytics"),
                metrics_label: "ch_claims".into(),
                qps: total_qps * 25 / 100,
            },
        );
    }

    register_external_endpoints(eden, config).await;
    Endpoints {
        vertical: "insurance".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Vertical: Migration (Source Redis + Destination Redis)
// ═══════════════════════════════════════════════════════════════

async fn register_migration_endpoints(eden: &EdenClient, config: &Config) -> Endpoints {
    info!("\nRegistering migration vertical endpoints with Eden...");

    let source_url = std::env::var("EDEN_REDIS_SOURCE_URL").unwrap_or_else(|_| {
        std::env::var("REDIS_SOURCE_URL").unwrap_or_else(|_| config.redis_url.clone())
    });
    let dest_url = std::env::var("EDEN_REDIS_DEST_URL").unwrap_or_else(|_| {
        std::env::var("REDIS_DEST_URL")
            .unwrap_or_else(|_| "redis://default:eden@localhost:6580".to_string())
    });

    // Register Source Redis (Azure Redis Cache — single password, no ACL users)
    let src_info = parse_redis_url(&source_url);
    let src_conn = json!({"host": src_info.host, "port": src_info.port,
        "tls": if src_info.tls { json!(true) } else { json!(null) },
        "password": src_info.password});

    let source_ep = eden
        .create_endpoint(
            "mig_redis_source",
            "redis",
            json!({"read_conn": src_conn.clone(), "write_conn": src_conn.clone(),
               "admin_conn": src_conn.clone(), "system_conn": src_conn}),
            "Source Redis — Azure Redis Cache (migration origin)",
        )
        .await;

    // Register Destination Redis (Azure Managed Redis — single password, no ACL users)
    let dst_info = parse_redis_url(&dest_url);
    let dst_conn = json!({"host": dst_info.host, "port": dst_info.port,
        "tls": if dst_info.tls { json!(true) } else { json!(null) },
        "password": dst_info.password});

    let _dest_ep = eden
        .create_endpoint(
            "mig_redis_dest",
            "redis",
            json!({"read_conn": dst_conn.clone(), "write_conn": dst_conn.clone(),
               "admin_conn": dst_conn.clone(), "system_conn": dst_conn}),
            "Destination Redis — Azure Managed Redis (migration target, starts empty)",
        )
        .await;

    let total_qps = config.queries_per_second;

    let silos = vec![EndpointDef {
        silo_name: "redis_source".into(),
        endpoint_id: ep_id(&source_ep, "mig_redis_source"),
        metrics_label: "redis_source".into(),
        qps: total_qps,
    }];

    Endpoints {
        vertical: "migration".into(),
        silos,
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    }
}

// ═══════════════════════════════════════════════════════════════
// Shared helpers
// ═══════════════════════════════════════════════════════════════

/// Replace user:password credentials in a postgresql:// or mongodb:// URL.
fn url_with_creds(base_url: &str, user: &str, password: &str) -> String {
    let re = Regex::new(r"://[^@]+@").unwrap();
    let url = re
        .replace(base_url, &format!("://{}:{}@", user, password))
        .to_string();
    if url.starts_with("mongodb://") {
        normalize_mongo_url(&url)
    } else {
        url
    }
}

fn env_url_or(names: &[&str], fallback: String) -> String {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .unwrap_or(fallback)
}

fn access_tier_urls(
    read_vars: &[&str],
    write_vars: &[&str],
    admin_vars: &[&str],
    fallback_admin_url: String,
) -> (String, String, String, String) {
    let admin_url = env_url_or(admin_vars, fallback_admin_url);
    let read_url = env_url_or(
        read_vars,
        url_with_creds(&admin_url, "reader", "reader_pass"),
    );
    let write_url = env_url_or(
        write_vars,
        url_with_creds(&admin_url, "writer", "writer_pass"),
    );
    let system_url = admin_url.clone();
    (read_url, write_url, admin_url, system_url)
}

#[derive(Debug, Clone)]
struct PostgresUrlParts {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    sslmode: Option<String>,
    application_name: Option<String>,
}

fn parse_postgres_url(url: &str) -> PostgresUrlParts {
    let without_scheme = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .unwrap_or(url);

    let (path_part, query_string) = match without_scheme.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (without_scheme, None),
    };

    let (userinfo, host_db) = match path_part.rsplit_once('@') {
        Some((info, host_db)) => (Some(info), host_db),
        None => (None, path_part),
    };

    let (username, password) = match userinfo {
        Some(info) => match info.split_once(':') {
            Some((user, password)) => (
                percent_decode_url_component(user),
                Some(percent_decode_url_component(password)),
            ),
            None => (percent_decode_url_component(info), None),
        },
        None => ("postgres".to_string(), None),
    };

    let (host_port, database) = match host_db.split_once('/') {
        Some((host_port, database)) => (
            host_port,
            if database.is_empty() {
                username.clone()
            } else {
                percent_decode_url_component(database)
            },
        ),
        None => (host_db, username.clone()),
    };

    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host.to_string(), port),
            Err(_) => (host_port.to_string(), 5432),
        },
        None => (host_port.to_string(), 5432),
    };

    let mut sslmode = None;
    let mut application_name = None;
    if let Some(query) = query_string {
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "sslmode" => sslmode = Some(value.to_string()),
                    "application_name" => {
                        application_name = Some(percent_decode_url_component(value));
                    }
                    _ => {}
                }
            }
        }
    }

    PostgresUrlParts {
        host: if is_loopback_host(&host) {
            "host.docker.internal".to_string()
        } else {
            host
        },
        port,
        database,
        username,
        password,
        sslmode,
        application_name,
    }
}

fn percent_decode_url_component(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = bytes[i + 1];
            let lo = bytes[i + 2];
            let decoded = match (hex_value(hi), hex_value(lo)) {
                (Some(hi), Some(lo)) => Some((hi << 4) | lo),
                _ => None,
            };

            if let Some(decoded) = decoded {
                result.push(decoded as char);
                i += 3;
                continue;
            }
        }

        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn postgres_endpoint_config(
    read_url: &str,
    write_url: &str,
    admin_url: &str,
    system_url: &str,
) -> serde_json::Value {
    let admin = parse_postgres_url(admin_url);
    let read = parse_postgres_url(read_url);
    let write = parse_postgres_url(write_url);
    let system = parse_postgres_url(system_url);

    json!({
        "target": {
            "host": &admin.host,
            "port": admin.port,
            "database": &admin.database,
            "sslmode": &admin.sslmode,
            "application_name": &admin.application_name
        },
        "read_credentials": {
            "username": &read.username,
            "password": &read.password
        },
        "write_credentials": {
            "username": &write.username,
            "password": &write.password
        },
        "admin_credentials": {
            "username": &admin.username,
            "password": &admin.password
        },
        "system_credentials": {
            "username": &system.username,
            "password": &system.password
        }
    })
}

fn normalize_mongo_url(url: &str) -> String {
    let url = normalize_loopback_url_host(url);
    let re = Regex::new(r"^(mongodb://[^/?]+)(/[^?]*)?(\?.*)?$").unwrap();
    if let Some(caps) = re.captures(&url) {
        let host = caps.get(1).map(|m| m.as_str()).unwrap_or(url.as_str());
        let path = caps.get(2).map(|m| m.as_str()).unwrap_or("/");
        let query = caps.get(3).map(|m| m.as_str()).unwrap_or("");
        let query = if query.contains("authSource=") {
            query.to_string()
        } else if query.is_empty() {
            "?authSource=admin".to_string()
        } else {
            format!("{query}&authSource=admin")
        };
        format!("{host}{path}{query}")
    } else {
        url.to_string()
    }
}

fn mongo_endpoint_config(
    read_url: &str,
    write_url: &str,
    admin_url: &str,
    system_url: &str,
    db_name: &str,
) -> serde_json::Value {
    json!({
        "db_name": db_name,
        "read_conn": { "url": read_url },
        "write_conn": { "url": write_url },
        "admin_conn": { "url": admin_url },
        "system_conn": { "url": system_url }
    })
}

fn localfs_endpoint_config(root_path: &str) -> serde_json::Value {
    json!({
        "read_conn": { "path": root_path },
        "write_conn": { "path": root_path },
        "admin_conn": { "path": root_path },
        "system_conn": { "path": root_path }
    })
}

async fn register_external_endpoints(eden: &EdenClient, config: &Config) {
    if !config.tavily_api_key.is_empty() {
        let conn = json!({ "api_key": &config.tavily_api_key });
        let _ = eden
            .create_endpoint(
                "adam_tavily",
                "tavily",
                json!({"read_conn": conn, "write_conn": conn}),
                "Tavily web search for real-time market data",
            )
            .await;
    }
    if !config.google_workspace_access_token.is_empty() {
        if let Some(base_url) =
            validate_public_http_base_url("Google Workspace", &config.google_workspace_api_base_url)
        {
            let conn = json!({
                "base_url": base_url,
                "headers": {
                    "Authorization": format!("Bearer {}", config.google_workspace_access_token),
                    "Content-Type": "application/json"
                }
            });
            let _ = eden
                .create_endpoint(
                    "adam_google_workspace",
                    "http",
                    conn,
                    "Google Workspace APIs via HTTP",
                )
                .await;
        } else {
            let _ = eden.delete_endpoint("adam_google_workspace").await;
        }
    } else if !config.google_workspace_client_id.is_empty()
        || !config.google_workspace_client_secret.is_empty()
    {
        warn!(
            "Google Workspace OAuth client credentials were provided, but GOOGLE_WORKSPACE_ACCESS_TOKEN is empty; skipping Google endpoint registration"
        );
        let _ = eden.delete_endpoint("adam_google_workspace").await;
    } else {
        let _ = eden.delete_endpoint("adam_google_workspace").await;
    }
    let azure_config_present = [
        config.azure_app_id.as_str(),
        config.azure_display_name.as_str(),
        config.azure_password.as_str(),
        config.azure_tenant.as_str(),
        config.azure_subscription_id.as_str(),
    ]
    .iter()
    .any(|value| !value.trim().is_empty());
    if azure_config_present {
        let mut missing = Vec::new();
        if config.azure_app_id.trim().is_empty() {
            missing.push("AZURE_APP_ID");
        }
        if config.azure_password.trim().is_empty() {
            missing.push("AZURE_PASSWORD");
        }
        if config.azure_tenant.trim().is_empty() {
            missing.push("AZURE_TENANT");
        }
        if config.azure_subscription_id.trim().is_empty() {
            missing.push("AZURE_SUBSCRIPTION_ID");
        }

        if missing.is_empty() {
            match azure_management_connection(config).await {
                Ok((conn, refresh_interval)) => {
                    let description = azure_endpoint_description(config);
                    let _ = eden
                        .create_endpoint("adam_azure", "http", conn, &description)
                        .await;
                    spawn_azure_endpoint_refresh(eden.clone(), config.clone(), refresh_interval);
                }
                Err(err) => {
                    warn!("Skipping Azure endpoint registration: {}", err);
                    let _ = eden.delete_endpoint("adam_azure").await;
                }
            }
        } else {
            warn!(
                "Azure service-principal credentials are partially configured (missing {}); skipping Azure endpoint registration",
                missing.join(", ")
            );
            let _ = eden.delete_endpoint("adam_azure").await;
        }
    } else {
        let _ = eden.delete_endpoint("adam_azure").await;
    }
    if !config.gitlab_access_token.is_empty() {
        if let Some(base_url) = validate_public_http_base_url("GitLab", &config.gitlab_api_base_url)
        {
            let conn = json!({
                "base_url": base_url,
                "headers": {
                    "PRIVATE-TOKEN": &config.gitlab_access_token,
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }
            });
            let _ = eden
                .create_endpoint("adam_gitlab", "http", conn, "GitLab APIs via HTTP")
                .await;
        } else {
            let _ = eden.delete_endpoint("adam_gitlab").await;
        }
    } else {
        let _ = eden.delete_endpoint("adam_gitlab").await;
    }
    if !config.openai_api_key.is_empty() {
        let conn = json!({
            "provider": "OpenAI",
            "inline_api_key": &config.openai_api_key,
            "defaults": {
                "model": &config.openai_model,
                "max_tokens": 2048
            }
        });
        let _ = eden
            .create_endpoint(
                &config.openai_model,
                "llm",
                json!({"read_conn": conn, "write_conn": conn}),
                "OpenAI LLM for natural language analysis and summarization",
            )
            .await;
    }
    if !config.openrouter_api_key.is_empty() {
        let conn = json!({"provider": "OpenRouter", "inline_api_key": &config.openrouter_api_key,
            "defaults": {"model": &config.openrouter_model, "max_tokens": 2048, "temperature": 0.7}});
        let _ = eden
            .create_endpoint(
                &config.openrouter_model,
                "llm",
                json!({"read_conn": conn, "write_conn": conn}),
                "LLM for natural language analysis and summarization",
            )
            .await;
    }
    if !config.dd_api_key.is_empty() {
        let mut dd_conn = json!({"site": &config.dd_site, "api_key": &config.dd_api_key});
        if !config.dd_app_key.is_empty() {
            dd_conn["application_key"] = json!(&config.dd_app_key);
        }
        let _ = eden
            .create_endpoint(
                "adam_datadog",
                "datadog",
                json!({"read_conn": dd_conn, "write_conn": dd_conn}),
                "Datadog observability — metrics, logs, and APM data",
            )
            .await;
    }
    if !config.eraser_api_key.is_empty() {
        let conn = json!({ "api_key": &config.eraser_api_key });
        let _ = eden
            .create_endpoint(
                "adam_eraser",
                "eraser",
                json!({"read_conn": conn, "write_conn": conn}),
                "Eraser — AI-powered diagram and architecture generation",
            )
            .await;
    }
}

fn ep_id(
    result: &Result<eden_client::EndpointInfo, Box<dyn std::error::Error + Send + Sync>>,
    default: &str,
) -> String {
    match result {
        Ok(info) => info.id.clone(),
        Err(e) => {
            error!(
                "Endpoint registration failed: {}. Using default '{}'",
                e, default
            );
            default.to_string()
        }
    }
}

/// Start server without query workers (when Eden API is unavailable).
async fn start_server_only(config: Arc<Config>, metrics: Arc<AppMetrics>) {
    let endpoints = Endpoints {
        vertical: config.vertical.clone(),
        silos: vec![],
        tavily: None,
        llm: None,
        datadog: None,
        eraser: None,
    };
    let state = AppState {
        metrics,
        config: config.clone(),
        endpoints,
    };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .with_state(state);

    let bind = &config.bind_address;
    info!("HTTP server listening on {} (metrics-only mode)", bind);
    let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ── HTTP Handlers ─────────────────────────────────────────────

async fn metrics_handler(State(state): State<AppState>) -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&state.metrics.registry.gather(), &mut buffer)
        .unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn status_handler(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    let mut db_status = serde_json::Map::new();
    for def in &state.endpoints.silos {
        let label = &def.metrics_label;
        let total = state
            .metrics
            .queries_total
            .with_label_values(&[label])
            .get();
        let errors = state.metrics.query_errors.with_label_values(&[label]).get();
        let qps = state
            .metrics
            .queries_per_second
            .with_label_values(&[label])
            .get();
        let healthy = state
            .metrics
            .endpoint_healthy
            .with_label_values(&[label])
            .get();
        db_status.insert(
            label.clone(),
            json!({
                "silo": def.silo_name,
                "endpoint_id": def.endpoint_id,
                "total_queries": total as u64,
                "errors": errors as u64,
                "qps": qps,
                "healthy": healthy == 1.0,
            }),
        );
    }

    axum::Json(json!({
        "status": "running",
        "vertical": state.config.vertical,
        "eden_api": state.config.eden_api_url,
        "org_id": state.config.eden_org_id,
        "target_qps": state.config.queries_per_second,
        "databases": db_status,
        "cross_db_queries": state.metrics.cross_db_queries_total.get() as u64,
    }))
}

// ── URL Parsers ───────────────────────────────────────────────

struct RedisConnInfo {
    host: String,
    port: u16,
    tls: bool,
    username: Option<String>,
    password: Option<String>,
}

fn parse_redis_url(url: &str) -> RedisConnInfo {
    let url = normalize_loopback_url_host(url);
    let tls = url.starts_with("rediss://");
    let stripped = url
        .strip_prefix("redis://")
        .or_else(|| url.strip_prefix("rediss://"))
        .unwrap_or(&url);
    let (username, password, host_port_path) = if let Some((auth, rest)) = stripped.split_once('@')
    {
        if let Some((user, pass)) = auth.split_once(':') {
            let username = if user.is_empty() {
                None
            } else {
                Some(decode_url_credential(user))
            };
            let password = if pass.is_empty() {
                None
            } else {
                Some(decode_url_credential(pass))
            };
            (username, password, rest)
        } else if auth.is_empty() {
            (None, None, rest)
        } else {
            (None, Some(decode_url_credential(auth)), rest)
        }
    } else {
        (None, None, stripped)
    };
    let host_port = host_port_path.split('/').next().unwrap_or(host_port_path);
    let (host, port) = if let Some((h, port_str)) = host_port.rsplit_once(':') {
        (h.to_string(), port_str.parse::<u16>().unwrap_or(6379))
    } else {
        (host_port.to_string(), 6379)
    };
    RedisConnInfo {
        host: if is_loopback_host(&host) {
            "host.docker.internal".to_string()
        } else {
            host
        },
        port,
        tls,
        username,
        password,
    }
}

fn decode_url_credential(value: &str) -> String {
    value
        .replace("%3D", "=")
        .replace("%40", "@")
        .replace("%3A", ":")
        .replace("%25", "%")
}

fn parse_clickhouse_url(url: &str) -> (String, String, String, String) {
    let normalized = normalize_loopback_url_host(url);
    let parsed = reqwest::Url::parse(&normalized)
        .or_else(|_| reqwest::Url::parse(&format!("http://{normalized}")))
        .unwrap_or_else(|_| reqwest::Url::parse("http://localhost").unwrap());
    let scheme = parsed.scheme().to_string();
    let host = parsed
        .host_str()
        .map(|host| {
            if is_loopback_host(host) {
                "host.docker.internal".to_string()
            } else {
                host.to_string()
            }
        })
        .unwrap_or_else(|| "host.docker.internal".to_string());
    let base = if let Some(port) = parsed.port() {
        format!("{scheme}://{host}:{port}")
    } else {
        format!("{scheme}://{host}")
    };
    let user = percent_decode_url_component(parsed.username());
    let password = parsed
        .password()
        .map(decode_url_credential)
        .unwrap_or_default();
    let database = parsed
        .path_segments()
        .and_then(|mut segments| segments.next())
        .filter(|segment| !segment.is_empty())
        .unwrap_or("default")
        .to_string();
    (base, user, password, database)
}

#[cfg(test)]
mod tests {
    use super::{
        is_loopback_host, normalize_loopback_url_host, parse_clickhouse_url,
        validate_public_http_base_url,
    };

    #[test]
    fn detects_loopback_hosts() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));
        assert!(!is_loopback_host("host.docker.internal"));
        assert!(!is_loopback_host("www.googleapis.com"));
    }

    #[test]
    fn rewrites_loopback_urls_for_container_visible_hosts() {
        assert_eq!(
            normalize_loopback_url_host("http://localhost:8280"),
            "http://host.docker.internal:8280/"
        );
        assert_eq!(
            normalize_loopback_url_host("redis://default:eden@127.0.0.1:6579"),
            "redis://default:eden@host.docker.internal:6579"
        );
    }

    #[test]
    fn rejects_private_http_tool_hosts() {
        assert!(
            validate_public_http_base_url("Google Workspace", "https://www.googleapis.com")
                .is_some()
        );
        assert!(
            validate_public_http_base_url("Google Workspace", "http://localhost:3001").is_none()
        );
        assert!(
            validate_public_http_base_url("Google Workspace", "http://127.0.0.1:3001").is_none()
        );
    }

    #[test]
    fn clickhouse_parser_normalizes_loopback_hosts() {
        let (base, user, password, database) =
            parse_clickhouse_url("http://eden:eden@localhost:8323/analytics");
        assert_eq!(base, "http://host.docker.internal:8323");
        assert_eq!(user, "eden");
        assert_eq!(password, "eden");
        assert_eq!(database, "analytics");
    }
}
