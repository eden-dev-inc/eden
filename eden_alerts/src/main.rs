//! Eden Alerts CLI - Standalone alerting service for Eden analytics.

use std::path::PathBuf;

use clap::Parser;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use eden_alerts::AlertService;
use eden_alerts::config::AlertsConfig;
use eden_alerts::provider::ClickhouseProvider;

/// Eden Alerts - Watches ClickHouse analytics and dispatches notifications.
#[derive(Parser, Debug)]
#[command(name = "eden-alerts")]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to configuration file (TOML format).
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// ClickHouse URL (overrides config file).
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,

    /// ClickHouse database (overrides config file).
    #[arg(long, env = "CLICKHOUSE_DATABASE")]
    clickhouse_database: Option<String>,

    /// Polling interval in seconds (overrides config file).
    #[arg(long, env = "EDEN_ALERTS_POLL_INTERVAL_SECS")]
    poll_interval: Option<u64>,

    /// Time window in minutes for queries (overrides config file).
    #[arg(long, env = "EDEN_ALERTS_WINDOW_MINUTES")]
    window_minutes: Option<i64>,

    /// Enable debug logging.
    #[arg(short, long)]
    debug: bool,

    /// Validate configuration and exit.
    #[arg(long)]
    validate: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing
    let log_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| format!("eden_alerts={},info", log_level).into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let mut config = match &args.config {
        Some(path) => {
            info!(?path, "loading configuration from file");
            AlertsConfig::from_file(path)?
        }
        None => {
            info!("loading configuration from environment");
            AlertsConfig::from_env()
        }
    };

    // Apply CLI overrides
    if let Some(url) = args.clickhouse_url {
        config.clickhouse.url = url;
    }
    if let Some(db) = args.clickhouse_database {
        config.clickhouse.database = db;
    }
    if let Some(interval) = args.poll_interval {
        config.poll_interval_secs = interval;
    }
    if let Some(window) = args.window_minutes {
        config.window_minutes = window;
    }

    // Validate configuration
    if let Err(err) = config.validate() {
        error!(?err, "invalid configuration");
        std::process::exit(1);
    }

    if args.validate {
        info!("configuration is valid");
        return Ok(());
    }

    info!(
        clickhouse_url = %config.clickhouse.url,
        clickhouse_database = %config.clickhouse.database,
        poll_interval_secs = config.poll_interval_secs,
        window_minutes = config.window_minutes,
        backends = config.notify.backends.len(),
        threshold_rules = config.rules.thresholds.len(),
        anti_pattern_rules = config.rules.anti_patterns.len(),
        report_rules = config.rules.reports.len(),
        "starting eden-alerts"
    );

    // Create ClickHouse provider
    let provider = ClickhouseProvider::new(config.clickhouse.clone().into())?;

    // Create and run service
    let service = AlertService::new(provider, config)?;

    // Setup shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("failed to listen for ctrl+c");
        info!("received shutdown signal");
        let _ = shutdown_tx.send(true);
    });

    // Run the service
    service.run(shutdown_rx).await;

    info!("eden-alerts stopped");
    Ok(())
}
