use crate::client;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

/// Launch the integrated observer TUI in a new Terminal window, then run the client workload
#[derive(Parser, Debug, Clone)]
pub struct ObserveClientConfig {
    #[clap(flatten)]
    pub client: client::ClientConfig,

    /// Source Redis URL used by Eden (for setup/migration metadata)
    #[clap(long, env = "REDIS_SOURCE_URL")]
    pub source_url: String,

    /// Destination Redis URL used by Eden (for setup/migration metadata)
    #[clap(long, env = "REDIS_DEST_URL")]
    pub dest_url: String,

    /// Eden API base URL
    #[clap(long, env = "EDEN_API_URL", default_value = "http://localhost:8000")]
    pub api_url: String,

    /// Eden organization ID
    #[clap(long, env = "EDEN_ORG_ID", default_value = "adam-demo")]
    pub org_id: String,

    /// Source Redis host:port for the observer TUI. Defaults to REDIS_SOURCE_URL, with
    /// host.docker.internal rewritten to localhost for local Docker-based setups.
    #[clap(long, env = "OBSERVER_SOURCE")]
    pub observer_source: Option<String>,

    /// Destination Redis host:port for the observer TUI. Defaults to REDIS_DEST_URL, with
    /// host.docker.internal rewritten to localhost for local Docker-based setups.
    #[clap(long, env = "OBSERVER_DEST")]
    pub observer_dest: Option<String>,

    /// Delay before starting the client workload, to give the observer a moment to launch
    #[clap(long, env = "OBSERVER_START_DELAY_MS", default_value = "1500")]
    pub observer_start_delay_ms: u64,

    /// Maximum time to wait for the observer to report that it is ready
    #[clap(long, env = "OBSERVER_READY_TIMEOUT_MS", default_value = "60000")]
    pub observer_ready_timeout_ms: u64,
}

struct RedisEndpoint {
    url: url::Url,
    host: String,
}

fn parse_redis_endpoint(url_str: &str) -> Result<RedisEndpoint> {
    let normalized = if url_str.contains("://") {
        url_str.to_string()
    } else {
        format!("redis://{}", url_str)
    };

    let parsed = url::Url::parse(&normalized).context("Invalid Redis URL")?;
    let tls = parsed.scheme() == "rediss";
    let host = parsed.host_str().unwrap_or("localhost").to_string();
    let _port = parsed.port().unwrap_or(if tls { 6380 } else { 6379 });

    Ok(RedisEndpoint { url: parsed, host })
}

pub async fn run_observe_client(config: ObserveClientConfig) -> Result<()> {
    let eden_source = parse_redis_endpoint(&config.source_url)?;
    let eden_dest = parse_redis_endpoint(&config.dest_url)?;

    let observer_source = config
        .observer_source
        .clone()
        .unwrap_or_else(|| observer_target(&eden_source));
    let observer_dest = config
        .observer_dest
        .clone()
        .unwrap_or_else(|| observer_target(&eden_dest));

    let ready_file = observer_ready_file_path();
    let _ = fs::remove_file(&ready_file);
    let workload_file = workload_stats_file_path();
    let _ = fs::remove_file(&workload_file);

    launch_observer(
        &observer_source,
        &observer_dest,
        &config.api_url,
        &config.source_url,
        &config.dest_url,
        &config.org_id,
        &ready_file,
        &workload_file,
    )?;

    wait_for_observer_ready(&ready_file, config.observer_ready_timeout_ms).await?;
    println!(
        "Observer launched for source={} dest={}",
        observer_source, observer_dest
    );
    println!(
        "Starting client workload in {} ms...",
        config.observer_start_delay_ms
    );
    println!();

    std::env::set_var("REDIS_WORKLOAD_STATS_FILE", workload_file.as_os_str());

    sleep(Duration::from_millis(config.observer_start_delay_ms)).await;
    client::run(config.client).await
}

fn observer_target(endpoint: &RedisEndpoint) -> String {
    let mut url = endpoint.url.clone();
    if endpoint.host == "host.docker.internal" {
        let _ = url.set_host(Some("localhost"));
    }
    url.to_string()
}

#[cfg(target_os = "macos")]
fn launch_observer(
    observer_source: &str,
    observer_dest: &str,
    api_url: &str,
    eden_source: &str,
    eden_dest: &str,
    org_id: &str,
    ready_file: &Path,
    workload_file: &Path,
) -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest_path = manifest_dir.join("Cargo.toml");

    ensure_exists(&manifest_dir, "redis-migrator directory")?;
    ensure_exists(&manifest_path, "redis-migrator Cargo.toml")?;

    let shell_command = format!(
        "cd {} && EDEN_ORG_ID={} REDIS_OBSERVER_READY_FILE={} REDIS_WORKLOAD_STATS_FILE={} cargo run --release --manifest-path {} -- observe {} {} {} {} {}",
        sh_quote_path(&manifest_dir),
        sh_quote(org_id),
        sh_quote_path(ready_file),
        sh_quote_path(&workload_file),
        sh_quote_path(&manifest_path),
        sh_quote(observer_source),
        sh_quote(observer_dest),
        sh_quote(api_url),
        sh_quote(eden_source),
        sh_quote(eden_dest),
    );

    let status = Command::new("osascript")
        .arg("-e")
        .arg("tell application \"Terminal\" to activate")
        .arg("-e")
        .arg(format!(
            "tell application \"Terminal\" to do script \"{}\"",
            applescript_escape(&shell_command)
        ))
        .status()
        .context("Failed to launch Terminal for the observer TUI")?;

    if !status.success() {
        return Err(anyhow!(
            "osascript failed to open the observer TUI in Terminal"
        ));
    }

    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn launch_observer(
    observer_source: &str,
    observer_dest: &str,
    api_url: &str,
    eden_source: &str,
    eden_dest: &str,
    org_id: &str,
    _ready_file: &Path,
    _workload_file: &Path,
) -> Result<()> {
    Err(anyhow!(
        "Automatic observer launch is only implemented on macOS. Run the integrated observer manually with: EDEN_ORG_ID={} cargo run --manifest-path examples/redis-migrator/Cargo.toml -- observe {} {} {} {} {}",
        org_id,
        observer_source,
        observer_dest,
        api_url,
        eden_source,
        eden_dest
    ))
}

async fn wait_for_observer_ready(path: &Path, timeout_ms: u64) -> Result<()> {
    let poll_interval = Duration::from_millis(250);
    let mut waited = 0u64;
    while waited <= timeout_ms {
        if path.exists() {
            let _ = fs::remove_file(path);
            return Ok(());
        }
        sleep(poll_interval).await;
        waited += poll_interval.as_millis() as u64;
    }
    Err(anyhow!(
        "Timed out waiting for the observer TUI to become ready after {} ms",
        timeout_ms
    ))
}

fn observer_ready_file_path() -> PathBuf {
    unique_temp_file("redis-migrator-observer-ready")
}

fn workload_stats_file_path() -> PathBuf {
    unique_temp_file("redis-workload-stats")
}

fn unique_temp_file(prefix: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir().join(format!("{}-{}-{}.tmp", prefix, std::process::id(), millis))
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn sh_quote_path(path: &Path) -> String {
    sh_quote(&path.display().to_string())
}

#[cfg(target_os = "macos")]
fn applescript_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn ensure_exists(path: &Path, label: &str) -> Result<()> {
    if path.exists() {
        Ok(())
    } else {
        Err(anyhow!("Missing {} at {}", label, path.display()))
    }
}
