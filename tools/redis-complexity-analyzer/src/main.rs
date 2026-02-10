use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use colored::*;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use rand::Rng;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Frame, Terminal,
};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;

// =============================================================================
// CLI Configuration
// =============================================================================

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "redis-complexity-analyzer",
    version = "0.1.0",
    about = "Analyze Redis database complexity and data type distribution"
)]
pub struct Config {
    /// Redis host to connect to
    #[clap(long, short = 'H', env = "REDIS_HOST", default_value = "localhost")]
    pub host: String,

    /// Redis port to connect to
    #[clap(long, short = 'P', env = "REDIS_PORT", default_value = "6379")]
    pub port: u16,

    /// Redis password (optional)
    #[clap(long, short = 'a', env = "REDIS_PASSWORD")]
    pub password: Option<String>,

    /// Redis database number
    #[clap(long, short = 'd', env = "REDIS_DB", default_value = "0")]
    pub db: u8,

    /// Sample percentage (0.01 to 1.0, default 0.05 = 5%)
    #[clap(long, short = 's', env = "SAMPLE_RATE", default_value = "0.05")]
    pub sample_rate: f64,

    /// Minimum samples before stopping (default 1000)
    #[clap(long, env = "MIN_SAMPLES", default_value = "1000")]
    pub min_samples: usize,

    /// Maximum samples to collect (default 100000)
    #[clap(long, env = "MAX_SAMPLES", default_value = "100000")]
    pub max_samples: usize,

    /// Output format: "console" or "json" (disables TUI)
    #[clap(long, short = 'o', env = "OUTPUT_FORMAT")]
    pub output_format: Option<OutputFormat>,

    /// Refresh interval in seconds for TUI mode
    #[clap(long, short = 'i', env = "INTERVAL", default_value = "5")]
    pub interval: u64,

    /// Run once and exit (disables TUI)
    #[clap(long)]
    pub once: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Console,
    Json,
}

// =============================================================================
// Redis Types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisType {
    // Core Redis types
    String,
    Hash,
    List,
    Set,
    #[serde(rename = "zset")]
    ZSet,
    Stream,

    // RedisJSON module
    #[serde(rename = "ReJSON-RL")]
    Json,

    // RedisTimeSeries module
    #[serde(rename = "TSDB-TYPE")]
    TimeSeries,

    // RedisBloom module (probabilistic data structures)
    #[serde(rename = "MBbloom--")]
    BloomFilter,
    #[serde(rename = "MBbloom--CF")]
    CuckooFilter,
    #[serde(rename = "MBbloom--CMS")]
    CountMinSketch,
    #[serde(rename = "MBbloom--TOPK")]
    TopK,
    #[serde(rename = "MBbloom--TDIGEST")]
    TDigest,

    // RedisGraph module
    #[serde(rename = "graphdata")]
    Graph,

    // RediSearch module
    #[serde(rename = "ft_invidx")]
    SearchIndex,

    // RedisGears module
    #[serde(rename = "GearsFunction")]
    GearsFunction,

    // Fallback for unknown/new types
    Unknown,
}

impl From<&str> for RedisType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            // Core Redis types
            "string" => RedisType::String,
            "hash" => RedisType::Hash,
            "list" => RedisType::List,
            "set" => RedisType::Set,
            "zset" => RedisType::ZSet,
            "stream" => RedisType::Stream,

            // RedisJSON - supports both legacy module and Redis 8+ native
            "rejson-rl" | "json" => RedisType::Json,

            // RedisTimeSeries - supports both legacy module and Redis 8+ native
            "tsdb-type" | "timeseries" | "ts" => RedisType::TimeSeries,

            // RedisBloom (probabilistic data structures)
            // Legacy module type strings
            "mbbloom--" | "bloom" => RedisType::BloomFilter,
            "mbbloom--cf" | "cuckoo" => RedisType::CuckooFilter,
            "mbbloom--cms" | "cms" | "countminsketch" => RedisType::CountMinSketch,
            "mbbloom--topk" | "topk" => RedisType::TopK,
            "mbbloom--tdigest" | "tdigest" => RedisType::TDigest,

            // RedisGraph (deprecated in Redis 8, but still support detection)
            "graphdata" | "graph" => RedisType::Graph,

            // RediSearch - supports both legacy module and Redis 8+ native
            "ft_invidx" | "ft_index" | "search" | "vectorset" => RedisType::SearchIndex,

            // RedisGears
            "gearsfunction" | "streamtrigger" | "gears" => RedisType::GearsFunction,

            // Unknown/other module types
            _ => RedisType::Unknown,
        }
    }
}

impl RedisType {
    fn display_name(&self) -> &'static str {
        match self {
            // Core Redis types
            RedisType::String => "String",
            RedisType::Hash => "Hash",
            RedisType::List => "List",
            RedisType::Set => "Set",
            RedisType::ZSet => "Sorted Set",
            RedisType::Stream => "Stream",

            // RedisJSON module
            RedisType::Json => "JSON",

            // RedisTimeSeries module
            RedisType::TimeSeries => "TimeSeries",

            // RedisBloom module
            RedisType::BloomFilter => "Bloom Filter",
            RedisType::CuckooFilter => "Cuckoo Filter",
            RedisType::CountMinSketch => "Count-Min Sketch",
            RedisType::TopK => "Top-K",
            RedisType::TDigest => "T-Digest",

            // RedisGraph module
            RedisType::Graph => "Graph",

            // RediSearch module
            RedisType::SearchIndex => "Search Index",

            // RedisGears module
            RedisType::GearsFunction => "Gears Function",

            // Unknown types
            RedisType::Unknown => "Unknown",
        }
    }
}

// =============================================================================
// Data Structures
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetrics {
    pub used_memory_bytes: u64,
    pub total_keys: u64,
    pub ops_per_sec: u64,
    pub redis_version: String,
    pub connected_clients: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDistribution {
    pub counts: HashMap<RedisType, u64>,
    pub total_sampled: u64,
}

impl TypeDistribution {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            total_sampled: 0,
        }
    }

    pub fn add(&mut self, key_type: RedisType) {
        *self.counts.entry(key_type).or_insert(0) += 1;
        self.total_sampled += 1;
    }

    pub fn percentage(&self, key_type: RedisType) -> f64 {
        if self.total_sampled == 0 {
            return 0.0;
        }
        let count = self.counts.get(&key_type).copied().unwrap_or(0);
        (count as f64 / self.total_sampled as f64) * 100.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub timestamp: String,
    pub host: String,
    pub port: u16,
    pub metrics: DatabaseMetrics,
    pub type_distribution: TypeDistribution,
    pub sample_coverage: f64,
    pub duration_ms: u64,
}

// =============================================================================
// TUI State
// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct HistoricalMetrics {
    // Maximum values observed
    pub max_memory_bytes: u64,
    pub max_keys: u64,
    pub max_ops_per_sec: u64,
    // Running averages
    pub avg_memory_bytes: f64,
    pub avg_keys: f64,
    pub avg_ops_per_sec: f64,
    // Sample count for averaging
    pub sample_count: u64,
}

impl HistoricalMetrics {
    fn update(&mut self, metrics: &DatabaseMetrics) {
        // Update maximums
        self.max_memory_bytes = self.max_memory_bytes.max(metrics.used_memory_bytes);
        self.max_keys = self.max_keys.max(metrics.total_keys);
        self.max_ops_per_sec = self.max_ops_per_sec.max(metrics.ops_per_sec);

        // Update running averages
        self.sample_count += 1;
        let n = self.sample_count as f64;
        self.avg_memory_bytes += (metrics.used_memory_bytes as f64 - self.avg_memory_bytes) / n;
        self.avg_keys += (metrics.total_keys as f64 - self.avg_keys) / n;
        self.avg_ops_per_sec += (metrics.ops_per_sec as f64 - self.avg_ops_per_sec) / n;
    }
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub result: Option<AnalysisResult>,
    pub current_metrics: Option<DatabaseMetrics>,
    pub historical: HistoricalMetrics,
    pub last_update: Option<Instant>,
    pub update_count: u64,
    pub error: Option<String>,
    pub is_sampling: bool,
}

impl AppState {
    fn new() -> Self {
        Self {
            result: None,
            current_metrics: None,
            historical: HistoricalMetrics::default(),
            last_update: None,
            update_count: 0,
            error: None,
            is_sampling: false,
        }
    }
}

// =============================================================================
// Redis Client
// =============================================================================

async fn connect_redis(config: &Config) -> Result<MultiplexedConnection> {
    let url = if let Some(ref password) = config.password {
        format!(
            "redis://:{}@{}:{}/{}",
            password, config.host, config.port, config.db
        )
    } else {
        format!("redis://{}:{}/{}", config.host, config.port, config.db)
    };

    let client = redis::Client::open(url).context("Failed to create Redis client")?;
    let conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to Redis")?;

    Ok(conn)
}

async fn fetch_info(conn: &mut MultiplexedConnection, section: &str) -> Result<String> {
    let info: String = redis::cmd("INFO")
        .arg(section)
        .query_async(conn)
        .await
        .context(format!("Failed to fetch INFO {}", section))?;
    Ok(info)
}

fn parse_info_field(info: &str, field: &str) -> Option<u64> {
    info.lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|val| val.trim().parse().ok())
}

fn parse_info_string(info: &str, field: &str) -> Option<String> {
    info.lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split(':').nth(1))
        .map(|val| val.trim().to_string())
}

async fn fetch_database_metrics(conn: &mut MultiplexedConnection) -> Result<DatabaseMetrics> {
    let memory_info = fetch_info(conn, "memory").await?;
    let server_info = fetch_info(conn, "server").await?;
    let clients_info = fetch_info(conn, "clients").await?;
    let stats_info = fetch_info(conn, "stats").await?;

    let used_memory_bytes = parse_info_field(&memory_info, "used_memory").unwrap_or(0);
    let connected_clients = parse_info_field(&clients_info, "connected_clients").unwrap_or(0);
    let ops_per_sec = parse_info_field(&stats_info, "instantaneous_ops_per_sec").unwrap_or(0);
    let redis_version =
        parse_info_string(&server_info, "redis_version").unwrap_or_else(|| "unknown".to_string());

    let total_keys: u64 = redis::cmd("DBSIZE")
        .query_async(conn)
        .await
        .context("Failed to fetch DBSIZE")?;

    Ok(DatabaseMetrics {
        used_memory_bytes,
        total_keys,
        ops_per_sec,
        redis_version,
        connected_clients,
    })
}

async fn sample_key_types(
    conn: &mut MultiplexedConnection,
    config: &Config,
    total_keys: u64,
) -> Result<TypeDistribution> {
    use rand::SeedableRng;

    let mut distribution = TypeDistribution::new();

    if total_keys == 0 {
        return Ok(distribution);
    }

    let mut cursor: u64 = 0;
    // Use StdRng which is Send-safe, seeded from system entropy
    let mut rng = rand::rngs::StdRng::from_entropy();

    // Calculate target samples
    let target_samples = ((total_keys as f64 * config.sample_rate) as usize)
        .max(config.min_samples)
        .min(config.max_samples)
        .min(total_keys as usize);

    loop {
        // Scan batch of keys
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(1000)
            .query_async(conn)
            .await
            .context("Failed to SCAN keys")?;

        // Probabilistically sample keys from this batch
        for key in keys {
            if rng.gen::<f64>() < config.sample_rate {
                let key_type: String = redis::cmd("TYPE")
                    .arg(&key)
                    .query_async(conn)
                    .await
                    .unwrap_or_else(|_| "unknown".to_string());

                distribution.add(RedisType::from(key_type.as_str()));

                // Check if we have enough samples
                if distribution.total_sampled >= target_samples as u64 {
                    return Ok(distribution);
                }
            }
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(distribution)
}

// =============================================================================
// Analysis
// =============================================================================

async fn analyze(conn: &mut MultiplexedConnection, config: &Config) -> Result<AnalysisResult> {
    let start = Instant::now();

    // Fetch database metrics
    let metrics = fetch_database_metrics(conn).await?;

    // Sample keys and determine types
    let type_distribution = sample_key_types(conn, config, metrics.total_keys).await?;

    let sample_coverage = if metrics.total_keys > 0 {
        (type_distribution.total_sampled as f64 / metrics.total_keys as f64) * 100.0
    } else {
        100.0
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(AnalysisResult {
        timestamp: chrono::Utc::now().to_rfc3339(),
        host: config.host.clone(),
        port: config.port,
        metrics,
        type_distribution,
        sample_coverage,
        duration_ms,
    })
}

// =============================================================================
// Formatting Helpers
// =============================================================================

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_memory(bytes: u64) -> String {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    let gb = mb / 1024.0;
    if gb >= 1.0 {
        format!("{:.2} GB", gb)
    } else {
        format!("{:.2} MB", mb)
    }
}

// =============================================================================
// TUI Rendering
// =============================================================================

fn ui(frame: &mut Frame, state: &AppState, config: &Config) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Length(8),  // Metrics (expanded for current/avg/max)
            Constraint::Min(10),    // Type distribution
            Constraint::Length(1),  // Footer
        ])
        .split(frame.area());

    render_header(frame, chunks[0], state, config);
    render_metrics(frame, chunks[1], state);
    render_type_distribution(frame, chunks[2], state);
    render_footer(frame, chunks[3], state, config);
}

fn render_header(frame: &mut Frame, area: Rect, state: &AppState, config: &Config) {
    let version = state
        .result
        .as_ref()
        .map(|r| r.metrics.redis_version.clone())
        .unwrap_or_else(|| "connecting...".to_string());

    let status = if state.is_sampling {
        Span::styled(" SAMPLING ", Style::default().bg(Color::Yellow).fg(Color::Black))
    } else if state.error.is_some() {
        Span::styled(" ERROR ", Style::default().bg(Color::Red).fg(Color::White))
    } else if state.result.is_some() {
        Span::styled(" CONNECTED ", Style::default().bg(Color::Green).fg(Color::Black))
    } else {
        Span::styled(" CONNECTING ", Style::default().bg(Color::Blue).fg(Color::White))
    };

    let title = Line::from(vec![
        Span::styled(
            " Redis Complexity Analyzer ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        status,
        Span::raw(format!("  {}:{} (v{})", config.host, config.port, version)),
    ]);

    let header = Paragraph::new(title).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    frame.render_widget(header, area);
}

fn render_metrics(frame: &mut Frame, area: Rect, state: &AppState) {
    let hist = &state.historical;
    let has_data = hist.sample_count > 0;

    let (curr_memory, curr_keys, curr_ops, clients) = state
        .current_metrics
        .as_ref()
        .map(|m| {
            (
                format_memory(m.used_memory_bytes),
                format_number(m.total_keys),
                format_number(m.ops_per_sec),
                m.connected_clients.to_string(),
            )
        })
        .unwrap_or_else(|| ("-".to_string(), "-".to_string(), "-".to_string(), "-".to_string()));

    let (max_memory, max_keys, max_ops) = if has_data {
        (
            format_memory(hist.max_memory_bytes),
            format_number(hist.max_keys),
            format_number(hist.max_ops_per_sec),
        )
    } else {
        ("-".to_string(), "-".to_string(), "-".to_string())
    };

    let (avg_memory, avg_keys, avg_ops) = if has_data {
        (
            format_memory(hist.avg_memory_bytes as u64),
            format_number(hist.avg_keys as u64),
            format_number(hist.avg_ops_per_sec as u64),
        )
    } else {
        ("-".to_string(), "-".to_string(), "-".to_string())
    };

    let header = Row::new(vec![
        Cell::from("Metric").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Cell::from("Current").style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Cell::from("Average").style(Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)),
        Cell::from("Maximum").style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
    ]);

    let rows = vec![
        header,
        Row::new(vec![
            Cell::from("Memory").style(Style::default().fg(Color::Yellow)),
            Cell::from(curr_memory).style(Style::default().fg(Color::Cyan)),
            Cell::from(avg_memory).style(Style::default().fg(Color::Blue)),
            Cell::from(max_memory).style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        ]),
        Row::new(vec![
            Cell::from("Keys").style(Style::default().fg(Color::Yellow)),
            Cell::from(curr_keys).style(Style::default().fg(Color::Cyan)),
            Cell::from(avg_keys).style(Style::default().fg(Color::Blue)),
            Cell::from(max_keys).style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        ]),
        Row::new(vec![
            Cell::from("Ops/sec").style(Style::default().fg(Color::Yellow)),
            Cell::from(curr_ops).style(Style::default().fg(Color::Cyan)),
            Cell::from(avg_ops).style(Style::default().fg(Color::Blue)),
            Cell::from(max_ops).style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        ]),
        Row::new(vec![
            Cell::from("Clients").style(Style::default().fg(Color::Yellow)),
            Cell::from(clients).style(Style::default().fg(Color::Cyan)),
            Cell::from("-").style(Style::default().fg(Color::DarkGray)),
            Cell::from("-").style(Style::default().fg(Color::DarkGray)),
        ]),
    ];

    let samples_info = if has_data {
        format!(" Database Metrics ({} samples) ", hist.sample_count)
    } else {
        " Database Metrics ".to_string()
    };

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
        ],
    )
    .block(
        Block::default()
            .title(samples_info)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    )
    .row_highlight_style(Style::default());

    frame.render_widget(table, area);
}

fn render_type_distribution(frame: &mut Frame, area: Rect, state: &AppState) {
    let types = [
        // Core Redis types
        RedisType::String,
        RedisType::Hash,
        RedisType::List,
        RedisType::Set,
        RedisType::ZSet,
        RedisType::Stream,
        // Module types
        RedisType::Json,
        RedisType::TimeSeries,
        RedisType::BloomFilter,
        RedisType::CuckooFilter,
        RedisType::CountMinSketch,
        RedisType::TopK,
        RedisType::TDigest,
        RedisType::Graph,
        RedisType::SearchIndex,
        RedisType::GearsFunction,
        RedisType::Unknown,
    ];

    let (sampled_info, rows): (String, Vec<Row>) = state
        .result
        .as_ref()
        .map(|r| {
            let info = format!(
                "{} sampled, {:.1}% coverage",
                format_number(r.type_distribution.total_sampled),
                r.sample_coverage
            );

            let rows: Vec<Row> = types
                .iter()
                .filter_map(|t| {
                    let pct = r.type_distribution.percentage(*t);
                    if pct > 0.0 {
                        let bar_width = (pct / 100.0 * 20.0) as usize;
                        let bar = "█".repeat(bar_width);
                        Some(Row::new(vec![
                            Cell::from(t.display_name()).style(Style::default().fg(Color::White)),
                            Cell::from(format!("{:>6.1}%", pct))
                                .style(Style::default().fg(Color::Cyan)),
                            Cell::from(bar).style(Style::default().fg(Color::Green)),
                        ]))
                    } else {
                        None
                    }
                })
                .collect();

            (info, rows)
        })
        .unwrap_or_else(|| ("No data".to_string(), vec![]));

    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Length(8),
            Constraint::Min(20),
        ],
    )
    .block(
        Block::default()
            .title(format!(" Type Distribution ({}) ", sampled_info))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );

    frame.render_widget(table, area);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &AppState, config: &Config) {
    let last_update = state
        .last_update
        .map(|t| format!("{:.1}s ago", t.elapsed().as_secs_f64()))
        .unwrap_or_else(|| "never".to_string());

    let error_text = state
        .error
        .as_ref()
        .map(|e| format!(" | Error: {}", e))
        .unwrap_or_default();

    let footer_text = format!(
        " Press 'q' to quit | Refresh: {}s | Last update: {} | Updates: {}{}",
        config.interval, last_update, state.update_count, error_text
    );

    let footer = Paragraph::new(footer_text).style(Style::default().fg(Color::DarkGray));

    frame.render_widget(footer, area);
}

// =============================================================================
// Console Output (non-TUI mode)
// =============================================================================

fn output_console(result: &AnalysisResult) {
    println!();
    println!("{}", "Redis Complexity Analyzer".bold().cyan());
    println!("{}", "=========================".cyan());
    println!();

    // Connection info
    println!(
        "{}: {}:{} (v{})",
        "Target".bold(),
        result.host,
        result.port,
        result.metrics.redis_version
    );
    println!();

    // Database metrics
    println!("{}", "Database Metrics".bold().yellow());
    println!("{}", "----------------".yellow());
    println!(
        "  Memory:     {}",
        format_memory(result.metrics.used_memory_bytes)
    );
    println!(
        "  Keys:       {}",
        format_number(result.metrics.total_keys)
    );
    println!(
        "  Ops/sec:    {}",
        format_number(result.metrics.ops_per_sec)
    );
    println!(
        "  Clients:    {}",
        result.metrics.connected_clients
    );
    println!();

    // Type distribution
    println!(
        "{} ({} sampled, {:.1}% coverage)",
        "Type Distribution".bold().yellow(),
        format_number(result.type_distribution.total_sampled),
        result.sample_coverage
    );
    println!("{}", "-".repeat(48).yellow());

    let types = [
        // Core Redis types
        RedisType::String,
        RedisType::Hash,
        RedisType::List,
        RedisType::Set,
        RedisType::ZSet,
        RedisType::Stream,
        // Module types
        RedisType::Json,
        RedisType::TimeSeries,
        RedisType::BloomFilter,
        RedisType::CuckooFilter,
        RedisType::CountMinSketch,
        RedisType::TopK,
        RedisType::TDigest,
        RedisType::Graph,
        RedisType::SearchIndex,
        RedisType::GearsFunction,
        RedisType::Unknown,
    ];

    for t in types {
        let pct = result.type_distribution.percentage(t);
        if pct > 0.0 {
            let bar_len = (pct / 5.0) as usize;
            let bar = "\u{2588}".repeat(bar_len);
            println!(
                "  {:16} {:>6.1}% {}",
                t.display_name(),
                pct,
                bar.green()
            );
        }
    }
    println!();

    // Analysis duration
    println!(
        "{}",
        format!("Completed in {}ms", result.duration_ms).dimmed()
    );
    println!();
}

fn output_json(result: &AnalysisResult) {
    match serde_json::to_string_pretty(result) {
        Ok(json) => println!("{}", json),
        Err(e) => eprintln!("Error serializing to JSON: {}", e),
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Handle one-shot or output format modes (non-TUI)
    if config.once || config.output_format.is_some() {
        let mut conn = connect_redis(&config).await?;

        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Failed to ping Redis server")?;

        let result = analyze(&mut conn, &config).await?;

        match config.output_format {
            Some(OutputFormat::Json) => output_json(&result),
            _ => output_console(&result),
        }

        return Ok(());
    }

    // TUI mode (default)
    run_tui(config).await
}

async fn run_tui(config: Config) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create shared state
    let state = Arc::new(RwLock::new(AppState::new()));

    // Clone for the background task
    let state_clone = Arc::clone(&state);
    let config_clone = config.clone();

    // Spawn background task for data fetching
    let fetch_handle = tokio::spawn(async move {
        let mut conn_result = connect_redis(&config_clone).await;

        loop {
            match &mut conn_result {
                Ok(conn) => {
                    // Set sampling flag
                    {
                        let mut state = state_clone.write().await;
                        state.is_sampling = true;
                        state.error = None;
                    }

                    match analyze(conn, &config_clone).await {
                        Ok(analysis) => {
                            let mut state = state_clone.write().await;

                            // Update historical tracking
                            state.historical.update(&analysis.metrics);
                            state.current_metrics = Some(analysis.metrics.clone());
                            state.result = Some(analysis);
                            state.last_update = Some(Instant::now());
                            state.update_count += 1;
                            state.is_sampling = false;
                            state.error = None;
                        }
                        Err(e) => {
                            let mut state = state_clone.write().await;
                            state.error = Some(e.to_string());
                            state.is_sampling = false;

                            // Try to reconnect
                            conn_result = connect_redis(&config_clone).await;
                        }
                    }
                }
                Err(e) => {
                    {
                        let mut state = state_clone.write().await;
                        state.error = Some(format!("Connection failed: {}", e));
                        state.is_sampling = false;
                    }

                    // Try to reconnect
                    sleep(Duration::from_secs(2)).await;
                    conn_result = connect_redis(&config_clone).await;
                    continue;
                }
            }

            sleep(Duration::from_secs(config_clone.interval)).await;
        }
    });

    // Main event loop
    let result = run_event_loop(&mut terminal, &state, &config).await;

    // Cleanup
    fetch_handle.abort();
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &Arc<RwLock<AppState>>,
    config: &Config,
) -> Result<()> {
    loop {
        // Draw UI
        {
            let state_guard = state.read().await;
            terminal.draw(|f| ui(f, &state_guard, config))?;
        }

        // Handle input with timeout for responsive UI
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            return Ok(());
                        }
                        KeyCode::Char('r') => {
                            // Force refresh (could add a flag to trigger immediate update)
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_type_from_string() {
        // Core types
        assert_eq!(RedisType::from("string"), RedisType::String);
        assert_eq!(RedisType::from("STRING"), RedisType::String);
        assert_eq!(RedisType::from("hash"), RedisType::Hash);
        assert_eq!(RedisType::from("list"), RedisType::List);
        assert_eq!(RedisType::from("set"), RedisType::Set);
        assert_eq!(RedisType::from("zset"), RedisType::ZSet);
        assert_eq!(RedisType::from("stream"), RedisType::Stream);

        // Legacy module type strings
        assert_eq!(RedisType::from("ReJSON-RL"), RedisType::Json);
        assert_eq!(RedisType::from("TSDB-TYPE"), RedisType::TimeSeries);
        assert_eq!(RedisType::from("MBbloom--"), RedisType::BloomFilter);
        assert_eq!(RedisType::from("graphdata"), RedisType::Graph);
        assert_eq!(RedisType::from("ft_invidx"), RedisType::SearchIndex);

        // Redis 8+ native type strings
        assert_eq!(RedisType::from("json"), RedisType::Json);
        assert_eq!(RedisType::from("timeseries"), RedisType::TimeSeries);
        assert_eq!(RedisType::from("bloom"), RedisType::BloomFilter);
        assert_eq!(RedisType::from("cuckoo"), RedisType::CuckooFilter);
        assert_eq!(RedisType::from("topk"), RedisType::TopK);
        assert_eq!(RedisType::from("tdigest"), RedisType::TDigest);
        assert_eq!(RedisType::from("graph"), RedisType::Graph);
        assert_eq!(RedisType::from("search"), RedisType::SearchIndex);

        // Unknown types
        assert_eq!(RedisType::from("unknown_type"), RedisType::Unknown);
    }

    #[test]
    fn test_type_distribution() {
        let mut dist = TypeDistribution::new();
        dist.add(RedisType::String);
        dist.add(RedisType::String);
        dist.add(RedisType::Hash);
        dist.add(RedisType::Hash);

        assert_eq!(dist.total_sampled, 4);
        assert_eq!(dist.percentage(RedisType::String), 50.0);
        assert_eq!(dist.percentage(RedisType::Hash), 50.0);
        assert_eq!(dist.percentage(RedisType::List), 0.0);
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.50K");
        assert_eq!(format_number(1_500_000), "1.50M");
        assert_eq!(format_number(1_500_000_000), "1.50B");
    }

    #[test]
    fn test_format_memory() {
        assert_eq!(format_memory(500 * 1024 * 1024), "500.00 MB");
        assert_eq!(format_memory(2 * 1024 * 1024 * 1024), "2.00 GB");
    }
}
