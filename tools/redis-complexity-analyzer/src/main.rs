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
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
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
// Customer Complexity Types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityFinding {
    pub category: String,
    pub severity: Severity,
    pub title: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomerComplexity {
    pub findings: Vec<ComplexityFinding>,
    pub cluster_mode: Option<String>,
    pub acl_rules: Vec<String>,
    pub loaded_modules: Vec<String>,
    pub persistence_config: PersistenceConfig,
    pub keyspace_notifications: String,
    pub max_memory_policy: String,
    pub total_score: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub rdb_enabled: bool,
    pub aof_enabled: bool,
    pub rdb_save_params: String,
    pub aof_fsync: String,
}

impl CustomerComplexity {
    fn new() -> Self {
        Self {
            findings: Vec::new(),
            cluster_mode: None,
            acl_rules: Vec::new(),
            loaded_modules: Vec::new(),
            persistence_config: PersistenceConfig::default(),
            keyspace_notifications: String::new(),
            max_memory_policy: String::new(),
            total_score: 0,
        }
    }

    fn add_finding(&mut self, category: &str, severity: Severity, title: &str, detail: &str) {
        let score = match severity {
            Severity::Info => 1,
            Severity::Warning => 3,
            Severity::Critical => 5,
        };
        self.total_score += score;
        self.findings.push(ComplexityFinding {
            category: category.to_string(),
            severity,
            title: title.to_string(),
            detail: detail.to_string(),
        });
    }
}

// =============================================================================
// Pricing Tier Types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PricingTier {
    Simple,
    Moderate,
    Difficult,
    Complex,
}

impl PricingTier {
    /// Determine pricing tier from complexity score
    fn from_score(score: u32) -> Self {
        match score {
            0..=10 => PricingTier::Simple,
            11..=25 => PricingTier::Moderate,
            26..=50 => PricingTier::Difficult,
            _ => PricingTier::Complex,
        }
    }

    /// Complexity multiplier applied on top of the 10% base price
    fn complexity_multiplier(&self) -> f64 {
        match self {
            PricingTier::Simple => 1.0,
            PricingTier::Moderate => 1.5,
            PricingTier::Difficult => 2.0,
            PricingTier::Complex => 2.5,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            PricingTier::Simple => "Simple (1.0x)",
            PricingTier::Moderate => "Moderate (1.5x)",
            PricingTier::Difficult => "Difficult (2.0x)",
            PricingTier::Complex => "Complex (2.5x)",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureSkuPrice {
    pub sku_name: String,
    pub product_name: String,
    pub retail_price: f64,
    pub unit_of_measure: String,
    pub region: String,
    pub meter_name: String,
}

impl AzureSkuPrice {
    /// Calculate annual cost from hourly retail price
    fn annual_cost(&self) -> f64 {
        // ACR is billed per hour, 8760 hours/year
        self.retail_price * 8760.0
    }

    /// Base Exodus price is 10% of annual Azure spend (min $2,500)
    fn exodus_base_price(&self) -> f64 {
        let raw = self.annual_cost() * 0.10;
        ((raw / 100.0).round() * 100.0).max(2500.0)
    }

    /// True if this is an Azure Managed Redis (AMR) SKU (not legacy ACR)
    fn is_amr(&self) -> bool {
        self.product_name.to_lowercase().contains("managed")
    }
}

const MANUAL_HOURLY_RATE: f64 = 200.0;
const MANUAL_HOURS_PER_WEEK: f64 = 10.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTimeEstimate {
    pub data_size_gb: f64,
    /// Exodus: 1 hour setup + 24 hours per TB of data
    pub exodus_setup_hours: f64,
    pub exodus_migration_hours: f64,
    pub exodus_total_hours: f64,
    /// Manual: 40 hours over 4 weeks baseline (10 hrs/week), scales with complexity
    pub manual_weeks: f64,
    pub manual_hours: f64,
    pub manual_cost: f64, // manual_hours × $200/hr
}

impl MigrationTimeEstimate {
    fn calculate(data_size_bytes: u64, tier: PricingTier) -> Self {
        let data_size_gb = data_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let data_size_tb = data_size_gb / 1024.0;

        // Exodus: < 1 hour setup + 24 hours per TB (scales linearly)
        let exodus_setup_hours = 1.0;
        let exodus_migration_hours = data_size_tb * 24.0;
        let exodus_total_hours = exodus_setup_hours + exodus_migration_hours;

        // Manual: 40 hours (10 hrs/week × 4 weeks) baseline, scales with complexity
        // More complex = more weeks at same 10 hrs/week pace
        let manual_weeks = match tier {
            PricingTier::Simple =>    4.0,
            PricingTier::Moderate =>  6.0,
            PricingTier::Difficult => 8.0,
            PricingTier::Complex =>  12.0,
        };
        let manual_hours = manual_weeks * MANUAL_HOURS_PER_WEEK;
        let manual_cost = manual_hours * MANUAL_HOURLY_RATE;

        Self {
            data_size_gb,
            exodus_setup_hours,
            exodus_migration_hours,
            exodus_total_hours,
            manual_weeks,
            manual_hours,
            manual_cost,
        }
    }

    fn exodus_summary(&self) -> String {
        if self.exodus_total_hours < 2.0 {
            format!("{:.0} mins", self.exodus_total_hours * 60.0)
        } else if self.exodus_total_hours < 48.0 {
            format!("{:.1} hours", self.exodus_total_hours)
        } else {
            format!("{:.1} days", self.exodus_total_hours / 24.0)
        }
    }

    fn manual_summary(&self) -> String {
        format!("{:.0} hrs over {:.0} weeks", self.manual_hours, self.manual_weeks)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricingEstimate {
    pub selected_sku: AzureSkuPrice,
    pub annual_azure_spend: f64,
    pub base_price: f64,            // always 10% of annual spend, rounded to $1k
    pub complexity_multiplier: f64, // from PricingTier (1.0x–2.5x)
    pub estimated_price: f64,       // base_price * complexity_multiplier, rounded to $1k
    pub tier: PricingTier,
    pub migration_time: Option<MigrationTimeEstimate>,
}

impl PricingEstimate {
    fn calculate(sku: &AzureSkuPrice, tier: PricingTier, data_size_bytes: Option<u64>) -> Self {
        let annual = sku.annual_cost();
        let base_price = sku.exodus_base_price();
        let multiplier = tier.complexity_multiplier();
        let raw_estimate = base_price * multiplier;
        // Cap at 20% of annual Azure spend, floor at $2,500
        let cap = ((annual * 0.20) / 100.0).round() * 100.0;
        let estimated_price = ((raw_estimate / 100.0).round() * 100.0).min(cap).max(2500.0);
        let migration_time = data_size_bytes.map(|bytes| MigrationTimeEstimate::calculate(bytes, tier));
        Self {
            selected_sku: sku.clone(),
            annual_azure_spend: annual,
            base_price,
            complexity_multiplier: multiplier,
            estimated_price,
            tier,
            migration_time,
        }
    }
}

// =============================================================================
// AMR Workload Profile & Recommendation
// =============================================================================

/// Workload classification based on ops_per_sec / used_memory_mb ratio.
/// Matches the eden-dev classification from analysis.rs / workload.rs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AmrProfile {
    Memory,   // ratio < 1.0  → Memory-optimized (M-series)
    Balanced, // ratio 1.0–50.0 → General Purpose (B/P-series)
    Compute,  // ratio > 50.0 → Compute-optimized (X/C-series)
}

impl AmrProfile {
    fn from_ratio(ratio: f64) -> Self {
        if ratio < 1.0 {
            AmrProfile::Memory
        } else if ratio <= 50.0 {
            AmrProfile::Balanced
        } else {
            AmrProfile::Compute
        }
    }

    fn label(&self) -> &'static str {
        match self {
            AmrProfile::Memory => "Memory Optimized",
            AmrProfile::Balanced => "Balanced / General Purpose",
            AmrProfile::Compute => "Compute Optimized",
        }
    }

    fn sku_prefix_hint(&self) -> &'static str {
        match self {
            AmrProfile::Memory => "M",
            AmrProfile::Balanced => "B",
            AmrProfile::Compute => "X",
        }
    }

    /// Match an Azure SKU product name to this profile
    fn matches_sku(&self, product_name: &str) -> bool {
        let p = product_name.to_lowercase();
        match self {
            AmrProfile::Memory => p.contains("memory"),
            AmrProfile::Balanced => {
                p.contains("balanced") || p.contains("standard") || p.contains("premium") || p.contains("basic")
            }
            AmrProfile::Compute => p.contains("compute"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmrRecommendation {
    pub profile: AmrProfile,
    pub ratio: f64,
    pub current_memory_gb: f64,
    pub current_ops: u64,
    pub overprovision_pct: u32,
    pub target_memory_gb: f64,
    pub recommended_sku: Option<AzureSkuPrice>,
    pub recommended_annual_cost: f64,
    pub current_sku_annual_cost: Option<f64>,
}

impl AmrRecommendation {
    fn calculate(
        metrics: &DatabaseMetrics,
        skus: &[AzureSkuPrice],
        overprovision_pct: u32,
        current_sku: Option<&AzureSkuPrice>,
    ) -> Self {
        let memory_mb = metrics.used_memory_bytes as f64 / (1024.0 * 1024.0);
        let memory_gb = memory_mb / 1024.0;
        let ops = metrics.ops_per_sec;

        let ratio = if memory_mb > 0.0 {
            ops as f64 / memory_mb
        } else {
            0.0
        };

        let profile = AmrProfile::from_ratio(ratio);
        let target_memory_gb = memory_gb * (1.0 + overprovision_pct as f64 / 100.0);

        // Find the cheapest SKU that matches the profile and has enough memory.
        // SKU meter names encode the tier size — we match on product name for profile
        // and filter by annual cost as a proxy for capacity (larger SKUs cost more).
        // For a proper match we'd parse the SKU GB from the name, but since SKUs are
        // already sorted by cost ascending we pick the cheapest profile-matching SKU
        // whose capacity meets target_memory_gb.
        //
        // Azure SKU naming: M10 = 10GB memory-optimized, B50 = 50GB balanced, etc.
        // We parse the numeric suffix as GB capacity.
        let recommended = skus
            .iter()
            .filter(|s| profile.matches_sku(&s.product_name))
            .find(|s| {
                let capacity_gb = parse_sku_capacity_gb(&s.sku_name);
                capacity_gb >= target_memory_gb
            })
            .or_else(|| {
                // Fallback: if no profile match, pick any SKU with enough capacity
                skus.iter().find(|s| {
                    let capacity_gb = parse_sku_capacity_gb(&s.sku_name);
                    capacity_gb >= target_memory_gb
                })
            });

        let recommended_annual_cost = recommended
            .map(|s| s.annual_cost())
            .unwrap_or(0.0);

        AmrRecommendation {
            profile,
            ratio,
            current_memory_gb: memory_gb,
            current_ops: ops,
            overprovision_pct,
            target_memory_gb,
            recommended_sku: recommended.cloned(),
            recommended_annual_cost,
            current_sku_annual_cost: current_sku.map(|s| s.annual_cost()),
        }
    }
}

/// Parse capacity in GB from an Azure SKU name like "M10", "B50", "X100", "C6", "P3", "E20".
/// For C-series (Basic/Standard): C0=250MB, C1=1GB, C2=2.5GB, C3=6GB, C4=13GB, C5=26GB, C6=53GB
/// For P-series (Premium): P1=6GB, P2=13GB, P3=26GB, P4=53GB, P5=120GB
/// For managed AMR: the numeric suffix IS the GB (M10=10GB, B50=50GB, etc.)
fn parse_sku_capacity_gb(sku_name: &str) -> f64 {
    let name = sku_name.trim();

    // C-series (Basic/Standard)
    if name.starts_with('C') || name.starts_with('c') {
        return match name.get(1..).and_then(|n| n.parse::<u32>().ok()) {
            Some(0) => 0.25,
            Some(1) => 1.0,
            Some(2) => 2.5,
            Some(3) => 6.0,
            Some(4) => 13.0,
            Some(5) => 26.0,
            Some(6) => 53.0,
            _ => 0.0,
        };
    }

    // P-series (Premium)
    if name.starts_with('P') || name.starts_with('p') {
        return match name.get(1..).and_then(|n| n.parse::<u32>().ok()) {
            Some(1) => 6.0,
            Some(2) => 13.0,
            Some(3) => 26.0,
            Some(4) => 53.0,
            Some(5) => 120.0,
            _ => 0.0,
        };
    }

    // E-series (Enterprise)
    if name.starts_with('E') || name.starts_with('e') {
        return match name.get(1..).and_then(|n| n.parse::<u32>().ok()) {
            Some(1) => 1.0,   // E1 Internal
            Some(5) => 5.0,
            Some(10) => 10.0,
            Some(20) => 20.0,
            Some(50) => 50.0,
            Some(100) => 100.0,
            Some(200) => 200.0,
            Some(400) => 400.0,
            _ => 0.0,
        };
    }

    // F-series (Enterprise Flash): F300=300GB, F700=700GB, F1500=1500GB
    if name.starts_with('F') || name.starts_with('f') {
        if let Some(n) = name.get(1..).and_then(|n| n.parse::<f64>().ok()) {
            return n;
        }
    }

    // Managed AMR: M10=10GB, B50=50GB, X100=100GB, A250=250GB, I100=100GB
    // Numeric suffix is the capacity in GB
    if let Some(n) = name.get(1..).and_then(|n| n.parse::<f64>().ok()) {
        return n;
    }

    0.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiView {
    Analysis,
    Complexity,
    Pricing,
    Recommend,
    Summary,
    Docs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PricingFocus {
    Region,
    Sku,
}

const AZURE_REGIONS: &[(&str, &str)] = &[
    ("eastus", "East US"),
    ("eastus2", "East US 2"),
    ("westus", "West US"),
    ("westus2", "West US 2"),
    ("westus3", "West US 3"),
    ("centralus", "Central US"),
    ("northcentralus", "North Central US"),
    ("southcentralus", "South Central US"),
    ("canadacentral", "Canada Central"),
    ("canadaeast", "Canada East"),
    ("westeurope", "West Europe"),
    ("northeurope", "North Europe"),
    ("uksouth", "UK South"),
    ("ukwest", "UK West"),
    ("francecentral", "France Central"),
    ("germanywestcentral", "Germany West Central"),
    ("swedencentral", "Sweden Central"),
    ("norwayeast", "Norway East"),
    ("switzerlandnorth", "Switzerland North"),
    ("eastasia", "East Asia"),
    ("southeastasia", "Southeast Asia"),
    ("japaneast", "Japan East"),
    ("japanwest", "Japan West"),
    ("australiaeast", "Australia East"),
    ("australiasoutheast", "Australia Southeast"),
    ("koreacentral", "Korea Central"),
    ("centralindia", "Central India"),
    ("southindia", "South India"),
    ("brazilsouth", "Brazil South"),
    ("southafricanorth", "South Africa North"),
    ("uaenorth", "UAE North"),
];

// =============================================================================
// CLI Configuration
// =============================================================================

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "eden-redis-migration-analyzer",
    version = "0.1.0",
    about = "Eden — Redis-to-Azure migration analysis, complexity scoring, and pricing"
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

    /// Redis username for ACL auth (Redis 6+)
    #[clap(long, short = 'u', env = "REDIS_USERNAME")]
    pub username: Option<String>,

    /// Enable TLS/SSL connection
    #[clap(long, env = "REDIS_TLS")]
    pub tls: bool,

    /// Allow invalid TLS certificates (for self-signed certs)
    #[clap(long, env = "REDIS_TLS_INSECURE")]
    pub tls_insecure: bool,

    /// Redis database number
    #[clap(long, short = 'n', env = "REDIS_DB", default_value = "0")]
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

    /// Azure region for pricing lookup in --once mode (e.g., "eastus", "westus2")
    /// In TUI mode, select the region interactively on the Pricing tab.
    #[clap(long, default_value = "eastus")]
    pub azure_region: String,
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
    pub complexity: Option<CustomerComplexity>,
    pub active_view: TuiView,
    pub complexity_scroll: u16,
    // Azure pricing state
    pub azure_skus: Vec<AzureSkuPrice>,      // all SKUs (for recommend tab)
    pub azure_acr_skus: Vec<AzureSkuPrice>,  // ACR only, no AMR (for pricing tab)
    pub azure_selected_sku: usize,           // indexes into azure_acr_skus
    pub azure_confirmed_sku: Option<usize>,  // indexes into azure_acr_skus
    pub azure_selected_region: usize,
    pub azure_confirmed_region: Option<usize>,
    pub azure_loading: bool,
    pub azure_error: Option<String>,
    pub pricing_estimate: Option<PricingEstimate>,
    pub pricing_focus: PricingFocus,
    // Recommendation state
    pub overprovision_pct: u32,
    pub db_size_override_mb: Option<u64>, // manual override in MB, None = use detected
    pub recommendation: Option<AmrRecommendation>,
    // Docs view
    pub docs_scroll: u16,
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
            complexity: None,
            active_view: TuiView::Analysis,
            complexity_scroll: 0,
            azure_skus: Vec::new(),
            azure_acr_skus: Vec::new(),
            azure_selected_sku: 0,
            azure_confirmed_sku: None,
            azure_selected_region: 0,
            azure_confirmed_region: None,
            azure_loading: false,
            azure_error: None,
            pricing_estimate: None,
            pricing_focus: PricingFocus::Region,
            overprovision_pct: 25,
            db_size_override_mb: None,
            recommendation: None,
            docs_scroll: 0,
        }
    }

    /// Build metrics from observed maximums — used for sizing and pricing
    /// since peak values represent what you actually need to provision for.
    fn peak_metrics(&self) -> Option<DatabaseMetrics> {
        self.current_metrics.as_ref().map(|current| {
            DatabaseMetrics {
                used_memory_bytes: self.historical.max_memory_bytes.max(current.used_memory_bytes),
                total_keys: self.historical.max_keys.max(current.total_keys),
                ops_per_sec: self.historical.max_ops_per_sec.max(current.ops_per_sec),
                redis_version: current.redis_version.clone(),
                connected_clients: current.connected_clients,
            }
        })
    }

    fn update_pricing_estimate(&mut self) {
        let sku_idx = self.azure_confirmed_sku.unwrap_or(self.azure_selected_sku);
        if let (Some(sku), Some(complexity)) = (
            self.azure_acr_skus.get(sku_idx),
            &self.complexity,
        ) {
            let tier = PricingTier::from_score(complexity.total_score);
            let data_size = self.peak_metrics().map(|m| m.used_memory_bytes);
            self.pricing_estimate = Some(PricingEstimate::calculate(sku, tier, data_size));
        }
    }

    fn update_recommendation(&mut self) {
        if let Some(peak) = self.peak_metrics() {
            let mut effective_metrics = peak;
            if let Some(override_mb) = self.db_size_override_mb {
                effective_metrics.used_memory_bytes = override_mb * 1024 * 1024;
            }
            let current_sku = self.azure_confirmed_sku
                .and_then(|i| self.azure_acr_skus.get(i));
            self.recommendation = Some(AmrRecommendation::calculate(
                &effective_metrics,
                &self.azure_skus, // recommend from all SKUs including AMR
                self.overprovision_pct,
                current_sku,
            ));
        }
    }
}

// =============================================================================
// Redis Client
// =============================================================================

async fn connect_redis(config: &Config) -> Result<MultiplexedConnection> {
    let scheme = if config.tls { "rediss" } else { "redis" };

    let auth = match (&config.username, &config.password) {
        (Some(user), Some(pass)) => format!("{}:{}@", user, pass),
        (None, Some(pass)) => format!(":{}@", pass),
        _ => String::new(),
    };

    let mut url = format!("{}://{}{}:{}/{}", scheme, auth, config.host, config.port, config.db);

    // For TLS with insecure mode (self-signed certs)
    if config.tls && config.tls_insecure {
        url.push_str("#insecure");
    }

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
// Customer Complexity Analysis
// =============================================================================

/// Modules not available in any AMR tier (per MS docs: only RediSearch, RedisBloom,
/// RedisTimeSeries, and RedisJSON are offered).
const AMR_UNAVAILABLE_MODULES: &[&str] = &[
    "redisgears",
    "graph",       // RedisGraph — deprecated, not in AMR
];

/// Modules not available on AMR Flash Optimized tier specifically.
/// Per MS docs: Flash Optimized only supports RedisJSON (and RediSearch in preview).
const AMR_FLASH_UNAVAILABLE_MODULES: &[&str] = &[
    "bf",          // RedisBloom
    "timeseries",  // RedisTimeSeries
    "search",      // RediSearch (only preview on Flash)
];

async fn analyze_customer_complexity(
    conn: &mut MultiplexedConnection,
    metrics: &DatabaseMetrics,
) -> Result<CustomerComplexity> {
    let mut complexity = CustomerComplexity::new();

    // --- Clustering Analysis ---
    analyze_clustering(conn, &mut complexity).await;

    // --- ACL Rules ---
    analyze_acls(conn, &mut complexity).await;

    // --- Loaded Modules ---
    analyze_modules(conn, &mut complexity).await;

    // --- Keyspace Notifications ---
    analyze_keyspace_notifications(conn, &mut complexity).await;

    // --- Persistence Configuration ---
    analyze_persistence(conn, &mut complexity).await;

    // --- Max Memory Policy ---
    analyze_memory_policy(conn, &mut complexity).await;

    // --- Lua Scripts ---
    analyze_lua_scripts(conn, &mut complexity).await;

    // --- Pub/Sub Channels ---
    analyze_pubsub(conn, &mut complexity).await;

    // --- Connection & Protocol ---
    analyze_connection_protocol(conn, &mut complexity).await;

    // --- Multi-key Command Usage ---
    analyze_multikey_commands(conn, &mut complexity).await;

    // --- Data Size ---
    analyze_data_size(&mut complexity, metrics);

    // --- Throughput ---
    analyze_throughput(&mut complexity, metrics);

    Ok(complexity)
}

fn analyze_data_size(complexity: &mut CustomerComplexity, metrics: &DatabaseMetrics) {
    let gb = metrics.used_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    if gb >= 100.0 {
        complexity.add_finding(
            "Scale",
            Severity::Critical,
            &format!("Large Dataset ({:.1} GB)", gb),
            "Datasets over 100 GB require careful tier selection, longer migration \
             windows, and higher risk of timeout during transfer. Enterprise or \
             Memory Optimized AMR tiers recommended.",
        );
    } else if gb >= 25.0 {
        complexity.add_finding(
            "Scale",
            Severity::Warning,
            &format!("Medium Dataset ({:.1} GB)", gb),
            "Dataset exceeds 25 GB — Non-Clustered AMR policy is not available. \
             Must use OSS Cluster or Enterprise clustering policy.",
        );
    } else if gb >= 1.0 {
        complexity.add_finding(
            "Scale",
            Severity::Warning,
            &format!("Dataset Size: {:.1} GB", gb),
            "Moderate dataset size. Verify target AMR SKU has sufficient memory \
             with headroom for fragmentation overhead.",
        );
    } else {
        complexity.add_finding(
            "Scale",
            Severity::Info,
            &format!("Small Dataset ({:.2} GB)", gb),
            "Small dataset — quick migration with minimal risk.",
        );
    }
}

fn analyze_throughput(complexity: &mut CustomerComplexity, metrics: &DatabaseMetrics) {
    let ops = metrics.ops_per_sec;

    if ops >= 50_000 {
        complexity.add_finding(
            "Scale",
            Severity::Critical,
            &format!("High Throughput ({} ops/sec)", format_number(ops)),
            "Very high throughput requires careful AMR tier sizing and tight \
             cutover coordination to avoid data loss during switchover. \
             Consider Compute Optimized or Enterprise tier.",
        );
    } else if ops >= 10_000 {
        complexity.add_finding(
            "Scale",
            Severity::Warning,
            &format!("Elevated Throughput ({} ops/sec)", format_number(ops)),
            "Elevated throughput — plan for a rapid cutover window and verify \
             the target AMR SKU can sustain this load.",
        );
    } else if ops >= 1_000 {
        complexity.add_finding(
            "Scale",
            Severity::Warning,
            &format!("Throughput: {} ops/sec", format_number(ops)),
            "Moderate throughput. Verify target AMR tier supports this level.",
        );
    } else {
        complexity.add_finding(
            "Scale",
            Severity::Info,
            &format!("Low Throughput ({} ops/sec)", format_number(ops)),
            "Low throughput — flexible cutover window.",
        );
    }
}

async fn analyze_clustering(conn: &mut MultiplexedConnection, complexity: &mut CustomerComplexity) {
    // Try CLUSTER INFO to detect clustering mode
    let cluster_info: Result<String, _> = redis::cmd("CLUSTER")
        .arg("INFO")
        .query_async(conn)
        .await;

    match cluster_info {
        Ok(info) => {
            let state = info
                .lines()
                .find(|l| l.starts_with("cluster_enabled:"))
                .and_then(|l| l.split(':').nth(1))
                .map(|v| v.trim().to_string())
                .unwrap_or_else(|| "unknown".to_string());

            if state == "1" {
                complexity.cluster_mode = Some("OSS Cluster".to_string());

                // Check cluster size
                let cluster_size = info
                    .lines()
                    .find(|l| l.starts_with("cluster_size:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(0);

                let slots_ok = info
                    .lines()
                    .find(|l| l.starts_with("cluster_slots_ok:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(0);

                complexity.add_finding(
                    "Clustering",
                    Severity::Critical,
                    "OSS Cluster Mode Enabled",
                    &format!(
                        "Cluster has {} shards with {} slots assigned. AMR supports OSS, \
                         Enterprise, and Non-Clustered policies. OSS policy uses port 10000 \
                         (initial) and 85XX (per-shard) — different from ACR's 6380/13XXX. \
                         Client library must support the Redis Cluster API. Multi-key commands \
                         must target the same hash slot or will receive CROSSSLOT errors.",
                        cluster_size, slots_ok
                    ),
                );
            } else {
                complexity.cluster_mode = Some("Standalone".to_string());
                complexity.add_finding(
                    "Clustering",
                    Severity::Info,
                    "Standalone Mode",
                    "No clustering detected. AMR Non-Clustered policy (<=25GB) or \
                     Enterprise policy can be used for a simple migration path.",
                );
            }
        }
        Err(_) => {
            complexity.cluster_mode = Some("Standalone (cluster disabled)".to_string());
            complexity.add_finding(
                "Clustering",
                Severity::Info,
                "Cluster Commands Disabled",
                "CLUSTER INFO not available — likely standalone mode.",
            );
        }
    }

    // Check for read replicas via INFO replication
    let repl_info: Result<String, _> = redis::cmd("INFO")
        .arg("replication")
        .query_async(conn)
        .await;

    if let Ok(info) = repl_info {
        let role = parse_info_string(&info, "role").unwrap_or_default();
        let connected_slaves = parse_info_field(&info, "connected_slaves").unwrap_or(0);

        if role == "master" && connected_slaves > 0 {
            complexity.add_finding(
                "Clustering",
                Severity::Warning,
                &format!("{} Read Replica(s) Detected", connected_slaves),
                "Read replicas are configured. Ensure AMR tier supports the same \
                 replica count and that client read/write splitting is preserved.",
            );
        }
    }
}

async fn analyze_acls(conn: &mut MultiplexedConnection, complexity: &mut CustomerComplexity) {
    let acl_list: Result<Vec<String>, _> = redis::cmd("ACL")
        .arg("LIST")
        .query_async(conn)
        .await;

    match acl_list {
        Ok(rules) => {
            let non_default: Vec<String> = rules
                .iter()
                .filter(|r| !r.starts_with("user default"))
                .cloned()
                .collect();

            complexity.acl_rules = rules;

            if !non_default.is_empty() {
                complexity.add_finding(
                    "ACLs",
                    Severity::Critical,
                    &format!("{} Custom ACL Rule(s)", non_default.len()),
                    "Custom Redis ACL rules detected. AMR does NOT support Redis ACL \
                     RBAC — it uses Microsoft Entra ID authentication instead. These \
                     rules cannot be migrated and must be recreated using Entra ID \
                     roles. Export rules for reference before migration.",
                );
            } else {
                complexity.add_finding(
                    "ACLs",
                    Severity::Info,
                    "Default ACLs Only",
                    "No custom ACL rules. AMR uses Microsoft Entra ID for auth — \
                     update connection code to use Entra ID instead of access keys.",
                );
            }
        }
        Err(_) => {
            complexity.add_finding(
                "ACLs",
                Severity::Info,
                "ACL Not Available",
                "ACL commands not supported on this Redis version.",
            );
        }
    }
}

async fn analyze_modules(conn: &mut MultiplexedConnection, complexity: &mut CustomerComplexity) {
    let module_list: Result<Vec<redis::Value>, _> = redis::cmd("MODULE")
        .arg("LIST")
        .query_async(conn)
        .await;

    if let Ok(modules) = module_list {
        let mut module_names: Vec<String> = Vec::new();

        for module in &modules {
            if let redis::Value::Array(ref fields) = module {
                // MODULE LIST returns arrays of [name, value, ...] pairs
                for chunk in fields.chunks(2) {
                    if let [redis::Value::BulkString(key), redis::Value::BulkString(val)] = chunk {
                        if String::from_utf8_lossy(key) == "name" {
                            module_names.push(String::from_utf8_lossy(val).to_string());
                        }
                    }
                }
            }
        }

        complexity.loaded_modules = module_names.clone();

        for name in &module_names {
            let lower = name.to_lowercase();
            if AMR_UNAVAILABLE_MODULES.iter().any(|m| lower.contains(m)) {
                complexity.add_finding(
                    "Modules",
                    Severity::Critical,
                    &format!("Unavailable Module: {}", name),
                    &format!(
                        "Module '{}' is NOT available in any AMR tier. AMR only supports \
                         RediSearch, RedisBloom, RedisTimeSeries, and RedisJSON. Workloads \
                         depending on this module will need alternatives.",
                        name
                    ),
                );
            } else if AMR_FLASH_UNAVAILABLE_MODULES.iter().any(|m| lower.contains(m)) {
                complexity.add_finding(
                    "Modules",
                    Severity::Warning,
                    &format!("Module: {} (Flash limit)", name),
                    &format!(
                        "Module '{}' is available in Memory Optimized, Balanced, and \
                         Compute Optimized AMR tiers but NOT on Flash Optimized. \
                         If targeting Flash Optimized tier, this module cannot be used.",
                        name
                    ),
                );
            } else {
                complexity.add_finding(
                    "Modules",
                    Severity::Info,
                    &format!("Module: {}", name),
                    &format!(
                        "Module '{}' is supported in AMR. Must be enabled at cache \
                         creation time — modules cannot be added to existing instances.",
                        name
                    ),
                );
            }
        }

        if module_names.is_empty() {
            complexity.add_finding(
                "Modules",
                Severity::Info,
                "No Custom Modules",
                "No additional modules loaded.",
            );
        }
    }
}

async fn analyze_keyspace_notifications(
    conn: &mut MultiplexedConnection,
    complexity: &mut CustomerComplexity,
) {
    let config: Result<Vec<String>, _> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("notify-keyspace-events")
        .query_async(conn)
        .await;

    if let Ok(vals) = config {
        let value = vals.get(1).cloned().unwrap_or_default();
        complexity.keyspace_notifications = value.clone();

        if !value.is_empty() {
            complexity.add_finding(
                "Features",
                Severity::Critical,
                "Keyspace Notifications Enabled",
                &format!(
                    "notify-keyspace-events = '{}'. Per Microsoft docs, keyspace \
                     notifications are NOT supported in any AMR tier (Memory Optimized, \
                     Balanced, Compute Optimized, or Flash Optimized). Applications \
                     relying on __keyevent@*__ or __keyspace@*__ subscriptions will \
                     break after migration.",
                    value
                ),
            );
        } else {
            complexity.add_finding(
                "Features",
                Severity::Info,
                "Keyspace Notifications Disabled",
                "No keyspace notifications configured.",
            );
        }
    }
}

async fn analyze_persistence(
    conn: &mut MultiplexedConnection,
    complexity: &mut CustomerComplexity,
) {
    // Check RDB (save params)
    let save_config: Result<Vec<String>, _> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("save")
        .query_async(conn)
        .await;

    if let Ok(vals) = save_config {
        let value = vals.get(1).cloned().unwrap_or_default();
        complexity.persistence_config.rdb_save_params = value.clone();
        complexity.persistence_config.rdb_enabled = !value.is_empty();

        if !value.is_empty() {
            complexity.add_finding(
                "Persistence",
                Severity::Warning,
                "RDB Snapshots Enabled",
                &format!(
                    "RDB save parameters: '{}'. AMR supports RDB persistence using \
                     managed disks (not storage accounts like ACR Premium). RDB export \
                     from ACR can be imported into AMR, but persisted files from AMR \
                     cannot be accessed directly or imported into other caches. \
                     Persistence requires High Availability enabled and cannot be \
                     combined with active geo-replication.",
                    value
                ),
            );
        }
    }

    // Check AOF
    let aof_config: Result<Vec<String>, _> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendonly")
        .query_async(conn)
        .await;

    if let Ok(vals) = aof_config {
        let value = vals.get(1).cloned().unwrap_or_default();
        complexity.persistence_config.aof_enabled = value == "yes";

        if value == "yes" {
            let fsync: Result<Vec<String>, _> = redis::cmd("CONFIG")
                .arg("GET")
                .arg("appendfsync")
                .query_async(conn)
                .await;

            let fsync_val = fsync
                .ok()
                .and_then(|v| v.get(1).cloned())
                .unwrap_or_else(|| "unknown".to_string());

            complexity.persistence_config.aof_fsync = fsync_val.clone();

            complexity.add_finding(
                "Persistence",
                Severity::Warning,
                "AOF Persistence Enabled",
                &format!(
                    "AOF is enabled (fsync: {}). AMR supports AOF persistence on \
                     managed disks with once-per-second writes. AOF impacts throughput \
                     as it runs on all primary processes. Persistence cannot be combined \
                     with active geo-replication in AMR.",
                    fsync_val
                ),
            );
        }
    }

    if !complexity.persistence_config.rdb_enabled && !complexity.persistence_config.aof_enabled {
        complexity.add_finding(
            "Persistence",
            Severity::Info,
            "No Persistence",
            "Ephemeral cache only — simplest migration path.",
        );
    }
}

async fn analyze_memory_policy(
    conn: &mut MultiplexedConnection,
    complexity: &mut CustomerComplexity,
) {
    let policy: Result<Vec<String>, _> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory-policy")
        .query_async(conn)
        .await;

    if let Ok(vals) = policy {
        let value = vals.get(1).cloned().unwrap_or_else(|| "unknown".to_string());
        complexity.max_memory_policy = value.clone();

        // Check if RediSearch is loaded — it requires NoEviction policy
        let has_redisearch = complexity.loaded_modules.iter().any(|m| {
            let lower = m.to_lowercase();
            lower.contains("search") || lower.contains("ft")
        });

        if has_redisearch && value != "noeviction" {
            complexity.add_finding(
                "Configuration",
                Severity::Critical,
                &format!("Eviction Policy: {} (RediSearch conflict)", value),
                "RediSearch module requires the 'noeviction' eviction policy in AMR. \
                 Current policy is incompatible — must change to 'noeviction' before \
                 using RediSearch on AMR.",
            );
        } else if value != "noeviction" && value != "volatile-lru" && value != "allkeys-lru" {
            complexity.add_finding(
                "Configuration",
                Severity::Warning,
                &format!("Eviction Policy: {}", value),
                "Non-standard eviction policy. Verify this policy is supported in \
                 AMR and test application behavior post-migration.",
            );
        } else {
            complexity.add_finding(
                "Configuration",
                Severity::Info,
                &format!("Eviction Policy: {}", value),
                "Standard eviction policy — compatible with AMR.",
            );
        }
    }
}

async fn analyze_lua_scripts(
    conn: &mut MultiplexedConnection,
    complexity: &mut CustomerComplexity,
) {
    // Check if there are cached Lua scripts via SCRIPT EXISTS is not practical,
    // but we can check INFO stats for eval calls
    let stats_info: Result<String, _> = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(conn)
        .await;

    if let Ok(info) = stats_info {
        let eval_calls: u64 = info
            .lines()
            .filter(|l| l.starts_with("cmdstat_eval:") || l.starts_with("cmdstat_evalsha:"))
            .filter_map(|l| {
                l.split("calls=")
                    .nth(1)
                    .and_then(|v| v.split(',').next())
                    .and_then(|v| v.parse::<u64>().ok())
            })
            .sum();

        if eval_calls > 0 {
            complexity.add_finding(
                "Features",
                Severity::Warning,
                &format!("Lua Scripts Detected ({} calls)", format_number(eval_calls)),
                "EVAL/EVALSHA usage detected. Lua scripts must be re-registered \
                 on the new AMR instance and tested for compatibility.",
            );
        }
    }
}

async fn analyze_pubsub(conn: &mut MultiplexedConnection, complexity: &mut CustomerComplexity) {
    let channels: Result<Vec<String>, _> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .query_async(conn)
        .await;

    if let Ok(ch) = channels {
        if !ch.is_empty() {
            complexity.add_finding(
                "Features",
                Severity::Warning,
                &format!("{} Active Pub/Sub Channel(s)", ch.len()),
                "Active Pub/Sub channels detected. Subscribers will need to \
                 reconnect to the AMR endpoint after migration. Plan for \
                 a coordinated switchover.",
            );
        }
    }
}

async fn analyze_connection_protocol(
    conn: &mut MultiplexedConnection,
    complexity: &mut CustomerComplexity,
) {
    let server_info: Result<String, _> = redis::cmd("INFO")
        .arg("server")
        .query_async(conn)
        .await;

    if let Ok(info) = server_info {
        let tcp_port = parse_info_field(&info, "tcp_port").unwrap_or(6379);
        let tls_port = parse_info_field(&info, "tls_port").unwrap_or(0);

        // AMR only supports TLS on port 10000 — no non-TLS port
        if tls_port == 0 && tcp_port == 6379 {
            complexity.add_finding(
                "Connection",
                Severity::Warning,
                "Non-TLS Connection (port 6379)",
                "Connected via non-TLS port 6379. AMR does NOT support non-TLS \
                 connections. All clients must use TLS. AMR uses port 10000 \
                 (not 6380). Update connection strings and ensure TLS is configured.",
            );
        }

        // DNS suffix change warning
        complexity.add_finding(
            "Connection",
            Severity::Info,
            "Endpoint Change Required",
            "AMR uses a different DNS suffix: <name>.<region>.redis.azure.net \
             (ACR used .redis.cache.windows.net). TLS port changes from 6380 to \
             10000. Per-shard ports change from 13XXX/15XXX to 85XX range. Update \
             all connection strings and firewall rules.",
        );
    }
}

async fn analyze_multikey_commands(
    conn: &mut MultiplexedConnection,
    complexity: &mut CustomerComplexity,
) {
    let stats_info: Result<String, _> = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(conn)
        .await;

    if let Ok(info) = stats_info {
        // Multi-key commands that can hit CROSSSLOT errors in AMR clustered mode
        let multikey_cmds = [
            "mget", "mset", "del", "unlink", "exists", "touch",
            "rename", "renamenx", "smove", "rpoplpush", "lmpop",
            "sinterstore", "sunionstore", "sdiffstore", "zunionstore",
            "zinterstore", "zdiffstore", "copy", "msetnx",
        ];

        // Commands that are ONLY allowed cross-slot with Enterprise policy: DEL, MSET, MGET, EXISTS, UNLINK, TOUCH
        // All others require same hash slot in ALL clustering policies
        let enterprise_allowed_crossslot = ["del", "mset", "mget", "exists", "unlink", "touch"];

        let mut crossslot_risk_cmds: Vec<(String, u64)> = Vec::new();

        for cmd in &multikey_cmds {
            let stat_key = format!("cmdstat_{}:", cmd);
            if let Some(line) = info.lines().find(|l| l.starts_with(&stat_key)) {
                let calls = line
                    .split("calls=")
                    .nth(1)
                    .and_then(|v| v.split(',').next())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);

                if calls > 0 && !enterprise_allowed_crossslot.contains(cmd) {
                    crossslot_risk_cmds.push((cmd.to_uppercase(), calls));
                }
            }
        }

        if !crossslot_risk_cmds.is_empty() {
            let cmd_list: Vec<String> = crossslot_risk_cmds
                .iter()
                .map(|(cmd, calls)| format!("{} ({}x)", cmd, calls))
                .collect();
            complexity.add_finding(
                "Commands",
                Severity::Warning,
                "Multi-Key Cross-Slot Risk",
                &format!(
                    "Commands that require same hash slot in AMR: {}. \
                     These will fail with CROSSSLOT errors unless all keys \
                     map to the same slot. Use {{hashtag}} key patterns or \
                     the Non-Clustered policy (<=25GB) to avoid this.",
                    cmd_list.join(", ")
                ),
            );
        }
    }
}

// =============================================================================
// Azure Retail Prices API
// =============================================================================

#[derive(Debug, Deserialize)]
struct AzureRetailPricesResponse {
    #[serde(rename = "Items")]
    items: Vec<AzureRetailPriceItem>,
    #[serde(rename = "NextPageLink")]
    next_page_link: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AzureRetailPriceItem {
    #[serde(rename = "skuName")]
    sku_name: String,
    #[serde(rename = "productName")]
    product_name: String,
    #[serde(rename = "retailPrice")]
    retail_price: f64,
    #[serde(rename = "unitOfMeasure")]
    unit_of_measure: String,
    #[serde(rename = "armRegionName")]
    arm_region_name: Option<String>,
    #[serde(rename = "meterName")]
    meter_name: String,
    #[allow(dead_code)]
    #[serde(rename = "type")]
    price_type: String,
}

async fn fetch_azure_redis_pricing(region: &str) -> Result<Vec<AzureSkuPrice>> {
    let client = reqwest::Client::new();
    let filter = format!(
        "serviceName eq 'Redis Cache' and armRegionName eq '{}' and priceType eq 'Consumption'",
        region
    );

    let mut all_skus: Vec<AzureSkuPrice> = Vec::new();
    let mut url = format!(
        "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview&$filter={}",
        filter
    );

    loop {
        let response: AzureRetailPricesResponse = client
            .get(&url)
            .send()
            .await
            .context("Failed to call Azure Retail Prices API")?
            .json()
            .await
            .context("Failed to parse Azure Retail Prices response")?;

        for item in response.items {
            // Skip $0 prices and non-hourly meters (e.g., overage)
            if item.retail_price <= 0.0 {
                continue;
            }

            all_skus.push(AzureSkuPrice {
                sku_name: item.sku_name,
                product_name: item.product_name,
                retail_price: item.retail_price,
                unit_of_measure: item.unit_of_measure,
                region: item.arm_region_name.unwrap_or_else(|| region.to_string()),
                meter_name: item.meter_name,
            });
        }

        match response.next_page_link {
            Some(next) if !next.is_empty() => url = next,
            _ => break,
        }
    }

    // Sort by annual cost ascending
    all_skus.sort_by(|a, b| a.annual_cost().partial_cmp(&b.annual_cost()).unwrap_or(std::cmp::Ordering::Equal));

    // Deduplicate by sku_name + meter_name (keep first/cheapest)
    let mut seen = std::collections::HashSet::new();
    all_skus.retain(|s| seen.insert(format!("{}|{}", s.sku_name, s.meter_name)));

    Ok(all_skus)
}

// =============================================================================
// Helpers
// =============================================================================

const EDEN_LEARN_MORE_URL: &str = "https://www.eden.dev/migrate/redis";

fn open_url(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).spawn()?;
    }
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).spawn()?;
    }
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd").args(["/C", "start", url]).spawn()?;
    }
    Ok(())
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
    match state.active_view {
        TuiView::Analysis => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),  // Header
                    Constraint::Length(6),  // Value prop
                    Constraint::Length(8),  // Metrics
                    Constraint::Min(8),    // Type distribution
                    Constraint::Length(2),  // Footer
                ])
                .split(frame.area());

            render_header(frame, chunks[0], state, config);
            render_value_prop(frame, chunks[1], state);
            render_metrics(frame, chunks[2], state);
            render_type_distribution(frame, chunks[3], state);
            render_footer(frame, chunks[4], state, config);
        }
        TuiView::Complexity => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),  // Header
                    Constraint::Length(5),  // Summary
                    Constraint::Min(10),    // Findings
                    Constraint::Length(2),  // Footer
                ])
                .split(frame.area());

            render_header(frame, chunks[0], state, config);
            render_complexity_summary(frame, chunks[1], state);
            render_complexity_findings(frame, chunks[2], state);
            render_footer(frame, chunks[3], state, config);
        }
        TuiView::Pricing => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),   // Header
                    Constraint::Min(10),     // Region selector + SKU list side-by-side
                    Constraint::Length(12),  // Pricing estimate + comparison
                    Constraint::Length(2),   // Footer
                ])
                .split(frame.area());

            // Split the selection area: region list (left) + SKU list (right)
            let selection = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(34),
                    Constraint::Min(40),
                ])
                .split(chunks[1]);

            // Split the bottom summary: pricing (left) + time comparison (right)
            let summary = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(50),
                    Constraint::Percentage(50),
                ])
                .split(chunks[2]);

            render_header(frame, chunks[0], state, config);
            render_region_list(frame, selection[0], state);
            render_sku_list(frame, selection[1], state);
            render_pricing_estimate(frame, summary[0], state);
            render_migration_time(frame, summary[1], state);
            render_footer(frame, chunks[3], state, config);
        }
        TuiView::Recommend => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),   // Header
                    Constraint::Length(5),   // Overprovisioning control
                    Constraint::Min(10),     // Recommendation details
                    Constraint::Length(2),   // Footer
                ])
                .split(frame.area());

            render_header(frame, chunks[0], state, config);
            render_overprovision_control(frame, chunks[1], state);
            render_recommendation(frame, chunks[2], state);
            render_footer(frame, chunks[3], state, config);
        }
        TuiView::Summary => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),  // Header
                    Constraint::Min(10),   // Summary content
                    Constraint::Length(2),  // Footer
                ])
                .split(frame.area());

            render_header(frame, chunks[0], state, config);
            render_summary(frame, chunks[1], state);
            render_footer(frame, chunks[2], state, config);
        }
        TuiView::Docs => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),  // Header
                    Constraint::Min(10),   // Docs content
                    Constraint::Length(2),  // Footer
                ])
                .split(frame.area());

            render_header(frame, chunks[0], state, config);
            render_docs(frame, chunks[1], state);
            render_footer(frame, chunks[2], state, config);
        }
    }
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
            " Eden Redis Migration Analyzer ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        status,
        Span::raw(format!("  {}:{} (v{})", config.host, config.port, version)),
    ]);

    let header = Paragraph::new(title).wrap(Wrap { trim: false }).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    frame.render_widget(header, area);
}

fn render_value_prop(frame: &mut Frame, area: Rect, state: &AppState) {
    let dim = Style::default().fg(Color::DarkGray);
    let green = Style::default().fg(Color::Green).add_modifier(Modifier::BOLD);
    let cyan = Style::default().fg(Color::Cyan);

    // Build a dynamic summary if we have data
    let savings_hint = state.peak_metrics().map(|peak| {
        let gb = peak.used_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if gb >= 1.0 {
            format!("  Analyzing {:.1} GB at {} ops/sec — ", gb, format_number(peak.ops_per_sec))
        } else {
            format!("  Analyzing {:.0} MB at {} ops/sec — ", gb * 1024.0, format_number(peak.ops_per_sec))
        }
    });

    let lines = vec![
        Line::from(vec![
            Span::styled(
                savings_hint.unwrap_or_else(|| "  ".to_string()),
                dim,
            ),
            Span::styled(
                "Use tabs 3-5 to see how much you can save migrating to Azure Managed Redis",
                cyan,
            ),
        ]),
        Line::from(vec![
            Span::styled("  Fully automated migration  ", green),
            Span::styled("·", dim),
            Span::styled("  Milliseconds of downtime  ", green),
            Span::styled("·", dim),
            Span::styled("  Checksum verified  ", green),
            Span::styled("·", dim),
            Span::styled("  Instant rollback", green),
        ]),
        Line::from(vec![
            Span::styled("  Ongoing monitoring, analysis, AI integrations & support included with every subscription", dim),
        ]),
    ];

    let block = Block::default()
        .title(" Exodus — Migrate Redis to Azure, the easy way ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
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

    let dim = Style::default().fg(Color::DarkGray);
    let cyan = Style::default().fg(Color::Cyan);
    let highlight = Style::default().fg(Color::White);

    // Row 1: View tabs with active highlighted
    let views = [
        (TuiView::Analysis, "1:Analysis"),
        (TuiView::Complexity, "2:Complexity"),
        (TuiView::Pricing, "3:Pricing"),
        (TuiView::Recommend, "4:Recommend"),
        (TuiView::Summary, "5:Summary"),
        (TuiView::Docs, "d:Docs"),
    ];

    let mut tab_spans: Vec<Span> = vec![Span::styled(" ", dim)];
    for (i, (view, label)) in views.iter().enumerate() {
        if i > 0 {
            tab_spans.push(Span::styled(" | ", dim));
        }
        if state.active_view == *view {
            tab_spans.push(Span::styled(format!("[{}]", label), highlight));
        } else {
            tab_spans.push(Span::styled(*label, dim));
        }
    }
    tab_spans.push(Span::styled(
        format!("  |  Refresh: {}s | Last: {} | #{}{}", config.interval, last_update, state.update_count, error_text),
        dim,
    ));

    // Row 2: Context-specific controls + learn more
    let context_hint = match state.active_view {
        TuiView::Pricing => match state.pricing_focus {
            PricingFocus::Region => " Up/Down browse | Enter confirm region | → SKUs",
            PricingFocus::Sku => " Up/Down browse | Enter confirm SKU | ← regions",
        },
        TuiView::Recommend => " Up/Down overprovision % | Left/Right database size",
        TuiView::Docs | TuiView::Summary => " Up/Down scroll",
        TuiView::Complexity => " Up/Down scroll findings",
        TuiView::Analysis => " Tab cycle views",
    };

    let row2 = Line::from(vec![
        Span::styled(" q:quit | Tab:next view", dim),
        Span::styled(context_hint, dim),
        Span::styled(
            format!("  |  l:Learn more {}", EDEN_LEARN_MORE_URL),
            cyan,
        ),
    ]);

    let lines = vec![Line::from(tab_spans), row2];
    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, area);
}

fn render_complexity_summary(frame: &mut Frame, area: Rect, state: &AppState) {
    let (score_text, cluster_text, acl_text) = state
        .complexity
        .as_ref()
        .map(|c| {
            let score_color = if c.total_score <= 10 {
                Color::Green
            } else if c.total_score <= 25 {
                Color::Yellow
            } else {
                Color::Red
            };

            let critical = c.findings.iter().filter(|f| f.severity == Severity::Critical).count();
            let warnings = c.findings.iter().filter(|f| f.severity == Severity::Warning).count();
            let infos = c.findings.iter().filter(|f| f.severity == Severity::Info).count();

            let score = format!(
                "Score: {} ({} critical, {} warnings, {} info)",
                c.total_score, critical, warnings, infos
            );
            let cluster = format!(
                "Cluster: {} | Modules: {}",
                c.cluster_mode.as_deref().unwrap_or("unknown"),
                if c.loaded_modules.is_empty() {
                    "none".to_string()
                } else {
                    c.loaded_modules.join(", ")
                }
            );
            let acl = format!(
                "ACL Rules: {} | Persistence: {}{}",
                c.acl_rules.len(),
                if c.persistence_config.rdb_enabled { "RDB " } else { "" },
                if c.persistence_config.aof_enabled { "AOF" } else {
                    if !c.persistence_config.rdb_enabled { "none" } else { "" }
                },
            );

            (
                (score, score_color),
                cluster,
                acl,
            )
        })
        .unwrap_or_else(|| {
            (
                ("Scanning...".to_string(), Color::DarkGray),
                "Cluster: -".to_string(),
                "ACL Rules: -".to_string(),
            )
        });

    let rows = vec![
        Row::new(vec![
            Cell::from(score_text.0).style(Style::default().fg(score_text.1).add_modifier(Modifier::BOLD)),
        ]),
        Row::new(vec![
            Cell::from(cluster_text).style(Style::default().fg(Color::Cyan)),
        ]),
        Row::new(vec![
            Cell::from(acl_text).style(Style::default().fg(Color::Cyan)),
        ]),
    ];

    let table = Table::new(rows, [Constraint::Min(60)])
        .block(
            Block::default()
                .title(" Complexity Summary ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Magenta)),
        );

    frame.render_widget(table, area);
}

fn render_complexity_findings(frame: &mut Frame, area: Rect, state: &AppState) {
    let rows: Vec<Row> = state
        .complexity
        .as_ref()
        .map(|c| {
            c.findings
                .iter()
                .skip(state.complexity_scroll as usize)
                .map(|f| {
                    let (severity_str, sev_color) = match f.severity {
                        Severity::Critical => ("CRIT", Color::Red),
                        Severity::Warning => ("WARN", Color::Yellow),
                        Severity::Info => ("INFO", Color::Green),
                    };

                    Row::new(vec![
                        Cell::from(severity_str)
                            .style(Style::default().fg(sev_color).add_modifier(Modifier::BOLD)),
                        Cell::from(f.category.as_str())
                            .style(Style::default().fg(Color::Cyan)),
                        Cell::from(f.title.as_str())
                            .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                        Cell::from(f.detail.as_str())
                            .style(Style::default().fg(Color::DarkGray)),
                    ])
                    .height(2)
                })
                .collect()
        })
        .unwrap_or_default();

    let finding_count = state
        .complexity
        .as_ref()
        .map(|c| c.findings.len())
        .unwrap_or(0);

    let table = Table::new(
        rows,
        [
            Constraint::Length(5),
            Constraint::Length(14),
            Constraint::Length(32),
            Constraint::Min(30),
        ],
    )
    .block(
        Block::default()
            .title(format!(
                " Findings ({}) — Up/Down to scroll ",
                finding_count
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );

    frame.render_widget(table, area);
}

fn format_with_commas(n: f64) -> String {
    let whole = n.round() as i64;
    let s = whole.to_string();
    let bytes = s.as_bytes();
    let mut result = String::new();
    let len = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 && b != b'-' {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

fn render_pricing_estimate(frame: &mut Frame, area: Rect, state: &AppState) {
    let lines = if state.azure_loading {
        vec![
            Line::from(Span::styled(
                "Loading ACR pricing...",
                Style::default().fg(Color::Yellow),
            )),
        ]
    } else if let Some(ref err) = state.azure_error {
        vec![
            Line::from(Span::styled(
                format!("Error: {}", err),
                Style::default().fg(Color::Red),
            )),
        ]
    } else if let Some(ref estimate) = state.pricing_estimate {
        vec![
            Line::from(vec![
                Span::styled("Tier: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    estimate.tier.label(),
                    Style::default().fg(match estimate.tier {
                        PricingTier::Simple => Color::Green,
                        PricingTier::Moderate => Color::Yellow,
                        PricingTier::Difficult => Color::Rgb(255, 165, 0),
                        PricingTier::Complex => Color::Red,
                    }).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("  (complexity score: {})", state.complexity.as_ref().map(|c| c.total_score).unwrap_or(0)),
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                Span::styled("Selected SKU: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{} — {}", estimate.selected_sku.sku_name, estimate.selected_sku.meter_name),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            Line::from(vec![
                Span::styled("Annual Azure Spend: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("${:.2}/yr", estimate.annual_azure_spend),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("  (${}/hr × 8,760 hrs)", estimate.selected_sku.retail_price),
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                Span::styled("Exodus License:  ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("${}/yr", format_with_commas(estimate.estimated_price)),
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("  (10% base × {} — capped at 20%)", estimate.tier.label()),
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(Span::styled(
                "Includes: migration + monitoring + analysis + AI integrations + support",
                Style::default().fg(Color::Cyan),
            )),
        ]
    } else if state.azure_confirmed_region.is_none() {
        vec![
            Line::from(Span::styled(
                "Select an Azure region, then choose your ACR SKU to see pricing.",
                Style::default().fg(Color::DarkGray),
            )),
        ]
    } else if state.azure_acr_skus.is_empty() {
        vec![
            Line::from(Span::styled(
                "No ACR SKUs found for this region.",
                Style::default().fg(Color::DarkGray),
            )),
        ]
    } else {
        vec![
            Line::from(Span::styled(
                "Select your ACR SKU and press Enter to see pricing.",
                Style::default().fg(Color::Yellow),
            )),
        ]
    };

    let block = Block::default()
        .title(" Exodus Annual License ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
}

fn render_migration_time(frame: &mut Frame, area: Rect, state: &AppState) {
    let green = Style::default().fg(Color::Green).add_modifier(Modifier::BOLD);
    let red = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);
    let dim = Style::default().fg(Color::DarkGray);

    let lines = if let Some(ref estimate) = state.pricing_estimate {
        if let Some(ref time) = estimate.migration_time {
            let diy_cost = time.manual_cost;
            let exodus_price = estimate.estimated_price;

            // Without Eden: manual engineering + stay on current ACR
            let current_acr_cost = state.recommendation.as_ref()
                .and_then(|r| r.current_sku_annual_cost)
                .unwrap_or(estimate.annual_azure_spend);
            let without_eden = diy_cost + current_acr_cost;

            // With Eden: exodus fee + recommended AMR (or same ACR if no recommendation)
            let amr_cost = state.recommendation.as_ref()
                .and_then(|r| r.recommended_sku.as_ref().map(|_| r.recommended_annual_cost))
                .unwrap_or(current_acr_cost);
            let with_eden = exodus_price + amr_cost;

            let total_saved = without_eden - with_eden;

            let amr_label = state.recommendation.as_ref()
                .and_then(|r| r.recommended_sku.as_ref().map(|s| s.sku_name.clone()))
                .unwrap_or_else(|| "same ACR".to_string());

            let lines = vec![
                Line::from(vec![
                    Span::styled("  WITHOUT Exodus:      ", red),
                    Span::styled(format!("${} DIY", format_with_commas(diy_cost)), red),
                    Span::styled(" + ", dim),
                    Span::styled(format!("${}/yr ACR", format_with_commas(current_acr_cost)), red),
                    Span::styled(format!(" = ${} yr 1", format_with_commas(without_eden)), red),
                ]),
                Line::from(vec![
                    Span::styled("  WITH Exodus:         ", green),
                    Span::styled(format!("${}/yr Exodus", format_with_commas(exodus_price)), green),
                    Span::styled(" + ", dim),
                    Span::styled(format!("${}/yr AMR ({})", format_with_commas(amr_cost), amr_label), green),
                    Span::styled(format!(" = ${}/yr", format_with_commas(with_eden)), green),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  YOU SAVE:          ", dim),
                    Span::styled(
                        format!("${} year 1", format_with_commas(total_saved.max(0.0))),
                        Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("  Timeline:          ", dim),
                    Span::styled(time.exodus_summary(), green),
                    Span::styled(format!(" vs {}", time.manual_summary()), red),
                ]),
                Line::from(vec![
                    Span::styled("  ", dim),
                    Span::styled("Milliseconds downtime", green),
                    Span::styled(" · ", dim),
                    Span::styled("Checksum verified", green),
                    Span::styled(" · ", dim),
                    Span::styled("Instant rollback", green),
                    Span::styled(" · ", dim),
                    Span::styled("Fully automated", green),
                ]),
            ];

            lines
        } else {
            vec![Line::from(Span::styled("Waiting for database metrics...", dim))]
        }
    } else {
        vec![Line::from(Span::styled("Select an ACR SKU to see total savings", dim))]
    };

    let block = Block::default()
        .title(" Total Savings with Exodus ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
}

/// Compute scroll offset so `cursor` stays visible within `visible_height` rows.
fn scroll_offset(cursor: usize, total: usize, visible_height: usize) -> usize {
    if total <= visible_height || cursor < visible_height / 2 {
        0
    } else if cursor + visible_height / 2 >= total {
        total.saturating_sub(visible_height)
    } else {
        cursor.saturating_sub(visible_height / 2)
    }
}

fn render_region_list(frame: &mut Frame, area: Rect, state: &AppState) {
    let is_focused = state.pricing_focus == PricingFocus::Region;
    let border_color = if is_focused { Color::Green } else { Color::DarkGray };

    // 2 for borders
    let visible_height = area.height.saturating_sub(2) as usize;
    let offset = scroll_offset(state.azure_selected_region, AZURE_REGIONS.len(), visible_height);

    let rows: Vec<Row> = AZURE_REGIONS
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, (code, label))| {
            let selected = i == state.azure_selected_region;
            let confirmed = state.azure_confirmed_region == Some(i);
            let marker = if confirmed && selected {
                "● ▶"
            } else if confirmed {
                "●  "
            } else if selected {
                "  ▶"
            } else {
                "   "
            };

            let style = if selected && is_focused {
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
            } else if confirmed {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            Row::new(vec![
                Cell::from(marker).style(Style::default().fg(Color::Green)),
                Cell::from(format!("{} ({})", label, code)).style(style),
            ])
        })
        .collect();

    let scroll_indicator = if AZURE_REGIONS.len() > visible_height {
        let pos = state.azure_selected_region + 1;
        let total = AZURE_REGIONS.len();
        format!(" [{}/{}] ", pos, total)
    } else {
        String::new()
    };

    let table = Table::new(
        rows,
        [Constraint::Length(4), Constraint::Min(20)],
    )
    .block(
        Block::default()
            .title(format!(" Azure Region — Enter to select{}", scroll_indicator))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color)),
    );

    frame.render_widget(table, area);
}

fn render_sku_list(frame: &mut Frame, area: Rect, state: &AppState) {
    let is_focused = state.pricing_focus == PricingFocus::Sku;
    let border_color = if is_focused { Color::Cyan } else { Color::DarkGray };

    // If no region selected yet, show a prompt
    if state.azure_confirmed_region.is_none() {
        let msg = Paragraph::new(Line::from(Span::styled(
            "← Select a region first, then press Enter",
            Style::default().fg(Color::DarkGray),
        )))
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(" ACR SKUs ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(msg, area);
        return;
    }

    let has_complexity = state.complexity.is_some();
    let complexity_mult = state.complexity.as_ref().map(|c| {
        PricingTier::from_score(c.total_score).complexity_multiplier()
    });

    let header = Row::new(vec![
        Cell::from("").style(Style::default().fg(Color::DarkGray)),
        Cell::from("SKU").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Cell::from("Meter").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Cell::from("$/Hour").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Cell::from("Annual Cost").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Cell::from("│").style(Style::default().fg(Color::DarkGray)),
        Cell::from("Exodus Base").style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Cell::from("Complexity Multiple").style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
    ])
    .height(1);

    // 2 for borders, 1 for header row
    let visible_height = area.height.saturating_sub(3) as usize;
    let sku_count = state.azure_acr_skus.len();
    let offset = scroll_offset(state.azure_selected_sku, sku_count, visible_height);

    let rows: Vec<Row> = state
        .azure_acr_skus
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, sku)| {
            let cursor = i == state.azure_selected_sku;
            let confirmed = state.azure_confirmed_sku == Some(i);
            let marker = if confirmed && cursor {
                "● ▶"
            } else if confirmed {
                "●  "
            } else if cursor {
                "  ▶"
            } else {
                "   "
            };

            let style = if cursor && is_focused {
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
            } else if confirmed {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            let annual = sku.annual_cost();
            let base = sku.exodus_base_price();

            let complexity_text = if has_complexity {
                let mult = complexity_mult.unwrap_or(1.0);
                let annual = sku.annual_cost();
                let cap = ((annual * 0.20) / 100.0).round() * 100.0;
                let est = ((base * mult) / 100.0).round() * 100.0;
                let est = est.min(cap).max(2500.0);
                format!("{}x → ${}", mult, format_with_commas(est))
            } else {
                "?".to_string()
            };

            let base_style = if cursor && is_focused {
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
            } else if confirmed {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            let mult_style = if has_complexity {
                if confirmed || (cursor && is_focused) {
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                }
            } else {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            };

            Row::new(vec![
                Cell::from(marker).style(Style::default().fg(Color::Green)),
                Cell::from(sku.sku_name.as_str()).style(style),
                Cell::from(sku.meter_name.as_str()).style(style),
                Cell::from(format!("${:.4}", sku.retail_price)).style(style),
                Cell::from(format!("${}", format_with_commas(annual))).style(style),
                Cell::from("│").style(Style::default().fg(Color::DarkGray)),
                Cell::from(format!("${}", format_with_commas(base))).style(base_style),
                Cell::from(complexity_text).style(mult_style),
            ])
        })
        .collect();

    let region_name = state.azure_confirmed_region
        .and_then(|i| AZURE_REGIONS.get(i))
        .map(|(_, label)| *label)
        .unwrap_or("--");

    let scroll_indicator = if sku_count > visible_height {
        format!(" [{}/{}]", state.azure_selected_sku + 1, sku_count)
    } else {
        String::new()
    };

    let title = if state.azure_loading {
        format!(" {} — Loading SKUs... ", region_name)
    } else if let Some(ref err) = state.azure_error {
        format!(" {} — Error: {} ", region_name, err)
    } else {
        format!(" {} — {} SKUs{} — Enter to confirm ", region_name, sku_count, scroll_indicator)
    };

    let table = Table::new(
        rows,
        [
            Constraint::Length(4),
            Constraint::Length(12),
            Constraint::Length(24),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(1),  // │ separator
            Constraint::Length(12),
            Constraint::Min(20),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color)),
    );

    frame.render_widget(table, area);
}

// =============================================================================
// =============================================================================
// Summary View Rendering
// =============================================================================

fn render_summary(frame: &mut Frame, area: Rect, state: &AppState) {
    let dim = Style::default().fg(Color::DarkGray);
    let white = Style::default().fg(Color::White);
    let green = Style::default().fg(Color::Green).add_modifier(Modifier::BOLD);
    let red = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);
    let yellow_bold = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let sep = "  ──────────────────────────────────────────────────────────────────────";

    let mut lines: Vec<Line> = Vec::new();

    // Check if we have enough data
    let has_pricing = state.pricing_estimate.is_some();
    if !has_pricing {
        lines.push(Line::from(Span::styled(
            "  Select an ACR SKU on the Pricing tab (3) to see the full cost comparison.",
            dim,
        )));

        let block = Block::default()
            .title(" Total Cost Comparison — Exodus vs Self ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Green));
        let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
        frame.render_widget(paragraph, area);
        return;
    }

    let estimate = state.pricing_estimate.as_ref().unwrap();
    let time = estimate.migration_time.as_ref();
    let exodus_price = estimate.estimated_price;
    let diy_cost = time.map(|t| t.manual_cost).unwrap_or(8000.0);
    let diy_hours = time.map(|t| t.manual_hours).unwrap_or(40.0);
    let diy_weeks = time.map(|t| t.manual_weeks).unwrap_or(4.0);

    // Infrastructure costs
    let current_acr_cost = state.recommendation.as_ref()
        .and_then(|r| r.current_sku_annual_cost)
        .unwrap_or(estimate.annual_azure_spend);
    let amr_cost = state.recommendation.as_ref()
        .and_then(|r| r.recommended_sku.as_ref().map(|_| r.recommended_annual_cost))
        .unwrap_or(current_acr_cost);
    let amr_sku_name = state.recommendation.as_ref()
        .and_then(|r| r.recommended_sku.as_ref().map(|s| s.sku_name.clone()))
        .unwrap_or_else(|| "same ACR".to_string());

    // Totals
    let self_yr1 = diy_cost + current_acr_cost;
    let self_yr2 = current_acr_cost;
    let eden_yr1 = exodus_price + amr_cost;
    let eden_yr2 = amr_cost;
    let save_yr1 = self_yr1 - eden_yr1;
    let save_yr2 = self_yr2 - eden_yr2;

    // ── Header ──
    lines.push(Line::from(vec![
        Span::styled(format!("{:<26}", ""), dim),
        Span::styled(format!("{:<26}", "WITH EDEN"), green),
        Span::styled(format!("{:<26}", "DO IT YOURSELF"), red),
    ]));
    lines.push(Line::from(Span::styled(sep, dim)));

    // ── Migration ──
    lines.push(Line::from(Span::styled("  MIGRATION", yellow_bold)));
    lines.push(Line::from(vec![
        Span::styled("  Automation:         ", dim),
        Span::styled(format!("{:<26}", "Fully automated"), green),
        Span::styled(format!("{:<26}", "Custom scripts"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Engineering:        ", dim),
        Span::styled(format!("{:<26}", "Included"), green),
        Span::styled(format!("{:<26}", format!("${} ({:.0} hrs × $200/hr)", format_with_commas(diy_cost), diy_hours)), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Timeline:           ", dim),
        Span::styled(format!("{:<26}", time.map(|t| t.exodus_summary()).unwrap_or_else(|| "--".to_string())), green),
        Span::styled(format!("{:<26}", format!("{:.0} hrs over {:.0} weeks", diy_hours, diy_weeks)), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Downtime:           ", dim),
        Span::styled(format!("{:<26}", "Milliseconds"), green),
        Span::styled(format!("{:<26}", "Hours to days"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Data Integrity:     ", dim),
        Span::styled(format!("{:<26}", "Checksum verified"), green),
        Span::styled(format!("{:<26}", "Manual spot-checks"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Rollback:           ", dim),
        Span::styled(format!("{:<26}", "Instant, built-in"), green),
        Span::styled(format!("{:<26}", "Restore from backup"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Client Changes:     ", dim),
        Span::styled(format!("{:<26}", "Transparent DNS cutover"), green),
        Span::styled(format!("{:<26}", "Rewrite conn strings"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Compliance:         ", dim),
        Span::styled(format!("{:<26}", "Full audit trail"), green),
        Span::styled(format!("{:<26}", "Manual screenshots"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Battle-Tested:      ", dim),
        Span::styled(format!("{:<26}", "Hundreds of migrations"), green),
        Span::styled(format!("{:<26}", "Your team's first try"), red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Support:            ", dim),
        Span::styled(format!("{:<26}", "Included"), green),
        Span::styled(format!("{:<26}", "None"), red),
    ]));

    lines.push(Line::from(Span::styled(sep, dim)));

    // ── Annual costs ──
    lines.push(Line::from(Span::styled("  ANNUAL COSTS", yellow_bold)));
    lines.push(Line::from(vec![
        Span::styled("  Exodus License:  ", dim),
        Span::styled(format!("{:<26}", format!("${}/yr", format_with_commas(exodus_price))), green),
        Span::styled(format!("{:<26}", "—"), dim),
    ]));
    lines.push(Line::from(vec![
        Span::styled("                      ", dim),
        Span::styled("Migration + monitoring + AI + support", Style::default().fg(Color::Cyan)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Infrastructure:     ", dim),
        Span::styled(format!("{:<26}", format!("${}/yr ({})", format_with_commas(amr_cost), amr_sku_name)), green),
        Span::styled(format!("{:<26}", format!("${}/yr (current ACR)", format_with_commas(current_acr_cost))), red),
    ]));

    lines.push(Line::from(Span::styled(sep, dim)));

    // ── Totals ──
    lines.push(Line::from(Span::styled("  TOTAL COST", yellow_bold)));
    let big_green = Style::default().fg(Color::Green).add_modifier(Modifier::BOLD);
    let big_red = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);

    lines.push(Line::from(vec![
        Span::styled("  Year 1:             ", white),
        Span::styled(format!("{:<26}", format!("${}", format_with_commas(eden_yr1))), big_green),
        Span::styled(format!("{:<26}", format!("${}", format_with_commas(self_yr1))), big_red),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Year 2+:            ", white),
        Span::styled(format!("{:<26}", format!("${}/yr", format_with_commas(eden_yr2))), big_green),
        Span::styled(format!("{:<26}", format!("${}/yr", format_with_commas(self_yr2))), big_red),
    ]));

    lines.push(Line::from(Span::styled(sep, dim)));

    // ── Savings ──
    let savings_style = Style::default().fg(Color::Green).add_modifier(Modifier::BOLD);

    lines.push(Line::from(vec![
        Span::styled("  YOU SAVE:           ", Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        Span::styled(
            format!("${} year 1", format_with_commas(save_yr1.max(0.0))),
            savings_style,
        ),
        Span::styled("  |  ", dim),
        Span::styled(
            format!("${}/yr ongoing", format_with_commas(save_yr2.max(0.0))),
            savings_style,
        ),
    ]));

    let block = Block::default()
        .title(" Total Cost Comparison — Exodus vs Self ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
}

// =============================================================================
// Docs View Rendering
// =============================================================================

const DOCS_CONTENT: &[(&str, Color, &str)] = &[
    ("EDEN REDIS MIGRATION ANALYZER", Color::Cyan, ""),
    ("", Color::White, ""),
    ("HOW PRICING WORKS", Color::Yellow, ""),
    ("", Color::White, "  Exodus annual license = 10% of your annual ACR spend (min $2,500, max 20%)."),
    ("", Color::White, "  A complexity multiplier (1.0x - 2.5x) is applied based on analysis results."),
    ("", Color::White, "  Prices rounded to nearest $100 (min $2,500/yr, capped at 20% of spend)."),
    ("", Color::White, "  Includes: migration + ongoing monitoring + analysis + AI integrations + support."),
    ("", Color::White, ""),
    ("COMPLEXITY SCORING", Color::Yellow, ""),
    ("", Color::White, "  Info findings    = 1 pt    (informational, minimal migration effort)"),
    ("", Color::White, "  Warning findings = 3 pts   (config changes or testing needed)"),
    ("", Color::White, "  Critical findings = 5 pts  (architecture changes needed)"),
    ("", Color::White, ""),
    ("", Color::White, "  Simple    0-10 pts  = 1.0x multiplier"),
    ("", Color::White, "  Moderate 11-25 pts  = 1.5x multiplier"),
    ("", Color::White, "  Difficult 26-50 pts = 2.0x multiplier"),
    ("", Color::White, "  Complex   51+ pts   = 2.5x multiplier"),
    ("", Color::White, ""),
    ("FULL PRICING EXAMPLES", Color::Yellow, ""),
    ("", Color::White, ""),
    ("Example 1: Small startup cache", Color::Cyan, ""),
    ("", Color::White, "  ACR SKU:         C1 Standard ($0.055/hr)"),
    ("", Color::White, "  Annual ACR cost: $482/yr"),
    ("", Color::White, "  Database:        500 MB, 200 ops/sec, standalone, default config"),
    ("", Color::White, "  Complexity:      Simple (score 8 — Info findings + non-TLS warning)"),
    ("", Color::White, "  Exodus base:     $1,000/yr (10% = $48, rounded to minimum)"),
    ("", Color::White, "  Multiplier:      1.0x"),
    ("", Color::Green, "  Exodus price:    $1,000/yr"),
    ("", Color::Red, "  Manual cost:     $8,000 one-time (40 hrs x $200/hr)"),
    ("", Color::Green, "  You save:        $7,000 — plus zero downtime risk"),
    ("", Color::White, ""),
    ("Example 2: Mid-size production cache", Color::Cyan, ""),
    ("", Color::White, "  ACR SKU:         P2 Premium ($0.555/hr)"),
    ("", Color::White, "  Annual ACR cost: $4,862/yr"),
    ("", Color::White, "  Database:        10 GB, 5K ops/sec, custom ACLs, RDB, Lua scripts"),
    ("", Color::White, "  Complexity:      Moderate (score 24 — ACLs +5, RDB +3, Lua +3, TLS +3, size +3, ops +3)"),
    ("", Color::White, "  Exodus base:     $1,000/yr (10% = $486, rounded to minimum)"),
    ("", Color::White, "  Multiplier:      1.5x"),
    ("", Color::Green, "  Exodus price:    $1,500/yr"),
    ("", Color::Red, "  Manual cost:     $12,000 one-time (60 hrs x $200/hr)"),
    ("", Color::Green, "  You save:        $10,500 — with built-in rollback & validation"),
    ("", Color::White, ""),
    ("Example 3: Large enterprise deployment", Color::Cyan, ""),
    ("", Color::White, "  ACR SKU:         E100 Enterprise ($3.769/hr)"),
    ("", Color::White, "  Annual ACR cost: $33,016/yr"),
    ("", Color::White, "  Database:        80 GB, 25K ops/sec, OSS cluster, keyspace notifs,"),
    ("", Color::White, "                   custom ACLs, RDB+AOF, Lua scripts, pub/sub"),
    ("", Color::White, "  Complexity:      Difficult (score 47)"),
    ("", Color::White, "  Exodus base:     $3,300/yr (10% of $33,016, rounded to $100)"),
    ("", Color::White, "  Multiplier:      2.0x"),
    ("", Color::Green, "  Exodus price:    $6,600/yr"),
    ("", Color::Red, "  Manual cost:     $16,000 one-time (80 hrs x $200/hr)"),
    ("", Color::Green, "  You save:        $9,400 — with milliseconds of downtime"),
    ("", Color::White, ""),
    ("Example 4: High-throughput complex system", Color::Cyan, ""),
    ("", Color::White, "  ACR SKU:         E400 Enterprise ($15.076/hr)"),
    ("", Color::White, "  Annual ACR cost: $132,066/yr"),
    ("", Color::White, "  Database:        300 GB, 80K ops/sec, OSS cluster, RedisGears,"),
    ("", Color::White, "                   custom ACLs, keyspace notifs, RDB+AOF, Lua, pub/sub"),
    ("", Color::White, "  Complexity:      Complex (score 69)"),
    ("", Color::White, "  Exodus base:     $13,200/yr (10% of $132,066, rounded to $100)"),
    ("", Color::White, "  Multiplier:      2.5x"),
    ("", Color::Green, "  Exodus price:    $33,000/yr"),
    ("", Color::Red, "  Manual cost:     $24,000 one-time (120 hrs x $200/hr)"),
    ("", Color::White, "  Note:            Manual is cheaper one-time, but Exodus includes"),
    ("", Color::White, "                   ongoing support, validated migration, rollback,"),
    ("", Color::White, "                   and milliseconds downtime — manual does not."),
    ("", Color::White, ""),
    ("WHY EDEN vs DOING IT YOURSELF", Color::Yellow, ""),
    ("", Color::White, ""),
    ("", Color::White, "                        Eden                    Do It Yourself"),
    ("", Color::White, "  ────────────────────────────────────────────────────────────────────"),
    ("", Color::White, "  Automation:           Fully automated         Custom scripts"),
    ("", Color::White, "  Timeline:             Minutes to hours        4-12 weeks"),
    ("", Color::White, "  Cost:                 10% of spend/yr         $8K-$24K one-time"),
    ("", Color::White, "  Downtime:             Milliseconds            Hours to days"),
    ("", Color::White, "  Data Integrity:       Checksum verified       Manual spot-checks"),
    ("", Color::White, "  Rollback:             Instant, built-in       Restore from backup"),
    ("", Color::White, "  Client Changes:       Transparent DNS cutover Rewrite conn strings"),
    ("", Color::White, "  Compliance:           Full audit trail        Manual screenshots"),
    ("", Color::White, "  Battle-Tested:        Hundreds of migrations  Your team's first try"),
    ("", Color::White, "  Support:              Included                None"),
    ("", Color::White, ""),
    ("ACR → AMR INFRASTRUCTURE SAVINGS", Color::Yellow, ""),
    ("", Color::White, ""),
    ("", Color::White, "  Azure Managed Redis (AMR) is often significantly cheaper than legacy"),
    ("", Color::White, "  Azure Cache for Redis (ACR) for the same capacity. The Recommend tab"),
    ("", Color::White, "  right-sizes your AMR instance based on your actual workload."),
    ("", Color::White, ""),
    ("", Color::White, "  Comparable examples (East US pricing):"),
    ("", Color::White, ""),
    ("", Color::White, "  Workload          Current ACR               Recommended AMR          Savings"),
    ("", Color::White, "  ──────────────────────────────────────────────────────────────────────────────"),
    ("", Color::White, "  6 GB general      P1 Premium    $2,427/yr   B5 Balanced   $1,367/yr   44%"),
    ("", Color::White, "  13 GB general     P2 Premium    $9,724/yr   B10 Balanced  $2,759/yr   72%"),
    ("", Color::White, "  26 GB general     P3 Premium   $19,438/yr   B20 Balanced  $5,510/yr   72%"),
    ("", Color::White, "  50 GB general     E50 Enterpr. $16,513/yr   B50 Balanced $11,011/yr   33%"),
    ("", Color::White, "  100 GB memory     E100 Enterpr.$33,016/yr   M100 Memory  $15,102/yr   54%"),
    ("", Color::White, "  100 GB compute    E100 Enterpr.$33,016/yr   X100 Compute $40,848/yr    --"),
    ("", Color::White, "  250 GB flash      F700 Flash   $35,136/yr   A500 Flash   $27,331/yr   22%"),
    ("", Color::White, ""),
    ("", Color::White, "  Note: Compute-optimized AMR may cost more than Enterprise ACR for the"),
    ("", Color::White, "  same capacity — but you get dedicated vCPUs and better tail latency."),
    ("", Color::White, "  The Recommend tab picks the best profile for YOUR workload pattern."),
    ("", Color::White, ""),
    ("TOTAL SAVINGS: EXODUS + AMR MIGRATION", Color::Yellow, ""),
    ("", Color::White, ""),
    ("", Color::White, "  Combining Exodus migration pricing with AMR infrastructure savings:"),
    ("", Color::White, ""),
    ("Example: P2 Premium (13 GB) → B10 Balanced", Color::Cyan, ""),
    ("", Color::White, "  Current ACR:           $9,724/yr"),
    ("", Color::White, "  New AMR:               $2,759/yr  (save $6,965/yr on infra)"),
    ("", Color::White, "  Exodus price:          $1,000/yr  (10% of $2,759 = minimum)"),
    ("", Color::Green, "  Year 1 net savings:    $5,965     ($9,724 - $2,759 - $1,000)"),
    ("", Color::Green, "  Year 2+ savings:       $6,965/yr  (Exodus is one-time)"),
    ("", Color::Red, "  Manual DIY cost:       $12,000    (60 hrs) + downtime risk"),
    ("", Color::White, ""),
    ("Example: E100 Enterprise (100 GB) → M100 Memory Optimized", Color::Cyan, ""),
    ("", Color::White, "  Current ACR:           $33,016/yr"),
    ("", Color::White, "  New AMR:               $15,102/yr (save $17,914/yr on infra)"),
    ("", Color::White, "  Exodus base:           $1,500/yr  (10% of $15,102, rounded)"),
    ("", Color::White, "  Exodus price:          $2,300/yr  ($1,500 × 1.5x Moderate)"),
    ("", Color::Green, "  Year 1 net savings:    $15,614    ($33,016 - $15,102 - $2,300)"),
    ("", Color::Green, "  Year 2+ savings:       $17,914/yr"),
    ("", Color::Red, "  Manual DIY cost:       $16,000    (80 hrs) + weeks of downtime risk"),
    ("", Color::White, ""),
    ("", Color::White, "  The migration pays for itself in infrastructure savings alone,"),
    ("", Color::White, "  before counting the avoided cost of building a manual migration."),
    ("", Color::White, ""),
    ("AMR WORKLOAD PROFILES", Color::Yellow, ""),
    ("", Color::White, "  The Recommend tab classifies your workload by ops/sec per MB of memory:"),
    ("", Color::White, ""),
    ("", Color::White, "  Ratio < 1.0     = Memory Optimized   (M-series — large data, low throughput)"),
    ("", Color::White, "  Ratio 1.0-50.0  = Balanced           (B-series — general purpose)"),
    ("", Color::White, "  Ratio > 50.0    = Compute Optimized  (X-series — high throughput, smaller data)"),
    ("", Color::White, ""),
    ("KEYBOARD SHORTCUTS", Color::Yellow, ""),
    ("", Color::White, "  1/2/3/4     Switch to Analysis / Complexity / Pricing / Recommend"),
    ("", Color::White, "  d           This documentation"),
    ("", Color::White, "  l           Open eden.dev/migrate/redis in browser"),
    ("", Color::White, "  Tab         Cycle through views"),
    ("", Color::White, "  Up/Down     Scroll / adjust values (context-dependent)"),
    ("", Color::White, "  Left/Right  Switch panel focus (Pricing view)"),
    ("", Color::White, "  Enter       Confirm selection"),
    ("", Color::White, "  q / Esc     Quit"),
    ("", Color::White, ""),
    ("", Color::Cyan, "  Learn more: https://www.eden.dev/migrate/redis"),
];

fn render_docs(frame: &mut Frame, area: Rect, state: &AppState) {
    let visible_height = area.height.saturating_sub(2) as usize;
    let total = DOCS_CONTENT.len();
    let offset = (state.docs_scroll as usize).min(total.saturating_sub(visible_height));

    let lines: Vec<Line> = DOCS_CONTENT
        .iter()
        .skip(offset)
        .take(visible_height)
        .map(|(heading, color, body)| {
            if !heading.is_empty() {
                Line::from(Span::styled(
                    format!("  {}", heading),
                    Style::default().fg(*color).add_modifier(Modifier::BOLD),
                ))
            } else {
                Line::from(Span::styled(*body, Style::default().fg(*color)))
            }
        })
        .collect();

    let scroll_indicator = if total > visible_height {
        format!(" [{}/{}] ", offset + 1, total)
    } else {
        String::new()
    };

    let block = Block::default()
        .title(format!(" Documentation{} — Up/Down to scroll ", scroll_indicator))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
}

// =============================================================================
// Recommend View Rendering
// =============================================================================

fn render_overprovision_control(frame: &mut Frame, area: Rect, state: &AppState) {
    let pct = state.overprovision_pct;
    let dim = Style::default().fg(Color::DarkGray);
    let cyan_bold = Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD);

    let bar_width = 20usize;
    let filled = ((pct as f64 / 100.0) * bar_width as f64).round() as usize;
    let bar = format!(
        "[{}{}] {}%",
        "█".repeat(filled),
        "░".repeat(bar_width.saturating_sub(filled)),
        pct,
    );

    let base_mb = state.db_size_override_mb.unwrap_or_else(|| {
        state.current_metrics.as_ref()
            .map(|m| m.used_memory_bytes / (1024 * 1024))
            .unwrap_or(0)
    });
    let base_gb = base_mb as f64 / 1024.0;
    let target_gb = base_gb * (1.0 + pct as f64 / 100.0);

    let size_label = if state.db_size_override_mb.is_some() {
        format!("{:.2} GB (manual)", base_gb)
    } else {
        format!("{:.2} GB (detected)", base_gb)
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("  Database Size:     ", dim),
            Span::styled(size_label, Style::default().fg(Color::White)),
            Span::styled("  (Left/Right ±100 MB)", dim),
        ]),
        Line::from(vec![
            Span::styled("  Overprovisioning:  ", dim),
            Span::styled(bar, cyan_bold),
            Span::styled(
                format!("  → Target: {:.2} GB", target_gb),
                Style::default().fg(Color::White),
            ),
            Span::styled("  (Up/Down ±5%)", dim),
        ]),
    ];

    let block = Block::default()
        .title(" Sizing Controls ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
}

fn render_recommendation(frame: &mut Frame, area: Rect, state: &AppState) {
    if state.azure_confirmed_region.is_none() {
        let msg = Paragraph::new(Line::from(Span::styled(
            "Select a region on the Pricing tab (3) first to get a recommendation.",
            Style::default().fg(Color::DarkGray),
        )))
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(" AMR Recommendation ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(msg, area);
        return;
    }

    if state.current_metrics.is_none() {
        let msg = Paragraph::new(Line::from(Span::styled(
            "Waiting for database metrics...",
            Style::default().fg(Color::Yellow),
        )))
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(" AMR Recommendation ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(msg, area);
        return;
    }

    let lines = if let Some(ref rec) = state.recommendation {
        let green = Style::default().fg(Color::Green).add_modifier(Modifier::BOLD);
        let cyan = Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD);
        let dim = Style::default().fg(Color::DarkGray);
        let white = Style::default().fg(Color::White);

        let profile_color = match rec.profile {
            AmrProfile::Memory => Color::Magenta,
            AmrProfile::Balanced => Color::Cyan,
            AmrProfile::Compute => Color::Yellow,
        };

        let mut lines = vec![
            Line::from(vec![
                Span::styled("  Workload Profile:  ", dim),
                Span::styled(
                    rec.profile.label(),
                    Style::default().fg(profile_color).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("  (ratio: {:.1} ops/MB — {}-series SKUs)", rec.ratio, rec.profile.sku_prefix_hint()),
                    dim,
                ),
            ]),
            Line::from(vec![
                Span::styled("  Current Usage:     ", dim),
                Span::styled(
                    format!("{:.2} GB memory, {} ops/sec", rec.current_memory_gb, format_number(rec.current_ops)),
                    white,
                ),
            ]),
            Line::from(vec![
                Span::styled(
                    format!("  Target ({}% over):  ", rec.overprovision_pct),
                    dim,
                ),
                Span::styled(
                    format!("{:.2} GB", rec.target_memory_gb),
                    cyan,
                ),
            ]),
            Line::from(""),
        ];

        if let Some(ref sku) = rec.recommended_sku {
            let capacity = parse_sku_capacity_gb(&sku.sku_name);
            lines.push(Line::from(vec![
                Span::styled("  Recommended SKU:   ", dim),
                Span::styled(
                    format!("{} — {} ({} GB)", sku.sku_name, sku.meter_name, capacity),
                    green,
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  AMR Annual Cost:   ", dim),
                Span::styled(
                    format!("${}/yr", format_with_commas(rec.recommended_annual_cost)),
                    green,
                ),
                Span::styled(
                    format!("  (${:.4}/hr)", sku.retail_price),
                    dim,
                ),
            ]));

            if let Some(current_cost) = rec.current_sku_annual_cost {
                let diff = current_cost - rec.recommended_annual_cost;
                if diff > 0.0 {
                    lines.push(Line::from(vec![
                        Span::styled("  vs Current ACR:    ", dim),
                        Span::styled(
                            format!("${}/yr", format_with_commas(current_cost)),
                            Style::default().fg(Color::Red),
                        ),
                        Span::styled("  →  ", dim),
                        Span::styled(
                            format!("Save ${}/yr on infrastructure", format_with_commas(diff)),
                            green,
                        ),
                    ]));
                } else {
                    lines.push(Line::from(vec![
                        Span::styled("  vs Current ACR:    ", dim),
                        Span::styled(
                            format!("${}/yr", format_with_commas(current_cost)),
                            white,
                        ),
                        Span::styled(
                            format!("  (AMR: +${}/yr — right-sized for your workload)", format_with_commas(diff.abs())),
                            dim,
                        ),
                    ]));
                }
            }
        } else {
            lines.push(Line::from(vec![
                Span::styled("  No matching SKU found. ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!("Need {:.2} GB — largest available may be too small.", rec.target_memory_gb),
                    dim,
                ),
            ]));
        }

        lines
    } else {
        vec![Line::from(Span::styled(
            "Calculating recommendation...",
            Style::default().fg(Color::Yellow),
        ))]
    };

    let block = Block::default()
        .title(" AMR Recommendation — Right-Sized for Your Workload ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false }).block(block);
    frame.render_widget(paragraph, area);
}

// =============================================================================
// Console Output (non-TUI mode)
// =============================================================================

fn output_console(result: &AnalysisResult) {
    println!();
    println!("{}", "Eden Redis Migration Analyzer".bold().cyan());
    println!("{}", "=============================".cyan());
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

fn output_complexity_console(complexity: &CustomerComplexity) {
    println!("{}", "Complexity Analysis".bold().magenta());
    println!("{}", "===================".magenta());
    println!();

    // Summary
    let critical = complexity.findings.iter().filter(|f| f.severity == Severity::Critical).count();
    let warnings = complexity.findings.iter().filter(|f| f.severity == Severity::Warning).count();

    let score_label = if complexity.total_score <= 10 {
        format!("{}", complexity.total_score).green()
    } else if complexity.total_score <= 25 {
        format!("{}", complexity.total_score).yellow()
    } else {
        format!("{}", complexity.total_score).red()
    };

    println!("  Complexity Score: {} ({} critical, {} warnings)", score_label, critical, warnings);
    println!(
        "  Cluster Mode:     {}",
        complexity.cluster_mode.as_deref().unwrap_or("unknown")
    );
    println!(
        "  Modules:          {}",
        if complexity.loaded_modules.is_empty() {
            "none".to_string()
        } else {
            complexity.loaded_modules.join(", ")
        }
    );
    println!("  ACL Rules:        {}", complexity.acl_rules.len());
    println!(
        "  Persistence:      {}{}",
        if complexity.persistence_config.rdb_enabled { "RDB " } else { "" },
        if complexity.persistence_config.aof_enabled {
            "AOF"
        } else if !complexity.persistence_config.rdb_enabled {
            "none (ephemeral)"
        } else {
            ""
        }
    );
    println!();

    // Findings
    println!("{}", "Findings".bold().yellow());
    println!("{}", "--------".yellow());

    for finding in &complexity.findings {
        let severity_label = match finding.severity {
            Severity::Critical => "CRIT".red().bold(),
            Severity::Warning => "WARN".yellow().bold(),
            Severity::Info => "INFO".green(),
        };

        println!(
            "  [{}] {}: {}",
            severity_label,
            finding.category.cyan(),
            finding.title.bold()
        );
        println!("         {}", finding.detail.dimmed());
    }
    println!();
}

fn output_pricing_console(skus: &[AzureSkuPrice], complexity: &CustomerComplexity, region: &str, data_size_bytes: u64) {
    let tier = PricingTier::from_score(complexity.total_score);

    println!("{}", "Exodus Annual License".bold().green());
    println!("{}", "=======================".green());
    println!();
    println!("  Pricing Tier:  {} (complexity score: {})", tier.label().bold(), complexity.total_score);
    println!("  Azure Region:  {}", region);
    println!();

    // Migration time comparison
    let time = MigrationTimeEstimate::calculate(data_size_bytes, tier);
    println!("{}", "Exodus vs Manual Migration".bold().magenta());
    println!("{}", "==========================".magenta());
    println!("  Data Size:     {:.2} GB", time.data_size_gb);
    println!();
    println!("  {:<20} {:<24} {}",
        "", "Eden".green().bold(), "Do It Yourself".red().bold(),
    );
    println!("  {}", "-".repeat(68).dimmed());
    println!("  {:<20} {:<24} {}",
        "Automation:", "Fully automated".green(), "Custom scripts".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Timeline:", time.exodus_summary().green(), time.manual_summary().red(),
    );
    println!("  {:<20} {:<24} {}",
        "Cost:",
        "10% of Azure spend/yr".green(),
        format!("${} one-time", format_with_commas(time.manual_cost)).red(),
    );
    println!("  {:<20} {:<24} {}",
        "Downtime:", "Milliseconds".green(), "Hours to days".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Data Integrity:", "Checksum verified".green(), "Manual spot-checks".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Rollback:", "Instant, built-in".green(), "Restore from backup".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Client Changes:", "Transparent DNS cutover".green(), "Rewrite conn strings".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Compliance:", "Full audit trail".green(), "Manual screenshots".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Battle-Tested:", "Hundreds of migrations".green(), "Your team's first try".red(),
    );
    println!("  {:<20} {:<24} {}",
        "Support:", "Included".green(), "None".red(),
    );
    println!();

    if skus.is_empty() {
        println!("  {} No ACR SKUs found for region '{}'", "!".yellow(), region);
        return;
    }

    let mult = tier.complexity_multiplier();

    // Print table header
    println!(
        "  {:<16} {:<24} {:>10} {:>14} {} {:>12} {:>18}",
        "SKU".yellow().bold(),
        "Meter".yellow().bold(),
        "$/Hour".yellow().bold(),
        "Annual Cost".yellow().bold(),
        "│".dimmed(),
        "Exodus Base".cyan().bold(),
        format!("{}x Multiple*", mult).green().bold(),
    );
    println!("  {:<16} {:<24} {:>10} {:>14} {} {:>12} {:>18}",
        "", "", "", "",
        "│".dimmed(),
        "".dimmed(), "".dimmed(),
    );

    for sku in skus {
        let annual = sku.annual_cost();
        let base = sku.exodus_base_price();
        let cap = ((annual * 0.20) / 100.0).round() * 100.0;
        let est = ((base * mult) / 100.0).round() * 100.0;
        let est = est.min(cap).max(2500.0);

        println!(
            "  {:<16} {:<24} {:>10.4} {:>14} {} {:>12} {:>18}",
            sku.sku_name, sku.meter_name, sku.retail_price,
            format_with_commas(annual),
            "│".dimmed(),
            format_with_commas(base).to_string().cyan(),
            format_with_commas(est).to_string().green(),
        );
    }
    println!();
    println!("  {}", "* Estimate only — final price depends on full complexity assessment".dimmed());
    println!();
}

fn output_json(result: &AnalysisResult, complexity: &CustomerComplexity, pricing: Option<&[AzureSkuPrice]>, region: &str) {
    let tier = PricingTier::from_score(complexity.total_score);
    let data_size_bytes = result.metrics.used_memory_bytes;

    let mult = tier.complexity_multiplier();

    let pricing_estimates: Vec<serde_json::Value> = pricing
        .unwrap_or(&[])
        .iter()
        .map(|sku| {
            let estimate = PricingEstimate::calculate(sku, tier, Some(data_size_bytes));
            serde_json::json!({
                "sku_name": sku.sku_name,
                "meter_name": sku.meter_name,
                "retail_price_per_hour": sku.retail_price,
                "annual_azure_spend": estimate.annual_azure_spend,
                "exodus_base_price": estimate.base_price,
                "complexity_multiplier": estimate.complexity_multiplier,
                "estimated_price": estimate.estimated_price,
                "note": "Estimate only — final price depends on full complexity assessment",
            })
        })
        .collect();

    let time = MigrationTimeEstimate::calculate(data_size_bytes, tier);

    let report = serde_json::json!({
        "analysis": result,
        "complexity": complexity,
        "pricing": {
            "tier": format!("{:?}", tier),
            "complexity_multiplier": mult,
            "base_rate": "10% of annual Azure spend",
            "azure_region": region,
            "estimates": pricing_estimates,
        },
        "migration_time": {
            "data_size_gb": time.data_size_gb,
            "exodus": {
                "setup_hours": time.exodus_setup_hours,
                "migration_hours": time.exodus_migration_hours,
                "total_hours": time.exodus_total_hours,
                "summary": time.exodus_summary(),
            },
            "manual": {
                "weeks": time.manual_weeks,
                "hours": time.manual_hours,
                "hourly_rate": MANUAL_HOURLY_RATE,
                "estimated_cost": time.manual_cost,
                "summary": time.manual_summary(),
            },
        }
    });

    match serde_json::to_string_pretty(&report) {
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
        let complexity = analyze_customer_complexity(&mut conn, &result.metrics).await?;

        // Fetch Azure pricing in parallel with analysis
        let azure_skus = match fetch_azure_redis_pricing(&config.azure_region).await {
            Ok(skus) => skus,
            Err(e) => {
                eprintln!("{}: {}", "Warning: Could not fetch Azure pricing".yellow(), e);
                Vec::new()
            }
        };

        match config.output_format {
            Some(OutputFormat::Json) => output_json(&result, &complexity, Some(&azure_skus), &config.azure_region),
            _ => {
                output_console(&result);
                output_complexity_console(&complexity);
                output_pricing_console(&azure_skus, &complexity, &config.azure_region, result.metrics.used_memory_bytes);
                println!("{}", format!("Learn more: {}", EDEN_LEARN_MORE_URL).cyan());
                println!();
            }
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
                            // Build peak metrics for complexity scoring
                            let peak = {
                                let s = state_clone.read().await;
                                DatabaseMetrics {
                                    used_memory_bytes: s.historical.max_memory_bytes.max(analysis.metrics.used_memory_bytes),
                                    total_keys: s.historical.max_keys.max(analysis.metrics.total_keys),
                                    ops_per_sec: s.historical.max_ops_per_sec.max(analysis.metrics.ops_per_sec),
                                    redis_version: analysis.metrics.redis_version.clone(),
                                    connected_clients: analysis.metrics.connected_clients,
                                }
                            };

                            // Run complexity analysis using peak values
                            let complexity = analyze_customer_complexity(conn, &peak).await.ok();

                            let mut state = state_clone.write().await;

                            // Update historical tracking
                            state.historical.update(&analysis.metrics);
                            state.current_metrics = Some(analysis.metrics.clone());
                            state.result = Some(analysis);
                            if complexity.is_some() {
                                state.complexity = complexity;
                                state.update_pricing_estimate();
                            }
                            state.update_recommendation();
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

fn spawn_azure_fetch(state: &Arc<RwLock<AppState>>, region: String) {
    let state_azure = Arc::clone(state);
    tokio::spawn(async move {
        {
            let mut s = state_azure.write().await;
            s.azure_loading = true;
            s.azure_error = None;
            s.azure_skus.clear();
            s.azure_acr_skus.clear();
            s.azure_selected_sku = 0;
            s.azure_confirmed_sku = None;
            s.pricing_estimate = None;
            s.recommendation = None;
        }
        match fetch_azure_redis_pricing(&region).await {
            Ok(skus) => {
                let mut s = state_azure.write().await;
                s.azure_acr_skus = skus.iter().filter(|s| !s.is_amr()).cloned().collect();
                s.azure_skus = skus;
                s.azure_loading = false;
                s.pricing_focus = PricingFocus::Sku;
                s.update_recommendation();
            }
            Err(e) => {
                let mut s = state_azure.write().await;
                s.azure_error = Some(format!("{}", e));
                s.azure_loading = false;
            }
        }
    });
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
                        KeyCode::Char('l') => {
                            // Open learn more URL in default browser
                            let _ = open_url("https://www.eden.dev/migrate/redis");
                        }
                        KeyCode::Tab => {
                            let mut state_w = state.write().await;
                            state_w.active_view = match state_w.active_view {
                                TuiView::Analysis => TuiView::Complexity,
                                TuiView::Complexity => TuiView::Pricing,
                                TuiView::Pricing => TuiView::Recommend,
                                TuiView::Recommend => TuiView::Summary,
                                TuiView::Summary => TuiView::Docs,
                                TuiView::Docs => TuiView::Analysis,
                            };
                        }
                        KeyCode::Char('1') => {
                            state.write().await.active_view = TuiView::Analysis;
                        }
                        KeyCode::Char('2') => {
                            state.write().await.active_view = TuiView::Complexity;
                        }
                        KeyCode::Char('3') => {
                            state.write().await.active_view = TuiView::Pricing;
                        }
                        KeyCode::Char('4') => {
                            let mut state_w = state.write().await;
                            state_w.active_view = TuiView::Recommend;
                            state_w.update_recommendation();
                        }
                        KeyCode::Char('5') => {
                            state.write().await.active_view = TuiView::Summary;
                        }
                        KeyCode::Char('d') => {
                            state.write().await.active_view = TuiView::Docs;
                        }
                        KeyCode::Up => {
                            let mut state_w = state.write().await;
                            match state_w.active_view {
                                TuiView::Pricing => match state_w.pricing_focus {
                                    PricingFocus::Region => {
                                        if state_w.azure_selected_region > 0 {
                                            state_w.azure_selected_region -= 1;
                                        }
                                    }
                                    PricingFocus::Sku => {
                                        if state_w.azure_selected_sku > 0 {
                                            state_w.azure_selected_sku -= 1;
                                        }
                                    }
                                },
                                TuiView::Complexity => {
                                    if state_w.complexity_scroll > 0 {
                                        state_w.complexity_scroll -= 1;
                                    }
                                }
                                TuiView::Recommend => {
                                    if state_w.overprovision_pct < 100 {
                                        state_w.overprovision_pct += 5;
                                        state_w.update_recommendation();
                                    }
                                }
                                TuiView::Docs => {
                                    if state_w.docs_scroll > 0 {
                                        state_w.docs_scroll -= 1;
                                    }
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Down => {
                            let mut state_w = state.write().await;
                            match state_w.active_view {
                                TuiView::Pricing => match state_w.pricing_focus {
                                    PricingFocus::Region => {
                                        if state_w.azure_selected_region < AZURE_REGIONS.len().saturating_sub(1) {
                                            state_w.azure_selected_region += 1;
                                        }
                                    }
                                    PricingFocus::Sku => {
                                        let max = state_w.azure_acr_skus.len().saturating_sub(1);
                                        if state_w.azure_selected_sku < max {
                                            state_w.azure_selected_sku += 1;
                                        }
                                    }
                                },
                                TuiView::Complexity => {
                                    let max = state_w
                                        .complexity
                                        .as_ref()
                                        .map(|c| c.findings.len().saturating_sub(1) as u16)
                                        .unwrap_or(0);
                                    if state_w.complexity_scroll < max {
                                        state_w.complexity_scroll += 1;
                                    }
                                }
                                TuiView::Recommend => {
                                    if state_w.overprovision_pct >= 5 {
                                        state_w.overprovision_pct -= 5;
                                        state_w.update_recommendation();
                                    }
                                }
                                TuiView::Docs => {
                                    let max = DOCS_CONTENT.len().saturating_sub(1) as u16;
                                    if state_w.docs_scroll < max {
                                        state_w.docs_scroll += 1;
                                    }
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Left => {
                            let mut state_w = state.write().await;
                            match state_w.active_view {
                                TuiView::Pricing => {
                                    state_w.pricing_focus = PricingFocus::Region;
                                }
                                TuiView::Recommend => {
                                    // Decrease DB size by 100 MB
                                    let current_mb = state_w.db_size_override_mb.unwrap_or_else(|| {
                                        state_w.current_metrics.as_ref()
                                            .map(|m| m.used_memory_bytes / (1024 * 1024))
                                            .unwrap_or(0)
                                    });
                                    if current_mb >= 100 {
                                        state_w.db_size_override_mb = Some(current_mb - 100);
                                        state_w.update_recommendation();
                                    }
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Right => {
                            let mut state_w = state.write().await;
                            match state_w.active_view {
                                TuiView::Pricing => {
                                    if !state_w.azure_acr_skus.is_empty() {
                                        state_w.pricing_focus = PricingFocus::Sku;
                                    }
                                }
                                TuiView::Recommend => {
                                    // Increase DB size by 100 MB
                                    let current_mb = state_w.db_size_override_mb.unwrap_or_else(|| {
                                        state_w.current_metrics.as_ref()
                                            .map(|m| m.used_memory_bytes / (1024 * 1024))
                                            .unwrap_or(0)
                                    });
                                    state_w.db_size_override_mb = Some(current_mb + 100);
                                    state_w.update_recommendation();
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Enter => {
                            let mut state_w = state.write().await;
                            if state_w.active_view == TuiView::Pricing {
                                match state_w.pricing_focus {
                                    PricingFocus::Region => {
                                        let idx = state_w.azure_selected_region;
                                        if idx < AZURE_REGIONS.len() && !state_w.azure_loading {
                                            let already_selected = state_w.azure_confirmed_region == Some(idx);
                                            if !already_selected {
                                                state_w.azure_confirmed_region = Some(idx);
                                                let region = AZURE_REGIONS[idx].0.to_string();
                                                drop(state_w);
                                                spawn_azure_fetch(state, region);
                                            }
                                        }
                                    }
                                    PricingFocus::Sku => {
                                        if !state_w.azure_acr_skus.is_empty() {
                                            state_w.azure_confirmed_sku = Some(state_w.azure_selected_sku);
                                            state_w.update_pricing_estimate();
                                        }
                                    }
                                }
                            }
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
    use std::sync::LazyLock;
    use tokio::sync::Mutex as TokioMutex;

    /// Integration tests share a single Redis instance and mutate global state
    /// (ACL rules, CONFIG settings). This mutex serializes them to prevent races.
    static REDIS_LOCK: LazyLock<TokioMutex<()>> = LazyLock::new(|| TokioMutex::new(()));

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

    // =========================================================================
    // Customer Complexity Unit Tests
    // =========================================================================

    #[test]
    fn test_complexity_scoring() {
        let mut c = CustomerComplexity::new();
        assert_eq!(c.total_score, 0);

        c.add_finding("test", Severity::Info, "info", "detail");
        assert_eq!(c.total_score, 1); // Info = 1

        c.add_finding("test", Severity::Warning, "warn", "detail");
        assert_eq!(c.total_score, 4); // 1 + 3

        c.add_finding("test", Severity::Critical, "crit", "detail");
        assert_eq!(c.total_score, 9); // 1 + 3 + 5
    }

    #[test]
    fn test_complexity_finding_counts() {
        let mut c = CustomerComplexity::new();
        c.add_finding("A", Severity::Critical, "c1", "");
        c.add_finding("A", Severity::Critical, "c2", "");
        c.add_finding("B", Severity::Warning, "w1", "");
        c.add_finding("C", Severity::Info, "i1", "");

        assert_eq!(c.findings.len(), 4);
        assert_eq!(
            c.findings.iter().filter(|f| f.severity == Severity::Critical).count(),
            2
        );
        assert_eq!(
            c.findings.iter().filter(|f| f.severity == Severity::Warning).count(),
            1
        );
        assert_eq!(
            c.findings.iter().filter(|f| f.severity == Severity::Info).count(),
            1
        );
    }

    #[test]
    fn test_complexity_serialization() {
        let mut c = CustomerComplexity::new();
        c.cluster_mode = Some("OSS Cluster".to_string());
        c.loaded_modules = vec!["redisgears".to_string()];
        c.acl_rules = vec!["user default on ~* +@all".to_string()];
        c.keyspace_notifications = "KEA".to_string();
        c.max_memory_policy = "allkeys-lru".to_string();
        c.persistence_config = PersistenceConfig {
            rdb_enabled: true,
            aof_enabled: false,
            rdb_save_params: "3600 1 300 100".to_string(),
            aof_fsync: String::new(),
        };
        c.add_finding("Clustering", Severity::Critical, "OSS Cluster", "detail");

        let json = serde_json::to_string(&c).unwrap();
        let deserialized: CustomerComplexity = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cluster_mode, Some("OSS Cluster".to_string()));
        assert_eq!(deserialized.loaded_modules, vec!["redisgears"]);
        assert_eq!(deserialized.findings.len(), 1);
        assert_eq!(deserialized.total_score, 5); // 1 Critical = 5
        assert!(deserialized.persistence_config.rdb_enabled);
        assert!(!deserialized.persistence_config.aof_enabled);
    }

    #[test]
    fn test_persistence_config_default() {
        let p = PersistenceConfig::default();
        assert!(!p.rdb_enabled);
        assert!(!p.aof_enabled);
        assert!(p.rdb_save_params.is_empty());
        assert!(p.aof_fsync.is_empty());
    }

    // =========================================================================
    // Integration Tests (require a running Redis on localhost:6379)
    // =========================================================================

    async fn get_test_conn() -> Option<MultiplexedConnection> {
        let config = Config {
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            username: None,
            tls: false,
            tls_insecure: false,
            db: 15, // Use DB 15 for tests to avoid conflicts
            sample_rate: 0.05,
            min_samples: 10,
            max_samples: 100,
            output_format: None,
            interval: 5,
            once: false,
            azure_region: "eastus".to_string(),
        };
        connect_redis(&config).await.ok()
    }

    #[tokio::test]
    async fn test_integration_full_complexity_analysis() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available on localhost:6379");
            return;
        };

        // Clean up any state from other tests for a clean baseline
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("")
            .query_async(&mut conn)
            .await;
        let _: Result<String, _> = redis::cmd("ACL")
            .arg("DELUSER")
            .arg("eden_test_user")
            .query_async(&mut conn)
            .await;
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("noeviction")
            .query_async(&mut conn)
            .await;

        let metrics = fetch_database_metrics(&mut conn).await.unwrap();
        let result = analyze_customer_complexity(&mut conn, &metrics).await;
        assert!(result.is_ok(), "analyze_customer_complexity failed: {:?}", result.err());

        let complexity = result.unwrap();

        // Local Redis 8.6.1 standalone with 5 modules, RDB on, no AOF, no keyspace notifs,
        // default ACLs, noeviction policy, eval calls in stats, no pub/sub.
        // Expected findings per category:
        //   Clustering:    1 Info  (cluster disabled)
        //   ACLs:          1 Info  (default only)
        //   Modules:       5 (mix of Info + Warning for Flash-limited)
        //   Features:      1 Info  (keyspace disabled) + 1 Warn (Lua scripts)
        //   Persistence:   1 Warn  (RDB enabled)
        //   Configuration: 1 Info  (noeviction — standard)
        //   Connection:    1-2 (endpoint change Info + possibly non-TLS Warning)
        //   Commands:      0-1 (multi-key if any detected)
        //   Pub/Sub:       0

        // Validate finding count per category
        let by_category = |cat: &str| -> Vec<&ComplexityFinding> {
            complexity.findings.iter().filter(|f| f.category == cat).collect()
        };

        assert_eq!(by_category("Clustering").len(), 1, "Expected 1 clustering finding");
        assert_eq!(by_category("ACLs").len(), 1, "Expected 1 ACL finding");
        assert!(by_category("Modules").len() >= 1, "Expected at least 1 module finding");
        assert_eq!(
            by_category("Persistence").len(), 1,
            "Expected 1 persistence finding (RDB Warning)"
        );
        assert_eq!(by_category("Configuration").len(), 1, "Expected 1 config finding");
        assert!(
            !by_category("Connection").is_empty(),
            "Expected at least 1 connection finding"
        );

        // With default ACLs only, ACL finding should be Info (not Critical)
        let acl_findings = by_category("ACLs");
        assert_eq!(acl_findings[0].severity, Severity::Info, "Default ACLs should be Info");

        // Warnings should include RDB + Lua + Flash-limited modules
        let warnings: Vec<_> = complexity.findings.iter().filter(|f| f.severity == Severity::Warning).collect();
        assert!(
            warnings.len() >= 2,
            "Expected at least 2 warnings (RDB + Lua or Flash modules), got {}: {:?}",
            warnings.len(),
            warnings.iter().map(|f| &f.title).collect::<Vec<_>>()
        );

        // Validate specific field values
        assert!(
            complexity.cluster_mode.as_ref().unwrap().contains("cluster disabled"),
            "Expected 'cluster disabled', got: {}",
            complexity.cluster_mode.as_ref().unwrap()
        );
        assert_eq!(
            complexity.max_memory_policy, "noeviction",
            "Expected noeviction policy"
        );
        assert!(
            complexity.keyspace_notifications.is_empty(),
            "Expected empty keyspace notifications"
        );

        // Validate total score is consistent with findings
        let expected_score: u32 = complexity.findings.iter().map(|f| match f.severity {
            Severity::Info => 1,
            Severity::Warning => 3,
            Severity::Critical => 5,
        }).sum();
        assert_eq!(
            complexity.total_score, expected_score,
            "total_score {} doesn't match sum of finding severities {}",
            complexity.total_score, expected_score
        );
    }

    #[tokio::test]
    async fn test_integration_clustering_standalone() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        let mut complexity = CustomerComplexity::new();
        analyze_clustering(&mut conn, &mut complexity).await;

        // Local Redis has cluster support disabled — CLUSTER INFO returns error
        assert_eq!(
            complexity.cluster_mode.as_deref(),
            Some("Standalone (cluster disabled)"),
            "Expected 'Standalone (cluster disabled)', got: {:?}",
            complexity.cluster_mode
        );

        // Exactly 1 finding: Info severity, "Cluster Commands Disabled"
        let cluster_findings: Vec<_> = complexity
            .findings
            .iter()
            .filter(|f| f.category == "Clustering")
            .collect();
        assert_eq!(cluster_findings.len(), 1, "Expected exactly 1 clustering finding");
        assert_eq!(cluster_findings[0].severity, Severity::Info);
        assert_eq!(cluster_findings[0].title, "Cluster Commands Disabled");

        // Score: 1 Info = 1
        assert_eq!(complexity.total_score, 1, "Expected score 1 for standalone Info finding");
    }

    #[tokio::test]
    async fn test_integration_acl_default_and_custom() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        // Clean up any leftover test users first
        let _: Result<String, _> = redis::cmd("ACL")
            .arg("DELUSER")
            .arg("eden_test_user")
            .query_async(&mut conn)
            .await;

        // --- Phase 1: Default ACLs only ---
        let mut complexity = CustomerComplexity::new();
        analyze_acls(&mut conn, &mut complexity).await;

        // Should detect exactly the default ACL rule
        assert_eq!(
            complexity.acl_rules.len(), 1,
            "Expected 1 ACL rule (default), got {}",
            complexity.acl_rules.len()
        );
        assert!(
            complexity.acl_rules[0].contains("default"),
            "Expected default ACL rule, got: {}",
            complexity.acl_rules[0]
        );

        // With only default user, should produce 1 Info finding "Default ACLs Only"
        assert_eq!(complexity.findings.len(), 1, "Expected exactly 1 ACL finding");
        assert_eq!(complexity.findings[0].severity, Severity::Info);
        assert_eq!(complexity.findings[0].title, "Default ACLs Only");
        assert_eq!(complexity.total_score, 1, "Expected score 1 for default ACL Info");

        // --- Phase 2: Add a custom ACL user ---
        let _: Result<String, _> = redis::cmd("ACL")
            .arg("SETUSER")
            .arg("eden_test_user")
            .arg("on")
            .arg(">testpass123")
            .arg("~eden:*")
            .arg("+get")
            .arg("+set")
            .query_async(&mut conn)
            .await;

        let mut complexity2 = CustomerComplexity::new();
        analyze_acls(&mut conn, &mut complexity2).await;

        // Should now have 2 ACL rules (default + eden_test_user)
        assert_eq!(
            complexity2.acl_rules.len(), 2,
            "Expected 2 ACL rules, got {}: {:?}",
            complexity2.acl_rules.len(), complexity2.acl_rules
        );

        // Exactly 1 finding: Critical about 1 custom rule (AMR doesn't support Redis ACL RBAC)
        assert_eq!(complexity2.findings.len(), 1, "Expected exactly 1 finding");
        assert_eq!(complexity2.findings[0].severity, Severity::Critical);
        assert_eq!(complexity2.findings[0].category, "ACLs");
        assert!(
            complexity2.findings[0].title.contains("1 Custom ACL Rule"),
            "Expected '1 Custom ACL Rule' in title, got: {}",
            complexity2.findings[0].title
        );
        assert_eq!(complexity2.total_score, 5, "Expected score 5 for 1 Critical");

        // Clean up
        let _: Result<String, _> = redis::cmd("ACL")
            .arg("DELUSER")
            .arg("eden_test_user")
            .query_async(&mut conn)
            .await;
    }

    #[tokio::test]
    async fn test_integration_module_analysis() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        let mut complexity = CustomerComplexity::new();
        analyze_modules(&mut conn, &mut complexity).await;

        // Local Redis 8.6.1 has 5 modules: timeseries, search, bf, vectorset, ReJSON
        // None match AMR_UNSUPPORTED_MODULES (redisgears, redisgraph)
        // So all 5 should be Info "Module: <name>" findings
        assert!(
            complexity.loaded_modules.len() >= 4,
            "Expected at least 4 modules, got {}: {:?}",
            complexity.loaded_modules.len(), complexity.loaded_modules
        );

        let module_findings: Vec<_> = complexity
            .findings
            .iter()
            .filter(|f| f.category == "Modules")
            .collect();
        assert_eq!(
            module_findings.len(), complexity.loaded_modules.len(),
            "Expected one finding per module"
        );

        // No Critical findings (no completely unavailable modules like RedisGears/Graph)
        let critical_modules: Vec<_> = module_findings
            .iter()
            .filter(|f| f.severity == Severity::Critical)
            .collect();
        assert_eq!(
            critical_modules.len(), 0,
            "Expected 0 critical module findings, got: {:?}",
            critical_modules.iter().map(|f| &f.title).collect::<Vec<_>>()
        );

        // Flash-limited modules (timeseries, search, bf) should be Warning
        // Other modules (ReJSON, vectorset) should be Info
        let warning_modules: Vec<_> = module_findings
            .iter()
            .filter(|f| f.severity == Severity::Warning)
            .collect();
        let info_modules: Vec<_> = module_findings
            .iter()
            .filter(|f| f.severity == Severity::Info)
            .collect();

        assert!(
            !warning_modules.is_empty(),
            "Expected at least 1 Flash-limited Warning module"
        );
        assert!(
            !info_modules.is_empty(),
            "Expected at least 1 fully-compatible Info module"
        );

        // Score: Warnings = 3 each, Info = 1 each
        let expected_score = (warning_modules.len() as u32 * 3) + (info_modules.len() as u32);
        assert_eq!(
            complexity.total_score, expected_score,
            "Score should be {} ({}*3 warns + {}*1 infos)",
            expected_score, warning_modules.len(), info_modules.len()
        );
    }

    #[tokio::test]
    async fn test_integration_keyspace_notifications_disabled_then_enabled() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        // --- Phase 1: Ensure disabled ---
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("")
            .query_async(&mut conn)
            .await;

        let mut complexity = CustomerComplexity::new();
        analyze_keyspace_notifications(&mut conn, &mut complexity).await;

        // Should produce exactly 1 Info finding
        assert_eq!(complexity.findings.len(), 1, "Expected exactly 1 finding (disabled)");
        assert_eq!(complexity.findings[0].severity, Severity::Info);
        assert_eq!(complexity.findings[0].title, "Keyspace Notifications Disabled");
        assert_eq!(complexity.findings[0].category, "Features");
        assert!(complexity.keyspace_notifications.is_empty());
        assert_eq!(complexity.total_score, 1); // Info = 1

        // --- Phase 2: Enable keyspace notifications ---
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("KEA")
            .query_async(&mut conn)
            .await;

        let mut complexity2 = CustomerComplexity::new();
        analyze_keyspace_notifications(&mut conn, &mut complexity2).await;

        // Redis normalizes flag order, but K and E must be present
        assert!(
            !complexity2.keyspace_notifications.is_empty(),
            "Expected non-empty keyspace notifications"
        );
        assert!(
            complexity2.keyspace_notifications.contains('K')
                && complexity2.keyspace_notifications.contains('E'),
            "Expected K and E flags, got: {}",
            complexity2.keyspace_notifications
        );

        // Should produce exactly 1 Critical finding
        assert_eq!(complexity2.findings.len(), 1, "Expected exactly 1 finding (enabled)");
        assert_eq!(complexity2.findings[0].severity, Severity::Critical);
        assert_eq!(complexity2.findings[0].title, "Keyspace Notifications Enabled");
        assert_eq!(complexity2.findings[0].category, "Features");
        assert_eq!(complexity2.total_score, 5, "Expected score 5 for 1 Critical");

        // Clean up — restore to empty
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("")
            .query_async(&mut conn)
            .await;
    }

    #[tokio::test]
    async fn test_integration_persistence_analysis() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        let mut complexity = CustomerComplexity::new();
        analyze_persistence(&mut conn, &mut complexity).await;

        // Local Redis: save = "3600 1 300 100 60 10000" (RDB on), appendonly = no (AOF off)
        assert!(complexity.persistence_config.rdb_enabled, "Expected RDB enabled");
        assert!(!complexity.persistence_config.aof_enabled, "Expected AOF disabled");
        assert!(
            !complexity.persistence_config.rdb_save_params.is_empty(),
            "Expected non-empty RDB save params"
        );

        // Should produce exactly 1 Warning finding for RDB
        let persist_findings: Vec<_> = complexity
            .findings
            .iter()
            .filter(|f| f.category == "Persistence")
            .collect();
        assert_eq!(persist_findings.len(), 1, "Expected exactly 1 persistence finding (RDB)");
        assert_eq!(persist_findings[0].severity, Severity::Warning);
        assert_eq!(persist_findings[0].title, "RDB Snapshots Enabled");
        assert_eq!(complexity.total_score, 3, "Expected score 3 for 1 Warning");
    }

    #[tokio::test]
    async fn test_integration_memory_policy() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        let mut complexity = CustomerComplexity::new();
        analyze_memory_policy(&mut conn, &mut complexity).await;

        // Local Redis: maxmemory-policy = noeviction (standard policy)
        assert_eq!(
            complexity.max_memory_policy, "noeviction",
            "Expected noeviction, got: {}",
            complexity.max_memory_policy
        );

        // Should produce exactly 1 Info finding (noeviction is in the standard list)
        assert_eq!(complexity.findings.len(), 1, "Expected exactly 1 finding");
        assert_eq!(complexity.findings[0].severity, Severity::Info);
        assert_eq!(complexity.findings[0].category, "Configuration");
        assert!(
            complexity.findings[0].title.contains("noeviction"),
            "Expected 'noeviction' in title, got: {}",
            complexity.findings[0].title
        );
        assert_eq!(complexity.total_score, 1, "Expected score 1 for Info");
    }

    #[tokio::test]
    async fn test_integration_memory_policy_nonstandard() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        // Save original policy
        let orig: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory-policy")
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
        let orig_policy = orig.get(1).cloned().unwrap_or_else(|| "noeviction".to_string());

        // Set a non-standard policy
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("volatile-random")
            .query_async(&mut conn)
            .await;

        let mut complexity = CustomerComplexity::new();
        analyze_memory_policy(&mut conn, &mut complexity).await;

        assert_eq!(complexity.max_memory_policy, "volatile-random");
        assert_eq!(complexity.findings.len(), 1, "Expected exactly 1 finding");
        assert_eq!(
            complexity.findings[0].severity, Severity::Warning,
            "Non-standard policy should be a Warning"
        );
        assert_eq!(complexity.total_score, 3, "Expected score 3 for 1 Warning");

        // Restore original
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg(&orig_policy)
            .query_async(&mut conn)
            .await;
    }

    #[tokio::test]
    async fn test_integration_lua_scripts() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        // Run a simple eval to ensure there's at least one call in command stats
        let _: Result<i64, _> = redis::cmd("EVAL")
            .arg("return 1")
            .arg(0)
            .query_async(&mut conn)
            .await;

        let mut complexity = CustomerComplexity::new();
        analyze_lua_scripts(&mut conn, &mut complexity).await;

        // Should produce exactly 1 Warning finding for Lua usage
        assert_eq!(complexity.findings.len(), 1, "Expected exactly 1 Lua finding");
        assert_eq!(complexity.findings[0].severity, Severity::Warning);
        assert!(
            complexity.findings[0].title.contains("Lua Scripts Detected"),
            "Expected 'Lua Scripts Detected' in title, got: {}",
            complexity.findings[0].title
        );
        assert_eq!(complexity.total_score, 3, "Expected score 3 for 1 Warning");
    }

    #[tokio::test]
    async fn test_integration_pubsub_no_active_channels() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        let mut complexity = CustomerComplexity::new();
        analyze_pubsub(&mut conn, &mut complexity).await;

        // With no active subscribers, should produce exactly 0 findings and score 0
        assert_eq!(complexity.findings.len(), 0, "Expected 0 Pub/Sub findings");
        assert_eq!(complexity.total_score, 0, "Expected score 0 with no pub/sub findings");
    }

    #[tokio::test]
    async fn test_integration_complexity_json_output() {
        let _guard = REDIS_LOCK.lock().await;
        let Some(mut conn) = get_test_conn().await else {
            eprintln!("Skipping: Redis not available");
            return;
        };

        let metrics = fetch_database_metrics(&mut conn).await.unwrap();
        let complexity = analyze_customer_complexity(&mut conn, &metrics).await.unwrap();

        // Verify the full complexity result serializes to valid JSON
        let json_str = serde_json::to_string_pretty(&complexity)
            .expect("Failed to serialize complexity to JSON");

        // Verify required top-level fields are present
        assert!(json_str.contains("\"findings\""), "Missing 'findings' field");
        assert!(json_str.contains("\"total_score\""), "Missing 'total_score' field");
        assert!(json_str.contains("\"cluster_mode\""), "Missing 'cluster_mode' field");
        assert!(json_str.contains("\"acl_rules\""), "Missing 'acl_rules' field");
        assert!(json_str.contains("\"loaded_modules\""), "Missing 'loaded_modules' field");
        assert!(json_str.contains("\"persistence_config\""), "Missing 'persistence_config' field");
        assert!(json_str.contains("\"keyspace_notifications\""), "Missing 'keyspace_notifications' field");
        assert!(json_str.contains("\"max_memory_policy\""), "Missing 'max_memory_policy' field");

        // Verify it round-trips with exact field preservation
        let deserialized: CustomerComplexity = serde_json::from_str(&json_str)
            .expect("Failed to deserialize complexity from JSON");
        assert_eq!(deserialized.total_score, complexity.total_score);
        assert_eq!(deserialized.findings.len(), complexity.findings.len());
        assert_eq!(deserialized.cluster_mode, complexity.cluster_mode);
        assert_eq!(deserialized.acl_rules.len(), complexity.acl_rules.len());
        assert_eq!(deserialized.loaded_modules, complexity.loaded_modules);
        assert_eq!(deserialized.max_memory_policy, complexity.max_memory_policy);
        assert_eq!(deserialized.keyspace_notifications, complexity.keyspace_notifications);
        assert_eq!(
            deserialized.persistence_config.rdb_enabled,
            complexity.persistence_config.rdb_enabled
        );
        assert_eq!(
            deserialized.persistence_config.aof_enabled,
            complexity.persistence_config.aof_enabled
        );

        // Verify each finding round-trips
        for (i, (orig, deser)) in complexity.findings.iter().zip(deserialized.findings.iter()).enumerate() {
            assert_eq!(orig.category, deser.category, "Finding {} category mismatch", i);
            assert_eq!(orig.severity, deser.severity, "Finding {} severity mismatch", i);
            assert_eq!(orig.title, deser.title, "Finding {} title mismatch", i);
            assert_eq!(orig.detail, deser.detail, "Finding {} detail mismatch", i);
        }
    }
}
