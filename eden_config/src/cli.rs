//! CLI argument definitions for Eden MDBS.
//!
//! Parsed via `clap` and merged as the highest-priority configuration source.

use clap::Parser;
use std::path::PathBuf;

/// Eden Database Management Service CLI arguments.
#[derive(Parser, Debug, Clone)]
#[command(name = "eden", about = "Eden Database Management Service", version)]
pub struct CliArgs {
    /// Path to TOML config file
    #[arg(long, short, default_value = "eden.toml")]
    pub config: PathBuf,

    /// Override service port
    #[arg(long)]
    pub port: Option<u16>,

    /// Override log level (trace, debug, info, warn, error)
    #[arg(long)]
    pub log_level: Option<String>,

    /// Override OTLP collector endpoint
    #[arg(long)]
    pub otlp_collector: Option<String>,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("eden.toml"),
            port: None,
            log_level: None,
            otlp_collector: None,
        }
    }
}
