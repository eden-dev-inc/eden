use std::fs;
use std::process;

use clap::{Parser, Subcommand};

use cacophony::backend::{RespBackendConfig, serve_resp_backend};
use cacophony::scenario::Scenario;

#[derive(Parser)]
#[command(name = "cacophony", about = "Open-loop Redis proxy load generator")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to the scenario TOML file.
    #[arg(long)]
    scenario: Option<String>,

    /// Target address (host:port) of the Redis-compatible server or proxy.
    #[arg(long, default_value = "localhost:6379")]
    target: String,

    /// Number of parallel load-generator shards to run per phase.
    #[arg(long, default_value_t = 1)]
    loadgen_shards: usize,
}

#[derive(Subcommand)]
enum Command {
    /// Serve a synthetic Redis-compatible RESP backend.
    ServeResp {
        /// Listen address for the synthetic backend.
        #[arg(long, default_value = "127.0.0.1:16379")]
        listen: String,

        /// Bulk-string payload size returned for every GET.
        #[arg(long, default_value_t = 65_536)]
        payload_size: usize,

        /// Repeated payload byte returned for every GET.
        #[arg(long, default_value_t = b'x')]
        payload_byte: u8,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Some(Command::ServeResp { listen, payload_size, payload_byte }) = cli.command {
        if let Err(e) = serve_resp_backend(RespBackendConfig { listen, payload_size, payload_byte }).await {
            eprintln!("error: {e}");
            process::exit(1);
        }
        return;
    }

    let scenario_path = match cli.scenario {
        Some(path) => path,
        None => {
            eprintln!("error: --scenario is required unless a subcommand is used");
            process::exit(1);
        }
    };

    let toml_str = match fs::read_to_string(&scenario_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to read scenario file '{scenario_path}': {e}");
            process::exit(1);
        }
    };

    let scenario: Scenario = match toml::from_str(&toml_str) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to parse scenario file '{scenario_path}': {e}");
            process::exit(1);
        }
    };

    if cli.loadgen_shards == 0 {
        eprintln!("error: --loadgen-shards must be > 0");
        process::exit(1);
    }

    eprintln!(
        "cacophony: scenario='{}' target={} loadgen_shards={}",
        scenario.meta.name, cli.target, cli.loadgen_shards
    );

    match cacophony::run_scenario_with_shards(&scenario, &cli.target, cli.loadgen_shards).await {
        Ok(result) => {
            let json = serde_json::to_string_pretty(&result).expect("JSON serialization");
            println!("{json}");
        }
        Err(e) => {
            eprintln!("error: {e}");
            process::exit(1);
        }
    }
}
