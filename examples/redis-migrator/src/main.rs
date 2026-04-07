mod client;
mod observer;
mod observer_tui;
mod populate;
mod setup;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(name = "redis-migrator", version = "0.1.0")]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Set up Eden organization, endpoints, interlay, and migration
    Setup(setup::SetupConfig),
    /// Populate a Redis database with configurable data
    Populate(populate::PopulateConfig),
    /// Run random read/write queries against Redis
    Client(client::ClientConfig),
    /// Run the Redis observer TUI
    Observe(observer_tui::ObserveConfig),
    /// Launch the observer TUI in a new Terminal window, then run the client workload
    ObserveClient(observer::ObserveClientConfig),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if present (won't override existing env vars)
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Commands::Setup(config) => setup::run(config).await,
        Commands::Populate(config) => {
            let result = populate::run(config).await?;

            if let Some(write_pct) = result.then_client_write_pct {
                println!();
                println!("Starting client automatically...");
                println!();

                let client_config = client::ClientConfig {
                    url: result.url,
                    prefix: result.prefix,
                    num_keys: result.num_keys,
                    write_pct,
                    value_size: result.key_size as usize,
                    concurrency: result.client_concurrency,
                    duration: result.client_duration,
                    report_interval: 5,
                };

                client::run(client_config).await?;
            }

            Ok(())
        }
        Commands::Client(config) => client::run(config).await,
        Commands::Observe(config) => {
            observer_tui::run(config)?;
            Ok(())
        }
        Commands::ObserveClient(config) => observer::run_observe_client(config).await,
    }
}
