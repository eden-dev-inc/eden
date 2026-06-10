use ai_workload::{LoadArgs, ServeArgs, load, serve};
use clap::{Parser, Subcommand};
use std::process;

#[derive(Parser)]
#[command(name = "ai-workload", about = "Synthetic LLM and agent gateway benchmark tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve an OpenAI-compatible synthetic backend.
    Serve(ServeArgs),
    /// Run an open-loop HTTP benchmark against Direct, Eden, or Envoy.
    Load(LoadArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Serve(args) => serve(args).await.map(|_| ()),
        Command::Load(args) => load(args).await.map(|result| {
            let json = serde_json::to_string_pretty(&result)
                .unwrap_or_else(|err| format!(r#"{{"error":"failed to serialize benchmark result","detail":"{err}"}}"#));
            println!("{json}");
        }),
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        process::exit(1);
    }
}
