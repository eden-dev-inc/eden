use analytics_server::{run_runtime_validation, ValidationOptions};
use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(name = "runtime-validator")]
#[clap(about = "Live runtime validator for analytics-server and traffic-client")]
struct ValidatorConfig {
    #[clap(long, env = "SERVER_BASE_URL", default_value = "http://127.0.0.1:3000")]
    server_base_url: String,

    #[clap(long, env = "CLIENT_BASE_URL")]
    client_base_url: Option<String>,

    #[clap(long, env = "REQUEST_TIMEOUT_MS", default_value = "5000")]
    request_timeout_ms: u64,

    #[clap(
        long,
        env = "REQUIRE_POSTGRES",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    require_postgres: bool,

    #[clap(
        long,
        env = "REQUIRE_REDIS",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    require_redis: bool,

    #[clap(
        long,
        env = "EXERCISE_CLIENT_CONFIG_PATCH",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    exercise_client_config_patch: bool,
}

fn parse_bool(value: &str) -> Result<bool, String> {
    value
        .parse::<bool>()
        .map_err(|error| format!("expected true or false, got '{value}': {error}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ValidatorConfig::parse();
    let report = run_runtime_validation(&ValidationOptions {
        server_base_url: config.server_base_url,
        client_base_url: config.client_base_url,
        request_timeout_ms: config.request_timeout_ms,
        require_postgres: config.require_postgres,
        require_redis: config.require_redis,
        exercise_client_config_patch: config.exercise_client_config_patch,
    })
    .await?;

    for step in &report.steps {
        println!("[{}] {}: {}", step.status, step.name, step.detail);
    }

    println!(
        "\nRuntime validation passed in {} ms.",
        (report.completed_at - report.started_at).num_milliseconds()
    );
    Ok(())
}
