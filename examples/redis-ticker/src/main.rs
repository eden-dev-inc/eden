/// A simple program that commects to a Redis database (via CLI command)
/// and writes ticks in two keys
/// On startup two keys are created / overwritten:
/// redis-ticker::timer - 0::<epoch_at_startup_in_seconds>
/// redis-ticker::counter - 0
/// Then ticker increases these values exactly every second:
/// - redis-ticker::timer - read the last value and adds n::<current_epoch>
/// - redis_ticker::cunter - INCR
///
/// In case internal counter doesn't match timer/counter, a warning is printed and
/// the values are fixed to contain proper values as epoch on startup in this script
/// is the source of truth.
///
/// Connect toa Redis DB using a subset of redis-cli options, i.e.
/// -h hostname (localhost by default)
/// -p port
/// --tls - use TLS
/// -a - password
///
/// This utility is useful to test Eden migrations on a live database
/// to demonstrate the data are copied and seamlessly used during migration.
use clap::Parser;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TIMER_KEY: &str = "redis-ticker::timer";
const COUNTER_KEY: &str = "redis-ticker::counter";

#[derive(Debug, Parser)]
#[command(
    name = "redis-ticker",
    about = "Simple Redis ticker using redis-cli",
    disable_help_flag = true
)]
struct RedisCliOpts {
    /// Redis hostname
    #[arg(short = 'h', long = "host", default_value = "localhost")]
    host: String,
    /// Redis port
    #[arg(short = 'p')]
    port: Option<String>,
    /// Enable TLS
    #[arg(long = "tls")]
    tls: bool,
    /// Redis password
    #[arg(short = 'a')]
    password: Option<String>,
    /// Show help
    #[arg(long = "help", action = clap::ArgAction::Help)]
    help: Option<bool>,
}

impl RedisCliOpts {
    fn base_args(&self) -> Vec<String> {
        let mut args = vec!["-h".to_string(), self.host.clone()];
        if let Some(port) = &self.port {
            args.push("-p".to_string());
            args.push(port.clone());
        }
        if self.tls {
            args.push("--tls".to_string());
        }
        if let Some(password) = &self.password {
            args.push("-a".to_string());
            args.push(password.clone());
        }
        args
    }
}

fn run_redis_cli(base_args: &[String], cmd_args: &[&str]) -> Result<String, String> {
    let output = Command::new("redis-cli")
        .args(base_args)
        .args(cmd_args)
        .output()
        .map_err(|err| format!("failed to run redis-cli: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "redis-cli failed (status {}): {}",
            output.status,
            stderr.trim()
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn parse_timer_value(value: &str) -> Result<u64, String> {
    let mut parts = value.split("::");
    let index = parts
        .next()
        .ok_or_else(|| "timer segment missing index".to_string())?
        .parse::<u64>()
        .map_err(|_| "timer segment index is not a number".to_string())?;
    if parts.next().is_none() {
        return Err("timer segment missing epoch".to_string());
    }
    Ok(index)
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

fn main() {
    let opts = RedisCliOpts::parse();

    let base_args = opts.base_args();
    let startup_epoch = epoch_seconds();
    let timer_start = format!("0::{startup_epoch}");

    if let Err(err) = run_redis_cli(&base_args, &["SET", TIMER_KEY, &timer_start]) {
        eprintln!("{err}");
        return;
    }
    if let Err(err) = run_redis_cli(&base_args, &["SET", COUNTER_KEY, "0"]) {
        eprintln!("{err}");
        return;
    }

    let start = Instant::now();
    let mut local_counter: u64 = 0;

    loop {
        let next_tick = start + Duration::from_secs(local_counter + 1);
        let now = Instant::now();
        if next_tick > now {
            std::thread::sleep(next_tick - now);
        }

        let timer_value = match run_redis_cli(&base_args, &["GET", TIMER_KEY]) {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => {
                eprintln!("timer key missing; resetting to startup epoch");
                let _ = run_redis_cli(&base_args, &["SET", TIMER_KEY, &timer_start]);
                let _ = run_redis_cli(&base_args, &["SET", COUNTER_KEY, "0"]);
                local_counter = 0;
                continue;
            }
            Err(err) => {
                eprintln!("{err}");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        let counter_value = match run_redis_cli(&base_args, &["GET", COUNTER_KEY]) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("{err}");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        let redis_counter = match counter_value.parse::<u64>() {
            Ok(value) => value,
            Err(_) => {
                eprintln!("counter key not numeric; resetting to startup epoch");
                let _ = run_redis_cli(&base_args, &["SET", TIMER_KEY, &timer_start]);
                let _ = run_redis_cli(&base_args, &["SET", COUNTER_KEY, "0"]);
                local_counter = 0;
                continue;
            }
        };

        let timer_index = match parse_timer_value(&timer_value) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("{err}; resetting to startup epoch");
                let _ = run_redis_cli(&base_args, &["SET", TIMER_KEY, &timer_start]);
                let _ = run_redis_cli(&base_args, &["SET", COUNTER_KEY, "0"]);
                local_counter = 0;
                continue;
            }
        };

        if redis_counter != timer_index {
            eprintln!(
                "counter/timer mismatch (counter={redis_counter}, timer_index={timer_index}); resetting"
            );
            let _ = run_redis_cli(&base_args, &["SET", TIMER_KEY, &timer_start]);
            let _ = run_redis_cli(&base_args, &["SET", COUNTER_KEY, "0"]);
            local_counter = 0;
            continue;
        }

        let now_epoch = epoch_seconds();
        let incr_value = match run_redis_cli(&base_args, &["INCR", COUNTER_KEY]) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("{err}");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        let new_counter = match incr_value.parse::<u64>() {
            Ok(value) => value,
            Err(_) => {
                eprintln!("INCR returned non-numeric value; resetting");
                let _ = run_redis_cli(&base_args, &["SET", TIMER_KEY, &timer_start]);
                let _ = run_redis_cli(&base_args, &["SET", COUNTER_KEY, "0"]);
                local_counter = 0;
                continue;
            }
        };

        let updated_timer = format!("{new_counter}::{now_epoch}");
        if let Err(err) = run_redis_cli(&base_args, &["SET", TIMER_KEY, &updated_timer]) {
            eprintln!("{err}");
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        local_counter = new_counter;
    }
}
