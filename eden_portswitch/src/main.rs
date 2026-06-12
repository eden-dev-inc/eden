mod iptables;

use clap::{Parser, Subcommand};
use iptables::{IptablesManager, RedirectRule};
use log::{error, info};

#[derive(Parser)]
#[command(name = "eden-portswitch")]
#[command(about = "Linux port redirection for Eden database migrations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a port redirect rule
    Add {
        /// Source port (the port your app connects to)
        #[arg(short, long)]
        from: u16,

        /// Destination port (Eden gateway port)
        #[arg(short, long)]
        to: u16,

        /// Exclude traffic from this user (prevents redirect loops for Eden)
        #[arg(short, long)]
        exclude_user: Option<String>,

        /// Protocol (tcp or udp, default: tcp)
        #[arg(short, long, default_value = "tcp")]
        protocol: String,
    },

    /// Remove a port redirect rule
    Remove {
        /// Source port to stop redirecting
        #[arg(short, long)]
        from: u16,

        /// Destination port (must match the original rule)
        #[arg(short, long)]
        to: u16,

        /// Excluded user (must match the original rule)
        #[arg(short, long)]
        exclude_user: Option<String>,

        /// Protocol (tcp or udp, default: tcp)
        #[arg(short, long, default_value = "tcp")]
        protocol: String,
    },

    /// List active redirect rules
    List,

    /// Remove all Eden redirect rules
    Clear,

    /// Show the commands that would be run without executing them
    DryRun {
        /// Source port
        #[arg(short, long)]
        from: u16,

        /// Destination port
        #[arg(short, long)]
        to: u16,

        /// Exclude traffic from this user
        #[arg(short, long)]
        exclude_user: Option<String>,

        /// Protocol (tcp or udp, default: tcp)
        #[arg(short, long, default_value = "tcp")]
        protocol: String,
    },

    /// Validate environment before applying rules
    Preflight {
        /// Port that Eden gateway is listening on
        #[arg(long)]
        eden_port: u16,

        /// Port that Redis is listening on
        #[arg(long)]
        redis_port: u16,

        /// User that Eden runs as
        #[arg(long)]
        eden_user: Option<String>,
    },
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let manager = IptablesManager::new();

    let result = match cli.command {
        Commands::Add { from, to, exclude_user, protocol } => {
            let rule = RedirectRule::new(from, to, exclude_user, protocol);
            info!("Adding redirect: {} -> {} (exclude: {:?})", rule.from_port, rule.to_port, rule.exclude_uid);
            manager.add(&rule)
        }

        Commands::Remove { from, to, exclude_user, protocol } => {
            let rule = RedirectRule::new(from, to, exclude_user, protocol);
            info!("Removing redirect: {} -> {}", rule.from_port, rule.to_port);
            manager.remove(&rule)
        }

        Commands::List => manager.list(),

        Commands::Clear => {
            info!("Clearing all Eden redirect rules");
            manager.clear()
        }

        Commands::DryRun { from, to, exclude_user, protocol } => {
            let rule = RedirectRule::new(from, to, exclude_user, protocol);
            manager.dry_run(&rule)
        }

        Commands::Preflight { eden_port, redis_port, eden_user } => manager.preflight(eden_port, redis_port, eden_user),
    };

    if let Err(e) = result {
        error!("Operation failed: {}", e);
        std::process::exit(1);
    }
}
