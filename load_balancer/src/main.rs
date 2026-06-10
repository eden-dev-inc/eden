//! # Load Balancer
//!
//! Pingora-based load balancer for distributing traffic across Eve service instances.
//!
//! ## Overview
//!
//! This binary implements a high-performance HTTP/HTTPS load balancer using Cloudflare's
//! Pingora framework. It provides request distribution, health checking, and telemetry
//! for Eden service clusters.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  Client Requests                        │
//! │  HTTP (8080) / HTTPS (8443)             │
//! └────────────────┬────────────────────────┘
//!                  │
//!                  ▼
//! ┌─────────────────────────────────────────┐
//! │  Load Balancer (Pingora)                │
//! │  - TLS termination                      │
//! │  - Round-robin selection                │
//! │  - Request metrics                      │
//! └────────────────┬────────────────────────┘
//!                  │
//!       ┌──────────┼──────────┐
//!       ▼          ▼          ▼
//! ┌─────────┐ ┌─────────┐ ┌─────────┐
//! │ Eden    │ │ Eden    │ │ Eden    │
//! │ Node 1  │ │ Node 2  │ │ Node 3  │
//! └─────────┘ └─────────┘ └─────────┘
//! ```
//!
//! ## Configuration
//!
//! Set via environment variables:
//! - `LB_PORT` - HTTP port (default: 8080)
//! - `LB_HTTPS_PORT` - HTTPS port (default: 8443)
//! - `EDEN_URL` - Upstream Eden service URL
//! - `EDEN_SNI` - Server Name Indication for TLS
//! - `LB_OTLP_COLLECTOR` - OpenTelemetry endpoint for metrics
//!
//! ## Features
//!
//! ### Round-Robin Load Balancing
//! Distributes requests evenly across configured upstream servers using Pingora's
//! built-in round-robin selection algorithm.
//!
//! ### TLS Support
//! - Automatic TLS termination on HTTPS port
//! - HTTP/2 support enabled
//! - Certificate-based authentication (certs/server.crt, certs/key.pem)
//! - SNI routing
//!
//! ### Telemetry
//! Exports metrics via OpenTelemetry:
//! - Request count by endpoint
//! - Request latency distribution
//! - Error rates and types
//!
//! ## Usage
//!
//! ```bash
//! # Run load balancer with environment config
//! LB_PORT=8080 EDEN_URL=http://localhost:8000 cargo run --bin load-balancer
//! ```
//!
//! ## Request Flow
//!
//! 1. Client connects to load balancer (HTTP/HTTPS)
//! 2. `early_request_filter` - Record request start time
//! 3. `upstream_peer` - Select Eden node via round-robin
//! 4. `upstream_request_filter` - Add Host header
//! 5. Forward to selected upstream
//! 6. `logging` - Record metrics and latency

use pingora_core::Result;
use pingora_core::server::Server;
use pingora_core::server::configuration::Opt;
use pingora_core::services::background::background_service;
use pingora_load_balancing::LoadBalancer;

mod config;
mod load_balancer;
mod telemetry;

use load_balancer::{LB, Upstreams};
pub(crate) use telemetry::{LB_METRICS, LbMetrics};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for console output
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "debug".into()))
        .init();

    let config = config::Config::new()?;
    // read command line arguments
    // let opt = Opt::parse();
    let opt = Opt::default();
    let mut my_server = Server::new(Some(opt)).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([config.eden_url()])?;

    // We add health check in the background so that the bad server is never selected.
    // // let hc = health_check::TcpHealthCheck::new();
    // // upstreams.set_health_check(hc);
    // upstreams.health_check_frequency = Some(Duration::from_secs(1));

    let background = background_service("health check", upstreams);

    let upstreams = Upstreams {
        upstreams: background.task(),
        sni: config.eden_sni().to_owned(),
    };

    // CryptoProvider needs to be set before pingora_proxy::http_proxy_service call
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut lb = pingora_proxy::http_proxy_service(&my_server.configuration, LB::new(upstreams, &config));
    lb.add_tcp(&format!("0.0.0.0:{}", config.port()));

    let cert_path = format!("{}/certs/server.crt", env!("CARGO_MANIFEST_DIR"));
    let key_path = format!("{}/certs/key.pem", env!("CARGO_MANIFEST_DIR"));

    let mut tls_settings = pingora_core::listeners::tls::TlsSettings::intermediate(&cert_path, &key_path)?;
    tls_settings.enable_h2();
    lb.add_tls_with_settings(&format!("0.0.0.0:{}", config.https_port()), None, tls_settings);
    my_server.add_service(lb);
    // my_server.add_service(background);
    my_server.run_forever();
}
