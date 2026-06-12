#![cfg_attr(test, allow(clippy::unwrap_used))]
//! Eden Alerts - Standalone alerting service for Eden analytics.
//!
//! This service watches ClickHouse analytics tables and dispatches notifications
//! when alert conditions are met. It provides a clean separation between data
//! ingestion (handled by the switchproxy) and alerting (handled by this service).
//!
//! # Architecture
//!
//! ```text
//! ClickHouse --> Provider --> RulesEngine --> Dispatcher --> Backends (Slack, Webhook)
//! ```
//!
//! # Features
//!
//! - **Threshold Alerts**: Trigger on metrics like error rate, latency, request count
//! - **Anti-Pattern Detection**: Alert on hot keys, N+1 queries, etc.
//! - **Periodic Reports**: Scheduled summary notifications
//! - **Rate Limiting**: Prevent notification storms
//! - **Deduplication**: Avoid duplicate alerts within a time window
//! - **Multiple Backends**: Slack, generic webhooks
//!
//! See `config.example.toml` for configuration options.

pub mod config;
pub mod notify;
pub mod provider;
pub mod rules;
pub mod service;

pub use config::AlertsConfig;
pub use service::AlertService;
