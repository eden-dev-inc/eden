//! Notification system for analytics alerts.
//!
//! This module provides notification backends (Slack, Webhook), rate limiting,
//! and deduplication for alert dispatch.

mod backends;
mod config;
mod limiter;

pub use backends::{NotificationBackend, SlackBackend, WebhookBackend};
pub use config::{BackendConfig, DedupConfig, NotifyConfig, RateLimitConfig, SlackConfig, WebhookConfig, WebhookHeader};
pub use limiter::{Deduplicator, RateLimiter};

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;

/// Notification kind emitted by the alerting service.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationKind {
    /// Threshold-based alert (error rate, latency, etc.)
    ThresholdAlert {
        rule_id: String,
        metric: String,
        value: f64,
        threshold: f64,
    },
    /// Anti-pattern detection alert (hot keys, N+1, etc.)
    AntiPattern {
        rule_id: String,
        pattern_type: String,
        occurrence_count: u64,
    },
    /// Periodic summary report
    PeriodicSummary { rule_id: String },
    /// Error spike alert
    ErrorSpike {
        endpoint_uuid: String,
        error_count: u64,
        window_minutes: u64,
    },
    /// Slow query alert
    SlowQuery {
        endpoint_uuid: String,
        latency_us: u64,
        query_pattern: String,
    },
}

/// Notification payload dispatched to backends.
#[derive(Debug, Clone, Serialize)]
pub struct Notification {
    pub kind: NotificationKind,
    pub title: String,
    pub body: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
}

impl Notification {
    pub fn new(kind: NotificationKind, title: String, body: String) -> Self {
        Self {
            kind,
            title,
            body,
            timestamp: Utc::now(),
            labels: HashMap::new(),
            dedup_key: None,
        }
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    pub fn with_dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }

    /// Format notification as plain text for Slack.
    pub fn plain_text(&self) -> String {
        format!("{}\n{}", self.title, self.body)
    }
}

/// Notification error type.
#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("config error: {0}")]
    Config(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("backend error: {0}")]
    Backend(String),
}

impl From<reqwest::Error> for NotifyError {
    fn from(err: reqwest::Error) -> Self {
        NotifyError::Transport(err.to_string())
    }
}
