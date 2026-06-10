//! Alert service - main polling loop and dispatcher.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::AlertsConfig;
use crate::notify::{BackendConfig, Deduplicator, NotificationBackend, NotifyError, RateLimiter, SlackBackend, WebhookBackend};
use crate::provider::{AlertSnapshot, AnalyticsProvider, ProviderError, TimeWindow};
use crate::rules::{PendingAlert, RulesEngine};

/// Alert service error.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("provider error: {0}")]
    Provider(#[from] ProviderError),
    #[error("notification error: {0}")]
    Notify(#[from] NotifyError),
    #[error("configuration error: {0}")]
    Config(String),
}

/// Alert service that polls ClickHouse and dispatches notifications.
pub struct AlertService<P: AnalyticsProvider> {
    provider: Arc<P>,
    backends: Vec<Arc<dyn NotificationBackend>>,
    rules_engine: RulesEngine,
    rate_limiter: Arc<RateLimiter>,
    deduplicator: Arc<Deduplicator>,
    poll_interval: Duration,
    window_minutes: i64,
}

impl<P: AnalyticsProvider + 'static> AlertService<P> {
    /// Create a new alert service from configuration.
    pub fn new(provider: P, config: AlertsConfig) -> Result<Self, ServiceError> {
        // Validate configuration
        config.notify.validate().map_err(ServiceError::Notify)?;
        config.rules.validate().map_err(ServiceError::Config)?;

        // Build notification backends
        let backends = build_backends(&config)?;

        // Build rate limiter and deduplicator
        let rate_limiter = Arc::new(RateLimiter::new(config.notify.rate_limit.window(), config.notify.rate_limit.max_per_window));
        let deduplicator = Arc::new(Deduplicator::new(config.notify.dedup.window()));

        // Build rules engine
        let rules_engine = RulesEngine::new(config.rules);

        Ok(Self {
            provider: Arc::new(provider),
            backends,
            rules_engine,
            rate_limiter,
            deduplicator,
            poll_interval: Duration::from_secs(config.poll_interval_secs),
            window_minutes: config.window_minutes,
        })
    }

    /// Run the service until shutdown signal is received.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        info!(
            poll_interval_secs = self.poll_interval.as_secs(),
            window_minutes = self.window_minutes,
            backends = self.backends.len(),
            "starting alert service"
        );

        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = self.process_once().await {
                        error!(?err, "alert processing error");
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("shutdown signal received, stopping alert service");
                        break;
                    }
                }
            }
        }
    }

    /// Process a single polling cycle.
    async fn process_once(&self) -> Result<(), ServiceError> {
        debug!("fetching analytics data");

        let window = TimeWindow::last_minutes(self.window_minutes);

        // Fetch data from ClickHouse
        let health = self.provider.fetch_endpoint_health(&window).await?;
        let anti_patterns = self.provider.fetch_anti_patterns(&window).await?;
        let signals = self.provider.fetch_signals(&window).await?;

        debug!(
            endpoints = health.len(),
            anti_patterns = anti_patterns.len(),
            signals = signals.len(),
            "fetched analytics data"
        );

        // Build snapshot for rule evaluation
        let snapshot = AlertSnapshot::new(self.window_minutes).with_health(health).with_anti_patterns(anti_patterns).with_signals(signals);

        // Evaluate rules
        let alerts = self.rules_engine.evaluate(&snapshot);
        debug!(pending_alerts = alerts.len(), "evaluated rules");

        // Dispatch alerts
        for alert in alerts {
            self.dispatch(alert).await?;
        }

        Ok(())
    }

    /// Dispatch a single alert to all backends.
    async fn dispatch(&self, alert: PendingAlert) -> Result<(), ServiceError> {
        let now = Instant::now();

        // Check deduplication
        if let Some(key) = &alert.notification.dedup_key
            && !self.deduplicator.allow_at(key, now)
        {
            debug!(dedup_key = %key, "notification deduped");
            return Ok(());
        }

        // Check rate limit
        if !self.rate_limiter.allow_at(now) {
            warn!("notification rate limited");
            return Ok(());
        }

        // Send to all backends
        for backend in &self.backends {
            match backend.send(&alert.notification).await {
                Ok(()) => {
                    info!(
                        backend = backend.name(),
                        rule_key = %alert.rule_key,
                        "notification sent"
                    );
                }
                Err(err) => {
                    error!(backend = backend.name(), ?err, "notification backend error");
                }
            }
        }

        Ok(())
    }
}

fn build_backends(config: &AlertsConfig) -> Result<Vec<Arc<dyn NotificationBackend>>, ServiceError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| ServiceError::Config(format!("failed to build HTTP client: {}", e)))?;

    let mut backends: Vec<Arc<dyn NotificationBackend>> = Vec::new();

    for backend_config in &config.notify.backends {
        match backend_config {
            BackendConfig::Slack(slack) => {
                backends.push(Arc::new(SlackBackend::new(client.clone(), slack.clone())));
            }
            BackendConfig::Webhook(webhook) => {
                backends.push(Arc::new(WebhookBackend::new(client.clone(), webhook.clone())));
            }
        }
    }

    if backends.is_empty() {
        warn!("no notification backends configured");
    }

    Ok(backends)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::NotifyConfig;
    use crate::provider::{EndpointHealth, HotKeyRow, HourlyRollup, SignalRow};
    use crate::rules::AlertRulesConfig;
    use async_trait::async_trait;

    struct MockProvider;

    #[async_trait]
    impl AnalyticsProvider for MockProvider {
        async fn fetch_endpoint_health(&self, _window: &TimeWindow) -> Result<Vec<EndpointHealth>, ProviderError> {
            Ok(vec![])
        }

        async fn fetch_anti_patterns(&self, _window: &TimeWindow) -> Result<Vec<crate::provider::AntiPatternRow>, ProviderError> {
            Ok(vec![])
        }

        async fn fetch_hourly_rollups(&self, _window: &TimeWindow) -> Result<Vec<HourlyRollup>, ProviderError> {
            Ok(vec![])
        }

        async fn fetch_signals(&self, _window: &TimeWindow) -> Result<Vec<SignalRow>, ProviderError> {
            Ok(vec![])
        }

        async fn fetch_hot_keys(&self, _window: &TimeWindow, _min_hits: u64) -> Result<Vec<HotKeyRow>, ProviderError> {
            Ok(vec![])
        }

        async fn fetch_error_spikes(
            &self,
            _window: &TimeWindow,
            _min_errors: u64,
        ) -> Result<Vec<crate::provider::ErrorSpikeRow>, ProviderError> {
            Ok(vec![])
        }
    }

    #[test]
    fn test_service_creation() {
        let config = AlertsConfig {
            poll_interval_secs: 30,
            window_minutes: 5,
            notify: NotifyConfig::default(),
            rules: AlertRulesConfig::default(),
            clickhouse: Default::default(),
        };

        let service = AlertService::new(MockProvider, config);
        assert!(service.is_ok());
    }
}
