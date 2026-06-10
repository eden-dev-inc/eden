//! Rules evaluation engine.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::provider::{AlertSnapshot, AntiPatternRow, EndpointHealth};

use super::config::{AlertRulesConfig, AntiPatternRule, ReportRule, ThresholdMetric, ThresholdRule};
use super::types::{AlertContext, PendingAlert};

/// Rules engine for evaluating alert conditions.
pub struct RulesEngine {
    config: AlertRulesConfig,
    /// Last time each rule fired (for cooldown tracking).
    last_fired: Mutex<HashMap<String, Instant>>,
    /// Maximum window to retain cooldown state.
    max_window: Duration,
}

impl RulesEngine {
    /// Create a new rules engine with the given configuration.
    pub fn new(config: AlertRulesConfig) -> Self {
        let max_window = config.max_window();
        Self { config, last_fired: Mutex::new(HashMap::new()), max_window }
    }

    /// Reload rules configuration.
    pub fn reload(&mut self, config: AlertRulesConfig) {
        self.max_window = config.max_window();
        self.config = config;
    }

    /// Evaluate all rules against the given snapshot.
    ///
    /// Returns a list of pending alerts that should be dispatched.
    pub fn evaluate(&self, snapshot: &AlertSnapshot) -> Vec<PendingAlert> {
        let now = Instant::now();
        self.prune_cooldowns(now);

        let mut alerts = Vec::new();

        // Evaluate threshold rules
        for rule in &self.config.thresholds {
            alerts.extend(self.evaluate_threshold(rule, &snapshot.endpoint_health, now));
        }

        // Evaluate anti-pattern rules
        for rule in &self.config.anti_patterns {
            alerts.extend(self.evaluate_anti_pattern(rule, &snapshot.anti_patterns, now));
        }

        // Evaluate report rules
        for rule in &self.config.reports {
            if let Some(alert) = self.evaluate_report(rule, snapshot, now) {
                alerts.push(alert);
            }
        }

        alerts
    }

    fn evaluate_threshold(&self, rule: &ThresholdRule, health: &[EndpointHealth], now: Instant) -> Vec<PendingAlert> {
        let mut alerts = Vec::new();

        for endpoint in health {
            if endpoint.requests < rule.min_requests {
                continue;
            }

            let value = extract_metric(rule.metric, endpoint);
            if !rule.operator.compare(value, rule.threshold) {
                continue;
            }

            let context = AlertContext::new()
                .with_tenant(&endpoint.organization_uuid)
                .with_endpoint(&endpoint.endpoint_uuid)
                .with_protocol(&endpoint.protocol);

            let rule_key = context.dedup_key("threshold", &rule.id);

            if !self.cooldown_ready(&rule_key, rule.cooldown(), now) {
                continue;
            }

            let alert = PendingAlert::threshold(&rule.id, rule.metric.as_ref(), value, rule.threshold, context);

            self.record_fired(&rule_key, now);
            alerts.push(alert);
        }

        alerts
    }

    fn evaluate_anti_pattern(&self, rule: &AntiPatternRule, patterns: &[AntiPatternRow], now: Instant) -> Vec<PendingAlert> {
        let mut alerts = Vec::new();

        for pattern in patterns {
            if pattern.occurrence_count < rule.min_occurrences {
                continue;
            }

            if !rule.matches_type(&pattern.pattern_type) {
                continue;
            }

            let context = AlertContext::new().with_tenant(&pattern.organization_uuid).with_endpoint(&pattern.endpoint_uuid);

            let rule_key = context.dedup_key("anti_pattern", &format!("{}:{}", rule.id, pattern.pattern_type));

            if !self.cooldown_ready(&rule_key, rule.cooldown(), now) {
                continue;
            }

            let alert = PendingAlert::anti_pattern(
                &rule.id,
                &pattern.pattern_type,
                pattern.occurrence_count,
                pattern.sample_details.as_deref(),
                context,
            );

            self.record_fired(&rule_key, now);
            alerts.push(alert);
        }

        alerts
    }

    fn evaluate_report(&self, rule: &ReportRule, snapshot: &AlertSnapshot, now: Instant) -> Option<PendingAlert> {
        let rule_key = format!("report:{}", rule.id);

        if !self.cooldown_ready(&rule_key, rule.interval(), now) {
            return None;
        }

        let body = build_summary_body(snapshot, rule);
        let alert = PendingAlert::summary(&rule.id, body);

        self.record_fired(&rule_key, now);
        Some(alert)
    }

    fn cooldown_ready(&self, rule_key: &str, cooldown: Duration, now: Instant) -> bool {
        let last_fired = self.last_fired.lock();
        match last_fired.get(rule_key) {
            Some(last) => now.duration_since(*last) >= cooldown,
            None => true,
        }
    }

    fn record_fired(&self, rule_key: &str, now: Instant) {
        self.last_fired.lock().insert(rule_key.to_string(), now);
    }

    fn prune_cooldowns(&self, now: Instant) {
        if self.max_window.is_zero() {
            return;
        }

        let mut last_fired = self.last_fired.lock();
        last_fired.retain(|_, last| now.saturating_duration_since(*last) <= self.max_window);
    }
}

fn extract_metric(metric: ThresholdMetric, health: &EndpointHealth) -> f64 {
    match metric {
        ThresholdMetric::ErrorRate => health.error_rate,
        ThresholdMetric::SlowRate => health.slow_rate,
        ThresholdMetric::AvgLatencyUs => health.avg_latency_us,
        ThresholdMetric::P95LatencyUs => health.p95_latency_us,
        ThresholdMetric::MaxLatencyUs => health.max_latency_us as f64,
        ThresholdMetric::RequestCount => health.requests as f64,
    }
}

fn build_summary_body(snapshot: &AlertSnapshot, rule: &ReportRule) -> String {
    let total_requests: u64 = snapshot.endpoint_health.iter().map(|h| h.requests).sum();
    let total_errors: u64 = snapshot.endpoint_health.iter().map(|h| h.errors).sum();
    let total_slow: u64 = snapshot.endpoint_health.iter().map(|h| h.slow_queries).sum();

    let error_rate = if total_requests > 0 {
        total_errors as f64 / total_requests as f64
    } else {
        0.0
    };
    let slow_rate = if total_requests > 0 {
        total_slow as f64 / total_requests as f64
    } else {
        0.0
    };

    let mut body = format!(
        "window={}min total_requests={} errors={} slow_queries={} error_rate={:.2}% slow_rate={:.2}%",
        snapshot.window_minutes,
        total_requests,
        total_errors,
        total_slow,
        error_rate * 100.0,
        slow_rate * 100.0
    );

    // Top endpoints by errors
    let mut top_endpoints = snapshot.endpoint_health.clone();
    top_endpoints.sort_by_key(|h| std::cmp::Reverse(h.errors));
    let top = top_endpoints.into_iter().take(rule.top_n).collect::<Vec<_>>();

    if !top.is_empty() {
        body.push_str("\nTop endpoints by errors:");
        for entry in top {
            body.push_str(&format!(
                "\n- endpoint={} protocol={} errors={} requests={}",
                entry.endpoint_uuid, entry.protocol, entry.errors, entry.requests
            ));
        }
    }

    // Top anti-patterns
    let mut top_patterns = snapshot.anti_patterns.clone();
    top_patterns.sort_by_key(|p| std::cmp::Reverse(p.occurrence_count));
    let top_patterns = top_patterns.into_iter().take(rule.top_n).collect::<Vec<_>>();

    if !top_patterns.is_empty() {
        body.push_str("\nTop anti-patterns:");
        for pattern in top_patterns {
            body.push_str(&format!(
                "\n- type={} occurrences={} endpoint={}",
                pattern.pattern_type, pattern.occurrence_count, pattern.endpoint_uuid
            ));
        }
    }

    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::config::{ThresholdOperator, ThresholdRule};

    fn sample_health() -> Vec<EndpointHealth> {
        vec![EndpointHealth {
            organization_uuid: "tenant-1".to_string(),
            endpoint_uuid: "endpoint-1".to_string(),
            protocol: "redis".to_string(),
            requests: 1000,
            errors: 100,
            slow_queries: 50,
            error_rate: 0.10,
            slow_rate: 0.05,
            avg_latency_us: 500.0,
            p95_latency_us: 1000.0,
            max_latency_us: 5000,
        }]
    }

    #[test]
    fn test_threshold_rule_triggers() {
        let config = AlertRulesConfig {
            thresholds: vec![ThresholdRule {
                id: "high_error_rate".to_string(),
                metric: ThresholdMetric::ErrorRate,
                operator: ThresholdOperator::GreaterThan,
                threshold: 0.05, // 5%
                min_requests: 100,
                cooldown_secs: 0,
                description: None,
            }],
            ..Default::default()
        };

        let engine = RulesEngine::new(config);
        let snapshot = AlertSnapshot::new(5).with_health(sample_health());

        let alerts = engine.evaluate(&snapshot);
        assert_eq!(alerts.len(), 1);
        assert!(alerts[0].rule_key.contains("high_error_rate"));
    }

    #[test]
    fn test_threshold_rule_min_requests() {
        let config = AlertRulesConfig {
            thresholds: vec![ThresholdRule {
                id: "test".to_string(),
                metric: ThresholdMetric::ErrorRate,
                operator: ThresholdOperator::GreaterThan,
                threshold: 0.05,
                min_requests: 10000, // Higher than sample
                cooldown_secs: 0,
                description: None,
            }],
            ..Default::default()
        };

        let engine = RulesEngine::new(config);
        let snapshot = AlertSnapshot::new(5).with_health(sample_health());

        let alerts = engine.evaluate(&snapshot);
        assert!(alerts.is_empty()); // Should not trigger due to min_requests
    }

    #[test]
    fn test_cooldown_prevents_duplicate() {
        let config = AlertRulesConfig {
            thresholds: vec![ThresholdRule {
                id: "test".to_string(),
                metric: ThresholdMetric::ErrorRate,
                operator: ThresholdOperator::GreaterThan,
                threshold: 0.05,
                min_requests: 100,
                cooldown_secs: 300, // 5 minute cooldown
                description: None,
            }],
            ..Default::default()
        };

        let engine = RulesEngine::new(config);
        let snapshot = AlertSnapshot::new(5).with_health(sample_health());

        // First evaluation should trigger
        let alerts1 = engine.evaluate(&snapshot);
        assert_eq!(alerts1.len(), 1);

        // Second evaluation should be blocked by cooldown
        let alerts2 = engine.evaluate(&snapshot);
        assert!(alerts2.is_empty());
    }
}
