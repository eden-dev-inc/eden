//! Alert evaluation types.

use crate::notify::{Notification, NotificationKind};

/// Context for alert evaluation (labels, metadata).
#[derive(Debug, Clone, Default)]
pub struct AlertContext {
    pub organization_uuid: Option<String>,
    pub endpoint_uuid: Option<String>,
    pub protocol: Option<String>,
}

impl AlertContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tenant(mut self, organization_uuid: impl Into<String>) -> Self {
        self.organization_uuid = Some(organization_uuid.into());
        self
    }

    pub fn with_endpoint(mut self, endpoint_uuid: impl Into<String>) -> Self {
        self.endpoint_uuid = Some(endpoint_uuid.into());
        self
    }

    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }

    /// Generate a dedup key for this context and rule.
    pub fn dedup_key(&self, rule_type: &str, rule_id: &str) -> String {
        let parts: Vec<&str> = [
            Some(rule_type),
            Some(rule_id),
            self.organization_uuid.as_deref(),
            self.endpoint_uuid.as_deref(),
            self.protocol.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect();

        parts.join(":")
    }
}

/// Result of evaluating a single rule against data.
#[derive(Debug, Clone)]
pub enum EvaluationResult {
    /// Rule did not trigger.
    NoAlert,
    /// Rule triggered, alert should be sent.
    Alert(Box<PendingAlert>),
    /// Rule is in cooldown.
    Cooldown { rule_id: String },
}

/// A pending alert ready for dispatch.
#[derive(Debug, Clone)]
pub struct PendingAlert {
    pub notification: Notification,
    pub rule_key: String,
    pub context: AlertContext,
}

impl PendingAlert {
    pub fn new(kind: NotificationKind, title: String, body: String, rule_key: String, context: AlertContext) -> Self {
        let mut notification = Notification::new(kind, title, body);

        // Add labels from context
        if let Some(tenant) = &context.organization_uuid {
            notification.labels.insert("organization_uuid".into(), tenant.clone());
        }
        if let Some(endpoint) = &context.endpoint_uuid {
            notification.labels.insert("endpoint_uuid".into(), endpoint.clone());
        }
        if let Some(protocol) = &context.protocol {
            notification.labels.insert("protocol".into(), protocol.clone());
        }

        notification.dedup_key = Some(rule_key.clone());

        Self { notification, rule_key, context }
    }

    /// Create a threshold alert.
    pub fn threshold(rule_id: &str, metric: &str, value: f64, threshold: f64, context: AlertContext) -> Self {
        let title = format!("Threshold alert: {}", rule_id);
        let body = format!(
            "metric={} value={:.4} threshold={:.4}{}{}{}",
            metric,
            value,
            threshold,
            context.organization_uuid.as_ref().map(|t| format!(" tenant={}", t)).unwrap_or_default(),
            context.endpoint_uuid.as_ref().map(|e| format!(" endpoint={}", e)).unwrap_or_default(),
            context.protocol.as_ref().map(|p| format!(" protocol={}", p)).unwrap_or_default(),
        );

        let rule_key = context.dedup_key("threshold", rule_id);

        Self::new(
            NotificationKind::ThresholdAlert {
                rule_id: rule_id.to_string(),
                metric: metric.to_string(),
                value,
                threshold,
            },
            title,
            body,
            rule_key,
            context,
        )
    }

    /// Create an anti-pattern alert.
    pub fn anti_pattern(
        rule_id: &str,
        pattern_type: &str,
        occurrence_count: u64,
        sample_details: Option<&str>,
        context: AlertContext,
    ) -> Self {
        let title = format!("Anti-pattern detected: {}", pattern_type);
        let mut body = format!(
            "rule={} occurrences={}{}{}",
            rule_id,
            occurrence_count,
            context.organization_uuid.as_ref().map(|t| format!(" tenant={}", t)).unwrap_or_default(),
            context.endpoint_uuid.as_ref().map(|e| format!(" endpoint={}", e)).unwrap_or_default(),
        );

        if let Some(details) = sample_details {
            body.push_str(&format!(" details={}", details));
        }

        let rule_key = context.dedup_key("anti_pattern", &format!("{}:{}", rule_id, pattern_type));

        Self::new(
            NotificationKind::AntiPattern {
                rule_id: rule_id.to_string(),
                pattern_type: pattern_type.to_string(),
                occurrence_count,
            },
            title,
            body,
            rule_key,
            context,
        )
    }

    /// Create a periodic summary alert.
    pub fn summary(rule_id: &str, body: String) -> Self {
        let title = format!("Analytics summary: {}", rule_id);
        let rule_key = format!("report:{}", rule_id);

        Self::new(
            NotificationKind::PeriodicSummary { rule_id: rule_id.to_string() },
            title,
            body,
            rule_key,
            AlertContext::default(),
        )
    }
}
