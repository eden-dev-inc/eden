//! Alert rules configuration.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default values for alert rules.
pub mod defaults {
    pub const ALERT_COOLDOWN_SECS: u64 = 300;
    pub const REPORT_INTERVAL_SECS: u64 = 3_600;
    pub const REPORT_TOP_N: usize = 5;
    pub const ANTI_PATTERN_MIN_OCCURRENCES: u64 = 1;
    pub const MIN_REQUESTS: u64 = 100;
}

/// Alert rules configuration container.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertRulesConfig {
    /// Threshold-based alert rules.
    pub thresholds: Vec<ThresholdRule>,
    /// Anti-pattern detection rules.
    pub anti_patterns: Vec<AntiPatternRule>,
    /// Periodic report rules.
    pub reports: Vec<ReportRule>,
}

/// Threshold alert rule configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdRule {
    /// Unique rule identifier.
    pub id: String,
    /// Metric to evaluate.
    pub metric: ThresholdMetric,
    /// Comparison operator.
    pub operator: ThresholdOperator,
    /// Threshold value.
    pub threshold: f64,
    /// Minimum requests before rule applies.
    #[serde(default = "default_min_requests")]
    pub min_requests: u64,
    /// Cooldown between alerts (seconds).
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u64,
    /// Optional description.
    pub description: Option<String>,
}

/// Anti-pattern alert rule configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiPatternRule {
    /// Unique rule identifier.
    pub id: String,
    /// Pattern types to match (empty = all).
    #[serde(default)]
    pub pattern_types: Vec<String>,
    /// Minimum occurrences to trigger.
    #[serde(default = "default_min_occurrences")]
    pub min_occurrences: u64,
    /// Cooldown between alerts (seconds).
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u64,
}

/// Periodic report configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportRule {
    /// Unique rule identifier.
    pub id: String,
    /// Interval between reports (seconds).
    #[serde(default = "default_report_interval")]
    pub interval_secs: u64,
    /// Number of top entries to include.
    #[serde(default = "default_top_n")]
    pub top_n: usize,
}

/// Threshold metrics supported by the rules engine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, strum::Display, strum::AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ThresholdMetric {
    ErrorRate,
    SlowRate,
    AvgLatencyUs,
    P95LatencyUs,
    MaxLatencyUs,
    RequestCount,
}

/// Threshold comparison operators.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, strum::Display, strum::AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ThresholdOperator {
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

fn default_min_requests() -> u64 {
    defaults::MIN_REQUESTS
}

fn default_cooldown() -> u64 {
    defaults::ALERT_COOLDOWN_SECS
}

fn default_min_occurrences() -> u64 {
    defaults::ANTI_PATTERN_MIN_OCCURRENCES
}

fn default_report_interval() -> u64 {
    defaults::REPORT_INTERVAL_SECS
}

fn default_top_n() -> usize {
    defaults::REPORT_TOP_N
}

impl ThresholdRule {
    pub fn cooldown(&self) -> Duration {
        Duration::from_secs(self.cooldown_secs)
    }
}

impl AntiPatternRule {
    pub fn cooldown(&self) -> Duration {
        Duration::from_secs(self.cooldown_secs)
    }

    pub fn matches_type(&self, pattern_type: &str) -> bool {
        self.pattern_types.is_empty() || self.pattern_types.iter().any(|p| p == pattern_type)
    }
}

impl ReportRule {
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }
}

impl ThresholdOperator {
    pub fn compare(&self, value: f64, threshold: f64) -> bool {
        match self {
            ThresholdOperator::GreaterThan => value > threshold,
            ThresholdOperator::LessThan => value < threshold,
            ThresholdOperator::GreaterThanOrEqual => value >= threshold,
            ThresholdOperator::LessThanOrEqual => value <= threshold,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ThresholdOperator::GreaterThan => ">",
            ThresholdOperator::LessThan => "<",
            ThresholdOperator::GreaterThanOrEqual => ">=",
            ThresholdOperator::LessThanOrEqual => "<=",
        }
    }
}

impl AlertRulesConfig {
    /// Validate all rules in the configuration.
    pub fn validate(&self) -> Result<(), String> {
        for rule in &self.thresholds {
            if rule.id.trim().is_empty() {
                return Err("threshold rule id is empty".into());
            }
            if rule.threshold.is_nan() {
                return Err(format!("threshold rule '{}' has NaN threshold", rule.id));
            }
        }
        for rule in &self.anti_patterns {
            if rule.id.trim().is_empty() {
                return Err("anti_pattern rule id is empty".into());
            }
        }
        for rule in &self.reports {
            if rule.id.trim().is_empty() {
                return Err("report rule id is empty".into());
            }
            if rule.interval_secs == 0 {
                return Err(format!("report rule '{}' has zero interval", rule.id));
            }
            if rule.top_n == 0 {
                return Err(format!("report rule '{}' has zero top_n", rule.id));
            }
        }
        Ok(())
    }

    /// Get the maximum cooldown/interval across all rules.
    pub fn max_window(&self) -> Duration {
        let mut max = Duration::from_secs(defaults::REPORT_INTERVAL_SECS);

        for rule in &self.thresholds {
            max = max.max(rule.cooldown());
        }
        for rule in &self.anti_patterns {
            max = max.max(rule.cooldown());
        }
        for rule in &self.reports {
            max = max.max(rule.interval());
        }

        max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threshold_operator_compare() {
        assert!(ThresholdOperator::GreaterThan.compare(5.0, 3.0));
        assert!(!ThresholdOperator::GreaterThan.compare(3.0, 5.0));
        assert!(ThresholdOperator::LessThan.compare(3.0, 5.0));
        assert!(ThresholdOperator::GreaterThanOrEqual.compare(5.0, 5.0));
        assert!(ThresholdOperator::LessThanOrEqual.compare(5.0, 5.0));
    }

    #[test]
    fn test_anti_pattern_matches_type() {
        let rule = AntiPatternRule {
            id: "test".to_string(),
            pattern_types: vec!["hot_key".to_string(), "n_plus_one".to_string()],
            min_occurrences: 1,
            cooldown_secs: 300,
        };

        assert!(rule.matches_type("hot_key"));
        assert!(rule.matches_type("n_plus_one"));
        assert!(!rule.matches_type("other"));

        let all_rule = AntiPatternRule {
            id: "all".to_string(),
            pattern_types: vec![],
            min_occurrences: 1,
            cooldown_secs: 300,
        };
        assert!(all_rule.matches_type("anything"));
    }

    #[test]
    fn test_validation() {
        let config = AlertRulesConfig {
            thresholds: vec![ThresholdRule {
                id: "".to_string(),
                metric: ThresholdMetric::ErrorRate,
                operator: ThresholdOperator::GreaterThan,
                threshold: 0.1,
                min_requests: 100,
                cooldown_secs: 300,
                description: None,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
