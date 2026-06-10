//! Alert rules engine.
//!
//! This module provides configurable alert rules for evaluating analytics data
//! and generating notifications.

mod config;
mod engine;
mod types;

pub use config::{AlertRulesConfig, AntiPatternRule, ReportRule, ThresholdMetric, ThresholdOperator, ThresholdRule};
pub use engine::RulesEngine;
pub use types::{AlertContext, EvaluationResult, PendingAlert};
