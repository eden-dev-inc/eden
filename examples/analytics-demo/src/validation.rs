// Data Validation Module
//
// Validates data integrity by comparing written data against read-back data.
// Uses sub-sampling at high load to minimize performance impact.

use rand::Rng;
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::metrics::AppMetrics;

/// DataValidator performs write-through validation with configurable sampling.
/// At high throughput, only a fraction of operations are validated to reduce overhead.
pub struct DataValidator {
    sample_rate: f64,
    metrics: Arc<AppMetrics>,
}

impl DataValidator {
    pub fn new(sample_rate: f64, metrics: Arc<AppMetrics>) -> Self {
        let sample_rate = sample_rate.clamp(0.0, 1.0);
        Self { sample_rate, metrics }
    }

    /// Returns true if this operation should be validated based on sample rate
    #[inline]
    pub fn should_validate(&self) -> bool {
        if self.sample_rate >= 1.0 {
            return true;
        }
        if self.sample_rate <= 0.0 {
            return false;
        }
        rand::thread_rng().gen::<f64>() < self.sample_rate
    }

    /// Validate that two JSON-serializable values are equivalent.
    /// Returns Ok(()) if valid, Err with description if mismatch.
    pub fn validate_json<T>(&self, data_type: &str, original: &T, retrieved: &T) -> Result<(), String>
    where
        T: Serialize,
    {
        let original_json = serde_json::to_string(original)
            .map_err(|e| format!("serialize_original: {}", e))?;
        let retrieved_json = serde_json::to_string(retrieved)
            .map_err(|e| format!("serialize_retrieved: {}", e))?;

        if original_json == retrieved_json {
            self.metrics.record_validation_success(data_type);
            debug!("Validation passed for {}", data_type);
            Ok(())
        } else {
            self.metrics.record_validation_error(data_type, "mismatch");
            warn!(
                "Validation FAILED for {}: data mismatch\nOriginal: {}\nRetrieved: {}",
                data_type,
                &original_json[..original_json.len().min(200)],
                &retrieved_json[..retrieved_json.len().min(200)]
            );
            Err(format!("data mismatch for {}", data_type))
        }
    }

    /// Validate raw JSON strings match
    pub fn validate_json_str(&self, data_type: &str, original: &str, retrieved: &str) -> Result<(), String> {
        if original == retrieved {
            self.metrics.record_validation_success(data_type);
            debug!("Validation passed for {}", data_type);
            Ok(())
        } else {
            // Try to parse and compare as JSON values to handle formatting differences
            let orig_val: Result<serde_json::Value, _> = serde_json::from_str(original);
            let ret_val: Result<serde_json::Value, _> = serde_json::from_str(retrieved);

            match (orig_val, ret_val) {
                (Ok(o), Ok(r)) if o == r => {
                    self.metrics.record_validation_success(data_type);
                    debug!("Validation passed for {} (semantic match)", data_type);
                    Ok(())
                }
                (Ok(_), Ok(_)) => {
                    self.metrics.record_validation_error(data_type, "mismatch");
                    warn!(
                        "Validation FAILED for {}: JSON values differ\nOriginal: {}\nRetrieved: {}",
                        data_type,
                        &original[..original.len().min(200)],
                        &retrieved[..retrieved.len().min(200)]
                    );
                    Err(format!("data mismatch for {}", data_type))
                }
                _ => {
                    self.metrics.record_validation_error(data_type, "parse_error");
                    warn!("Validation FAILED for {}: JSON parse error", data_type);
                    Err(format!("JSON parse error for {}", data_type))
                }
            }
        }
    }

    /// Record a validation where data was not found on read-back
    pub fn record_not_found(&self, data_type: &str) {
        self.metrics.record_validation_error(data_type, "not_found");
        warn!("Validation FAILED for {}: data not found on read-back", data_type);
    }

    /// Record a read error during validation
    pub fn record_read_error(&self, data_type: &str) {
        self.metrics.record_validation_error(data_type, "read_error");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_rate_bounds() {
        let metrics = Arc::new(AppMetrics::new());

        // 0% should never validate
        let v = DataValidator::new(0.0, metrics.clone());
        let mut validated = false;
        for _ in 0..100 {
            if v.should_validate() {
                validated = true;
                break;
            }
        }
        assert!(!validated, "0% sample rate should never validate");

        // 100% should always validate
        let v = DataValidator::new(1.0, metrics.clone());
        for _ in 0..100 {
            assert!(v.should_validate(), "100% sample rate should always validate");
        }
    }
}
