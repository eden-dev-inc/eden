use datadog_core::comm::DatadogClient;
use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::ResultEP;

pub const DD_API_VALID: CapabilityId = CapabilityId("dd.api_valid");

#[derive(Debug, Clone)]
pub struct DatadogCapabilities {
    pub api_valid: bool,
}

impl DatadogCapabilities {
    pub async fn discover(client: &DatadogClient) -> ResultEP<Self> {
        let api_valid = client.health_check().await.is_ok();

        Ok(Self { api_valid })
    }
}

impl CapabilityChecker for DatadogCapabilities {
    fn has(&self, id: &CapabilityId) -> bool {
        match id.0 {
            "dd.api_valid" => self.api_valid,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_checks() {
        let valid = DatadogCapabilities { api_valid: true };
        assert!(valid.has(&DD_API_VALID));
        assert!(!valid.has(&CapabilityId("dd.nonexistent")));

        let invalid = DatadogCapabilities { api_valid: false };
        assert!(!invalid.has(&DD_API_VALID));
    }
}
