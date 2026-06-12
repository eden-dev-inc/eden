use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use serde::{Deserialize, Serialize};
use telemetry::TelemetryWrapper;

use crate::ep::AwsAsync;

/// Caller identity from STS GetCallerIdentity.
/// Validates that credentials are still working and captures account-level info.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct AwsAccountIdentity {
    pub account_id: String,
    pub arn: String,
    pub user_id: String,
}

impl MetadataCollection for AwsAccountIdentity {
    type Request = ();

    fn request(&self) -> Self::Request {}

    fn description(&self) -> &'static str {
        "Collect AWS account identity via STS GetCallerIdentity"
    }

    fn category(&self) -> &'static str {
        "identity"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl AwsAccountIdentity {
    pub(crate) async fn sync_metadata(
        &self,
        context: AwsAsync,
        _telemetry: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let client = context.get().await.map_err(error::EpError::request)?;
        let resp_text = client.execute_form("sts", "Action=GetCallerIdentity&Version=2011-06-15").await?;

        // Parse XML response: <GetCallerIdentityResult><Account>...</Account><Arn>...</Arn><UserId>...</UserId></GetCallerIdentityResult>
        let identity = AwsAccountIdentity {
            account_id: extract_xml_tag(&resp_text, "Account").unwrap_or_default(),
            arn: extract_xml_tag(&resp_text, "Arn").unwrap_or_default(),
            user_id: extract_xml_tag(&resp_text, "UserId").unwrap_or_default(),
        };

        Ok(identity)
    }
}

/// Simple XML tag extractor for flat AWS API responses.
/// Extracts the text content between `<tag>` and `</tag>`.
pub(crate) fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let remaining = xml.get(start..)?;
    let end_offset = remaining.find(&close)?;
    let value = remaining.get(..end_offset)?;
    Some(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const STS_RESPONSE: &str = r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>arn:aws:iam::123456789012:user/Alice</Arn>
    <UserId>AIDACKCEVSQ6C2EXAMPLE</UserId>
    <Account>123456789012</Account>
  </GetCallerIdentityResult>
</GetCallerIdentityResponse>"#;

    #[test]
    fn extract_xml_tag_parses_sts_response() {
        assert_eq!(extract_xml_tag(STS_RESPONSE, "Account"), Some("123456789012".to_string()));
        assert_eq!(extract_xml_tag(STS_RESPONSE, "Arn"), Some("arn:aws:iam::123456789012:user/Alice".to_string()));
        assert_eq!(extract_xml_tag(STS_RESPONSE, "UserId"), Some("AIDACKCEVSQ6C2EXAMPLE".to_string()));
    }

    #[test]
    fn extract_xml_tag_returns_none_for_missing_tag() {
        assert_eq!(extract_xml_tag(STS_RESPONSE, "Missing"), None);
    }
}
