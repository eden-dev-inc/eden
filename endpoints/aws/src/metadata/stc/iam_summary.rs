use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use serde::{Deserialize, Serialize};
use telemetry::TelemetryWrapper;

use super::identity::extract_xml_tag;
use crate::ep::AwsAsync;

/// Account-level IAM summary from GetAccountSummary.
/// Provides counts of IAM resources (users, roles, policies, groups, MFA).
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct AwsIamSummary {
    pub users: u64,
    pub roles: u64,
    pub groups: u64,
    pub policies: u64,
    pub mfa_devices: u64,
    pub mfa_devices_in_use: u64,
    pub access_keys_per_user_quota: u64,
}

impl MetadataCollection for AwsIamSummary {
    type Request = ();

    fn request(&self) -> Self::Request {}

    fn description(&self) -> &'static str {
        "Collect AWS IAM account summary (users, roles, policies, MFA)"
    }

    fn category(&self) -> &'static str {
        "iam"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl AwsIamSummary {
    pub(crate) async fn sync_metadata(
        &self,
        context: AwsAsync,
        _telemetry: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let client = context.get().await.map_err(error::EpError::request)?;
        let resp_text = client.execute_form("iam", "Action=GetAccountSummary&Version=2010-05-08").await?;

        // IAM GetAccountSummary returns XML with <SummaryMap><entry><key>Name</key><value>N</value></entry>...</SummaryMap>
        let summary = AwsIamSummary {
            users: extract_summary_entry(&resp_text, "Users").unwrap_or(0),
            roles: extract_summary_entry(&resp_text, "Roles").unwrap_or(0),
            groups: extract_summary_entry(&resp_text, "Groups").unwrap_or(0),
            policies: extract_summary_entry(&resp_text, "Policies").unwrap_or(0),
            mfa_devices: extract_summary_entry(&resp_text, "MFADevices").unwrap_or(0),
            mfa_devices_in_use: extract_summary_entry(&resp_text, "MFADevicesInUse").unwrap_or(0),
            access_keys_per_user_quota: extract_summary_entry(&resp_text, "AccessKeysPerUserQuota").unwrap_or(0),
        };

        Ok(summary)
    }
}

/// Extract a value from the IAM SummaryMap XML.
/// Format: `<entry><key>KeyName</key><value>123</value></entry>`
fn extract_summary_entry(xml: &str, key: &str) -> Option<u64> {
    let key_tag = format!("<key>{key}</key>");
    let pos = xml.find(&key_tag)?;
    let after_key = &xml[pos + key_tag.len()..];
    let value_str = extract_xml_tag(after_key, "value")?;
    value_str.parse().ok()
}
