use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use serde::{Deserialize, Serialize};
use telemetry::TelemetryWrapper;

use crate::ep::AwsAsync;

/// Human-readable account aliases from IAM ListAccountAliases.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct AwsAccountAliases {
    pub aliases: Vec<String>,
}

impl MetadataCollection for AwsAccountAliases {
    type Request = ();

    fn request(&self) -> Self::Request {}

    fn description(&self) -> &'static str {
        "Collect AWS account aliases"
    }

    fn category(&self) -> &'static str {
        "account"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

impl AwsAccountAliases {
    pub(crate) async fn sync_metadata(
        &self,
        context: AwsAsync,
        _telemetry: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let client = context.get().await.map_err(error::EpError::request)?;
        let resp_text = client.execute_form("iam", "Action=ListAccountAliases&Version=2010-05-08").await?;

        // Parse XML: <AccountAliases><member>alias-name</member>...</AccountAliases>
        let mut aliases = Vec::new();
        let mut search_from = resp_text.as_str();
        let open_tag = "<member>";
        let close_tag = "</member>";
        while let Some(start) = search_from.find(open_tag) {
            let value_start = start + open_tag.len();
            let remaining = match search_from.get(value_start..) {
                Some(s) => s,
                None => break,
            };
            if let Some(end_offset) = remaining.find(close_tag) {
                if let Some(alias) = remaining.get(..end_offset) {
                    aliases.push(alias.to_string());
                }
                let advance = value_start + end_offset + close_tag.len();
                search_from = match search_from.get(advance..) {
                    Some(s) => s,
                    None => break,
                };
            } else {
                break;
            }
        }

        Ok(AwsAccountAliases { aliases })
    }
}
