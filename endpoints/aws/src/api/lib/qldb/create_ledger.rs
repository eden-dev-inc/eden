use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, QldbCreateLedgerInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::QldbCreateLedger, "qldb_create_ledger", ReqType::Write, true);

crate::aws_endpoint! {
    QldbCreateLedger,
    API_INFO,
    struct {
        name: String,
        permissions_mode: String,
        deletion_protection: Option<bool>,
        tags: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "Name": self.name,
            "PermissionsMode": self.permissions_mode
        });
        let result = client.execute("qldb", "POST", "/ledgers", None, Some(&body_val), None).await?;

        span.add_event("received result from aws qldb", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = QldbCreateLedgerInputBuilder::default()
            .name("my-ledger")
            .permissions_mode("ALLOW_ALL")
            .deletion_protection(None::<bool>)
            .tags(None::<serde_json::Value>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "qldb_create_ledger");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-ledger",
            "permissions_mode": "ALLOW_ALL"
        });
        let _: QldbCreateLedgerInput = serde_json::from_value(json).unwrap();
    }
}
