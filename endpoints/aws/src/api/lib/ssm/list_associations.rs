use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, SsmListAssociationsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SsmListAssociations, "ssm_list_associations", ReqType::Read, true);

crate::aws_endpoint! {
    SsmListAssociations,
    API_INFO,
    struct {
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(mr) = self.max_results {
            body.insert("MaxResults".to_string(), serde_json::json!(mr));
        }
        if let Some(nt) = &self.next_token {
            body.insert("NextToken".to_string(), Value::String(nt.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ssm", "AmazonSSM.ListAssociations", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws ssm", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SsmListAssociationsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ssm_list_associations");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: SsmListAssociationsInput = serde_json::from_value(json).unwrap();
    }
}
