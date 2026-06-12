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

const API_INFO: ApiInfo<AwsApi, SsmGetParameterHistoryInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SsmGetParameterHistory, "ssm_get_parameter_history", ReqType::Read, true);

crate::aws_endpoint! {
    SsmGetParameterHistory,
    API_INFO,
    struct {
        name: String,
        with_decryption: Option<bool>,
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
        body.insert("Name".to_string(), Value::String(self.name.clone()));
        if let Some(b) = self.with_decryption {
            body.insert("WithDecryption".to_string(), serde_json::json!(b));
        }
        if let Some(v) = self.max_results {
            body.insert("MaxResults".to_string(), serde_json::json!(v));
        }
        if let Some(v) = &self.next_token {
            body.insert("NextToken".to_string(), Value::String(v.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ssm", "AmazonSSM.GetParameterHistory", Some(&body_val), "1.1").await?;

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
        let input = SsmGetParameterHistoryInputBuilder::default().name("/my/param").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ssm_get_parameter_history");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "/my/param"});
        let _: SsmGetParameterHistoryInput = serde_json::from_value(json).unwrap();
    }
}
