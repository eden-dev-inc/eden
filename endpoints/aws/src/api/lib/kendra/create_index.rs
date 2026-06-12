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

const API_INFO: ApiInfo<AwsApi, KendraCreateIndexInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::KendraCreateIndex, "kendra_create_index", ReqType::Write, true);

crate::aws_endpoint! {
    KendraCreateIndex,
    API_INFO,
    struct {
        name: String,
        role_arn: String,
        edition: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_val = serde_json::json!({
            "Name": self.name,
            "RoleArn": self.role_arn
        });
        if let Some(ref v) = self.edition {
            body_val["Edition"] = serde_json::Value::String(v.clone());
        }
        let result = client.execute_json_target("kendra", "AmazonKendra.CreateIndex", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws kendra", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = KendraCreateIndexInputBuilder::default()
            .name("my-index")
            .role_arn("arn:aws:iam::123456789012:role/KendraRole")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kendra_create_index");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-index",
            "role_arn": "arn:aws:iam::123456789012:role/KendraRole"
        });
        let _: KendraCreateIndexInput = serde_json::from_value(json).unwrap();
    }
}
