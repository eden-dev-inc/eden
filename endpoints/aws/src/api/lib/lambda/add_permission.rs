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

const API_INFO: ApiInfo<AwsApi, LambdaAddPermissionInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::LambdaAddPermission, "lambda_add_permission", ReqType::Write, true);

crate::aws_endpoint! {
    LambdaAddPermission,
    API_INFO,
    struct {
        function_name: String,
        statement_id: String,
        action: String,
        principal: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2015-03-31/functions/{}/policy", self.function_name);
        let body_val = serde_json::json!({
            "StatementId": self.statement_id,
            "Action": self.action,
            "Principal": self.principal
        });
        let result = client.execute("lambda", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LambdaAddPermissionInputBuilder::default()
            .function_name("fn")
            .statement_id("s1")
            .action("lambda:InvokeFunction")
            .principal("events.amazonaws.com")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_add_permission");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "function_name": "fn",
            "statement_id": "s1",
            "action": "lambda:InvokeFunction",
            "principal": "events.amazonaws.com"
        });
        let _: LambdaAddPermissionInput = serde_json::from_value(json).unwrap();
    }
}
