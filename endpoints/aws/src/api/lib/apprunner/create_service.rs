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

const API_INFO: ApiInfo<AwsApi, AppRunnerCreateServiceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::AppRunnerCreateService, "apprunner_create_service", ReqType::Write, true);

crate::aws_endpoint! {
    AppRunnerCreateService,
    API_INFO,
    struct {
        service_name: String,
        source_configuration: serde_json::Value,
        instance_configuration: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"ServiceName": self.service_name, "SourceConfiguration": self.source_configuration});
        let result = client.execute("apprunner", "POST", "/2020-05-15/CreateService", None, Some(&body_val), None).await?;

        span.add_event("received result from aws apprunner", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = AppRunnerCreateServiceInputBuilder::default()
            .service_name("my-service")
            .source_configuration(serde_json::json!({}))
            .instance_configuration(None::<serde_json::Value>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "apprunner_create_service");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"service_name": "my-service", "source_configuration": {}});
        let _: AppRunnerCreateServiceInput = serde_json::from_value(json).unwrap();
    }
}
