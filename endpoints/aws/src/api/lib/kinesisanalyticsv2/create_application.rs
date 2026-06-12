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

const API_INFO: ApiInfo<AwsApi, KinesisAnalyticsCreateApplicationInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::KinesisAnalyticsCreateApplication,
    "kinesisanalyticsv2_create_application",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    KinesisAnalyticsCreateApplication,
    API_INFO,
    struct {
        application_name: String,
        runtime_environment: String,
        service_execution_role: String,
        application_configuration: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "ApplicationName": self.application_name,
            "RuntimeEnvironment": self.runtime_environment,
            "ServiceExecutionRole": self.service_execution_role
        });
        let result = client
            .execute_json_target("kinesisanalytics", "KinesisAnalytics_20180523.CreateApplication", Some(&body_val), "1.1")
            .await?;

        span.add_event(
            "received result from aws kinesisanalytics",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = KinesisAnalyticsCreateApplicationInputBuilder::default()
            .application_name("app")
            .runtime_environment("FLINK-1_15")
            .service_execution_role("arn:aws:iam::123456789012:role/role")
            .application_configuration(None::<serde_json::Value>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kinesisanalyticsv2_create_application");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "application_name": "app",
            "runtime_environment": "FLINK-1_15",
            "service_execution_role": "arn:aws:iam::123456789012:role/role"
        });
        let _: KinesisAnalyticsCreateApplicationInput = serde_json::from_value(json).unwrap();
    }
}
