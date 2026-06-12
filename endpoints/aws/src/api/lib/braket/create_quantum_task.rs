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

const API_INFO: ApiInfo<AwsApi, BraketCreateQuantumTaskInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::BraketCreateQuantumTask, "braket_create_quantum_task", ReqType::Write, true);

crate::aws_endpoint! {
    BraketCreateQuantumTask,
    API_INFO,
    struct {
        action: String,
        device_arn: String,
        output_s3_bucket: String,
        output_s3_key_prefix: String,
        shots: i64
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "action": self.action,
            "deviceArn": self.device_arn,
            "outputS3Bucket": self.output_s3_bucket,
            "outputS3KeyPrefix": self.output_s3_key_prefix,
            "shots": self.shots
        });
        let result = client.execute("braket", "POST", "/quantum-task", None, Some(&body), None).await?;

        span.add_event("received result from aws braket", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = BraketCreateQuantumTaskInputBuilder::default()
            .action("{\"braketSchemaHeader\":{\"name\":\"braket.ir.openqasm.program\",\"version\":\"1\"}}")
            .device_arn("arn:aws:braket:::device/quantum-simulator/amazon/sv1")
            .output_s3_bucket("my-bucket")
            .output_s3_key_prefix("my-prefix")
            .shots(100_i64)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "braket_create_quantum_task");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "action": "{}",
            "device_arn": "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
            "output_s3_bucket": "my-bucket",
            "output_s3_key_prefix": "my-prefix",
            "shots": 100
        });
        let _: BraketCreateQuantumTaskInput = serde_json::from_value(json).unwrap();
    }
}
