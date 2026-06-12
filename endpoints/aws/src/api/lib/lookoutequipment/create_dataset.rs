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

const API_INFO: ApiInfo<AwsApi, LookoutEquipmentCreateDatasetInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LookoutEquipmentCreateDataset,
    "lookoutequipment_create_dataset",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    LookoutEquipmentCreateDataset,
    API_INFO,
    struct {
        dataset_name: String,
        dataset_schema: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "DatasetName": self.dataset_name,
            "DatasetSchema": self.dataset_schema
        });
        let result = client.execute("lookoutequipment", "POST", "/CreateDataset", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws lookoutequipment",
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
        let input = LookoutEquipmentCreateDatasetInputBuilder::default()
            .dataset_name("my-dataset")
            .dataset_schema(serde_json::json!({"key": "value"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lookoutequipment_create_dataset");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "dataset_name": "my-dataset",
            "dataset_schema": {}
        });
        let _: LookoutEquipmentCreateDatasetInput = serde_json::from_value(json).unwrap();
    }
}
