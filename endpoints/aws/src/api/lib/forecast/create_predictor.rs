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

const API_INFO: ApiInfo<AwsApi, ForecastCreatePredictorInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ForecastCreatePredictor, "forecast_create_predictor", ReqType::Write, true);

crate::aws_endpoint! {
    ForecastCreatePredictor,
    API_INFO,
    struct {
        predictor_name: String,
        algorithm_arn: Option<String>,
        forecast_horizon: i64,
        input_data_config: serde_json::Value,
        featurization_config: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "PredictorName": self.predictor_name,
            "ForecastHorizon": self.forecast_horizon,
            "InputDataConfig": self.input_data_config,
            "FeaturizationConfig": self.featurization_config
        });
        let result = client.execute("forecast", "POST", "/CreatePredictor", None, Some(&body_val), None).await?;

        span.add_event("received result from aws forecast", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ForecastCreatePredictorInputBuilder::default()
            .predictor_name("my-predictor")
            .algorithm_arn(None::<String>)
            .forecast_horizon(10i64)
            .input_data_config(serde_json::json!({"key": "value"}))
            .featurization_config(serde_json::json!({"key": "value"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "forecast_create_predictor");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "predictor_name": "my-predictor",
            "forecast_horizon": 10,
            "input_data_config": {},
            "featurization_config": {}
        });
        let _: ForecastCreatePredictorInput = serde_json::from_value(json).unwrap();
    }
}
