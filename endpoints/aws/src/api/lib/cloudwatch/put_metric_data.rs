use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, CloudWatchPutMetricDataInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudWatchPutMetricData, "cloudwatch_put_metric_data", ReqType::Write, true);

crate::aws_endpoint! {
    CloudWatchPutMetricData,
    API_INFO,
    struct {
        namespace: String,
        metric_name: String,
        value: f64,
        unit: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("Namespace".to_string(), self.namespace.clone());
        params.insert("MetricData.member.1.MetricName".to_string(), self.metric_name.clone());
        params.insert("MetricData.member.1.Value".to_string(), self.value.to_string());
        if let Some(u) = &self.unit {
            params.insert("MetricData.member.1.Unit".to_string(), u.clone());
        }
        let form_body = build_query_body("PutMetricData", "2010-08-01", &params);
        let result = client.execute_form("cloudwatch", &form_body).await?;

        span.add_event(
            "received result from aws cloudwatch",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = CloudWatchPutMetricDataInputBuilder::default()
            .namespace("MyNamespace")
            .metric_name("MyMetric")
            .value(1.0)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_put_metric_data");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"namespace": "MyNamespace", "metric_name": "MyMetric", "value": 1.0});
        let _: CloudWatchPutMetricDataInput = serde_json::from_value(json).unwrap();
    }
}
