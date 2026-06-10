use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
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

const API_INFO: ApiInfo<AwsApi, CloudWatchGetMetricStatisticsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudWatchGetMetricStatistics,
    "cloudwatch_get_metric_statistics",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudWatchGetMetricStatistics,
    API_INFO,
    struct {
        namespace: String,
        metric_name: String,
        start_time: String,
        end_time: String,
        period: i64,
        statistics: Option<Vec<String>>
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
        params.insert("MetricName".to_string(), self.metric_name.clone());
        params.insert("StartTime".to_string(), self.start_time.clone());
        params.insert("EndTime".to_string(), self.end_time.clone());
        params.insert("Period".to_string(), self.period.to_string());
        if let Some(stats) = &self.statistics {
            params.extend(indexed_list_params("Statistics.member", stats));
        }
        let form_body = build_query_body("GetMetricStatistics", "2010-08-01", &params);
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
        let input = CloudWatchGetMetricStatisticsInputBuilder::default()
            .namespace("AWS/EC2")
            .metric_name("CPUUtilization")
            .start_time("2024-01-01T00:00:00Z")
            .end_time("2024-01-02T00:00:00Z")
            .period(300i64)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_get_metric_statistics");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "namespace": "AWS/EC2",
            "metric_name": "CPUUtilization",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z",
            "period": 300
        });
        let _: CloudWatchGetMetricStatisticsInput = serde_json::from_value(json).unwrap();
    }
}
