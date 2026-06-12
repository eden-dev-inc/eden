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

const API_INFO: ApiInfo<AwsApi, CloudWatchPutMetricAlarmInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudWatchPutMetricAlarm, "cloudwatch_put_metric_alarm", ReqType::Write, true);

crate::aws_endpoint! {
    CloudWatchPutMetricAlarm,
    API_INFO,
    struct {
        alarm_name: String,
        comparison_operator: String,
        evaluation_periods: i64,
        metric_name: String,
        namespace: String,
        period: i64,
        threshold: String,
        statistic: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("AlarmName".to_string(), self.alarm_name.clone());
        params.insert("ComparisonOperator".to_string(), self.comparison_operator.clone());
        params.insert("EvaluationPeriods".to_string(), self.evaluation_periods.to_string());
        params.insert("MetricName".to_string(), self.metric_name.clone());
        params.insert("Namespace".to_string(), self.namespace.clone());
        params.insert("Period".to_string(), self.period.to_string());
        params.insert("Threshold".to_string(), self.threshold.clone());
        if let Some(stat) = &self.statistic {
            params.insert("Statistic".to_string(), stat.clone());
        }
        let form_body = build_query_body("PutMetricAlarm", "2010-08-01", &params);
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
        let input = CloudWatchPutMetricAlarmInputBuilder::default()
            .alarm_name("my-alarm")
            .comparison_operator("GreaterThanThreshold")
            .evaluation_periods(1i64)
            .metric_name("CPUUtilization")
            .namespace("AWS/EC2")
            .period(300i64)
            .threshold("80.0")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_put_metric_alarm");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "alarm_name": "my-alarm",
            "comparison_operator": "GreaterThanThreshold",
            "evaluation_periods": 1,
            "metric_name": "CPUUtilization",
            "namespace": "AWS/EC2",
            "period": 300,
            "threshold": "80.0"
        });
        let _: CloudWatchPutMetricAlarmInput = serde_json::from_value(json).unwrap();
    }
}
