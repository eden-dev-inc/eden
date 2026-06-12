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

const API_INFO: ApiInfo<AwsApi, SchedulerCreateScheduleInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SchedulerCreateSchedule, "scheduler_create_schedule", ReqType::Write, true);

crate::aws_endpoint! {
    SchedulerCreateSchedule,
    API_INFO,
    struct {
        name: String,
        schedule_expression: String,
        target: serde_json::Value,
        flexible_time_window: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/schedules/{}", self.name);
        let body_val = serde_json::json!({"ScheduleExpression": self.schedule_expression, "Target": self.target, "FlexibleTimeWindow": self.flexible_time_window});
        let result = client.execute("scheduler", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws scheduler", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SchedulerCreateScheduleInputBuilder::default()
            .name("my-schedule")
            .schedule_expression("rate(1 hour)")
            .target(serde_json::json!({"Arn": "arn:aws:lambda:us-east-1:123456789012:function:MyFunction", "RoleArn": "arn:aws:iam::123456789012:role/MyRole"}))
            .flexible_time_window(serde_json::json!({"Mode": "OFF"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "scheduler_create_schedule");
    }

    #[test]
    fn deserialize_minimal() {
        let json =
            serde_json::json!({"name": "my-schedule", "schedule_expression": "rate(1 hour)", "target": {}, "flexible_time_window": {}});
        let _: SchedulerCreateScheduleInput = serde_json::from_value(json).unwrap();
    }
}
