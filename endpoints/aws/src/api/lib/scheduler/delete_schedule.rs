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

const API_INFO: ApiInfo<AwsApi, SchedulerDeleteScheduleInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SchedulerDeleteSchedule, "scheduler_delete_schedule", ReqType::Write, true);

crate::aws_endpoint! {
    SchedulerDeleteSchedule,
    API_INFO,
    struct {
        name: String,
        group_name: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/schedules/{}", self.name);
        let result = client.execute("scheduler", "DELETE", &path, None, None, None).await?;

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
        let input = SchedulerDeleteScheduleInputBuilder::default().name("my-schedule").group_name(None::<String>).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "scheduler_delete_schedule");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "my-schedule"});
        let _: SchedulerDeleteScheduleInput = serde_json::from_value(json).unwrap();
    }
}
