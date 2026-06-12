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

const API_INFO: ApiInfo<AwsApi, MqCreateBrokerInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::MqCreateBroker, "mq_create_broker", ReqType::Write, true);

crate::aws_endpoint! {
    MqCreateBroker,
    API_INFO,
    struct {
        broker_name: String,
        engine_type: String,
        engine_version: String,
        host_instance_type: String,
        deployment_mode: String,
        publicly_accessible: bool
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "brokerName": self.broker_name,
            "engineType": self.engine_type,
            "engineVersion": self.engine_version,
            "hostInstanceType": self.host_instance_type,
            "deploymentMode": self.deployment_mode,
            "publiclyAccessible": self.publicly_accessible
        });
        let result = client.execute("mq", "POST", "/v1/brokers", None, Some(&body_val), None).await?;

        span.add_event("received result from aws mq", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = MqCreateBrokerInputBuilder::default()
            .broker_name("my-broker")
            .engine_type("ACTIVEMQ")
            .engine_version("5.15.14")
            .host_instance_type("mq.m5.large")
            .deployment_mode("SINGLE_INSTANCE")
            .publicly_accessible(true)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "mq_create_broker");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "broker_name": "my-broker",
            "engine_type": "ACTIVEMQ",
            "engine_version": "5.15.14",
            "host_instance_type": "mq.m5.large",
            "deployment_mode": "SINGLE_INSTANCE",
            "publicly_accessible": true
        });
        let _: MqCreateBrokerInput = serde_json::from_value(json).unwrap();
    }
}
