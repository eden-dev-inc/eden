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

const API_INFO: ApiInfo<AwsApi, MskCreateClusterInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::MskCreateCluster, "Creates an MSK cluster", ReqType::Write, true);

crate::aws_endpoint! {
    MskCreateCluster,
    API_INFO,
    struct {
        cluster_name: String,
        kafka_version: String,
        number_of_broker_nodes: i64,
        broker_node_group_info: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "clusterName": self.cluster_name,
            "kafkaVersion": self.kafka_version,
            "numberOfBrokerNodes": self.number_of_broker_nodes,
            "brokerNodeGroupInfo": self.broker_node_group_info
        });
        let result = client.execute("kafka", "POST", "/v1/clusters", None, Some(&body_val), None).await?;

        span.add_event("received result from aws kafka", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = MskCreateClusterInputBuilder::default()
            .cluster_name("my-cluster")
            .kafka_version("2.8.0")
            .number_of_broker_nodes(3_i64)
            .broker_node_group_info(serde_json::json!({"instanceType": "kafka.m5.large"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "msk_create_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "cluster_name": "my-cluster",
            "kafka_version": "2.8.0",
            "number_of_broker_nodes": 3,
            "broker_node_group_info": {}
        });
        let _: MskCreateClusterInput = serde_json::from_value(json).unwrap();
    }
}
