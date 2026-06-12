use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, EksUpdateClusterConfigInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::EksUpdateClusterConfig,
    "Updates an EKS cluster configuration",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    EksUpdateClusterConfig,
    API_INFO,
    struct {
        name: String,
        resources_vpc_config: Option<Value>,
        logging: Option<Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(vpc) = &self.resources_vpc_config {
            body.insert("resourcesVpcConfig".to_string(), vpc.clone());
        }
        if let Some(log) = &self.logging {
            body.insert("logging".to_string(), log.clone());
        }
        let body_val = Value::Object(body);
        let path = format!("/clusters/{}/update-config", self.name);
        let result = client.execute("eks", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws eks", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = EksUpdateClusterConfigInputBuilder::default().name("c").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eks_update_cluster_config");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "c"});
        let _: EksUpdateClusterConfigInput = serde_json::from_value(json).unwrap();
    }
}
