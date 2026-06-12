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

const API_INFO: ApiInfo<AwsApi, SsmCreateAssociationInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SsmCreateAssociation, "ssm_create_association", ReqType::Write, true);

crate::aws_endpoint! {
    SsmCreateAssociation,
    API_INFO,
    struct {
        name: String,
        instance_id: Option<String>,
        targets: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("Name".to_string(), Value::String(self.name.clone()));
        if let Some(iid) = &self.instance_id {
            body.insert("InstanceId".to_string(), Value::String(iid.clone()));
        }
        if let Some(t) = &self.targets {
            body.insert("Targets".to_string(), t.clone());
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ssm", "AmazonSSM.CreateAssociation", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws ssm", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SsmCreateAssociationInputBuilder::default().name("AWS-RunShellScript").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ssm_create_association");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "AWS-RunShellScript"});
        let _: SsmCreateAssociationInput = serde_json::from_value(json).unwrap();
    }
}
