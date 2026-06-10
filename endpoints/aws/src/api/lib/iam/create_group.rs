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

const API_INFO: ApiInfo<AwsApi, IamCreateGroupInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IamCreateGroup, "iam_create_group", ReqType::Write, true);

crate::aws_endpoint! {
    IamCreateGroup,
    API_INFO,
    struct {
        group_name: String,
        path: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("GroupName".to_string(), self.group_name.clone());
        if let Some(p) = &self.path {
            params.insert("Path".to_string(), p.clone());
        }
        let form_body = build_query_body("CreateGroup", "2010-05-08", &params);
        let result = client.execute_form("iam", &form_body).await?;

        span.add_event("received result from aws iam", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = IamCreateGroupInputBuilder::default().group_name("my-group").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_create_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"group_name": "g"});
        let _: IamCreateGroupInput = serde_json::from_value(json).unwrap();
    }
}
