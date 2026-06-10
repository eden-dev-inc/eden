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

const API_INFO: ApiInfo<AwsApi, CfListStacksInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CfListStacks,
    "Returns the summary information for stacks whose status matches the specified filter",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CfListStacks,
    API_INFO,
    struct {
        next_token: Option<String>,
        stack_status_filter: Option<Vec<String>>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(t) = &self.next_token {
            params.insert("NextToken".to_string(), t.clone());
        }
        if let Some(filters) = &self.stack_status_filter {
            params.extend(indexed_list_params("StackStatusFilter.member", filters));
        }
        let form_body = build_query_body("ListStacks", "2010-05-15", &params);
        let result = client.execute_form("cloudformation", &form_body).await?;

        span.add_event(
            "received result from aws cloudformation",
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
        let input = CfListStacksInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudformation_list_stacks");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: CfListStacksInput = serde_json::from_value(json).unwrap();
    }
}
