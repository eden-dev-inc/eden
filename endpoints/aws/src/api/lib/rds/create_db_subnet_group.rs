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

const API_INFO: ApiInfo<AwsApi, RdsCreateDbSubnetGroupInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RdsCreateDbSubnetGroup, "rds_create_db_subnet_group", ReqType::Write, true);

crate::aws_endpoint! {
    RdsCreateDbSubnetGroup,
    API_INFO,
    struct {
        db_subnet_group_name: String,
        db_subnet_group_description: String,
        subnet_ids: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("DBSubnetGroupName".to_string(), self.db_subnet_group_name.clone());
        params.insert("DBSubnetGroupDescription".to_string(), self.db_subnet_group_description.clone());
        params.extend(indexed_list_params("SubnetIds.member", &self.subnet_ids));
        let form_body = build_query_body("CreateDBSubnetGroup", "2014-10-31", &params);
        let result = client.execute_form("rds", &form_body).await?;

        span.add_event("received result from aws rds", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RdsCreateDbSubnetGroupInputBuilder::default()
            .db_subnet_group_name("my-subnet-group")
            .db_subnet_group_description("desc")
            .subnet_ids(vec!["subnet-1".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rds_create_db_subnet_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"db_subnet_group_name": "g", "db_subnet_group_description": "d", "subnet_ids": ["s1"]});
        let _: RdsCreateDbSubnetGroupInput = serde_json::from_value(json).unwrap();
    }
}
