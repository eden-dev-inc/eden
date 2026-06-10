use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, filters_to_params, indexed_list_params};
use crate::api::lib::types::AwsFilter;
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

const API_INFO: ApiInfo<AwsApi, DescribeRegionsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Ec2DescribeRegions,
    "Describes the AWS Regions that are enabled for your account",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    DescribeRegions,
    API_INFO,
    struct {
        region_names: Option<Vec<String>>,
        filters: Option<Vec<AwsFilter>>,
        all_regions: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(names) = &self.region_names {
            params.extend(indexed_list_params("RegionName", names));
        }
        if let Some(filters) = &self.filters {
            params.extend(filters_to_params(filters));
        }
        if let Some(all) = self.all_regions {
            params.insert("AllRegions".to_string(), all.to_string());
        }
        let form_body = build_query_body("DescribeRegions", "2016-11-15", &params);
        let result = client.execute_form("ec2", &form_body).await?;

        span.add_event("received result from aws ec2", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = DescribeRegionsInputBuilder::default()
            .region_names(None::<Vec<String>>)
            .filters(None::<Vec<AwsFilter>>)
            .all_regions(None::<bool>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_describe_regions");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: DescribeRegionsInput = serde_json::from_value(json).unwrap();
    }
}
