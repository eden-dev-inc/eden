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

const API_INFO: ApiInfo<AwsApi, DescribeImagesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2DescribeImages, "Describes one or more AMIs", ReqType::Read, true);

crate::aws_endpoint! {
    DescribeImages,
    API_INFO,
    struct {
        image_ids: Option<Vec<String>>,
        owners: Option<Vec<String>>,
        filters: Option<Vec<AwsFilter>>,
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(ids) = &self.image_ids {
            params.extend(indexed_list_params("ImageId", ids));
        }
        if let Some(owners) = &self.owners {
            params.extend(indexed_list_params("Owner", owners));
        }
        if let Some(filters) = &self.filters {
            params.extend(filters_to_params(filters));
        }
        if let Some(max) = self.max_results {
            params.insert("MaxResults".to_string(), max.to_string());
        }
        if let Some(token) = &self.next_token {
            params.insert("NextToken".to_string(), token.clone());
        }
        let form_body = build_query_body("DescribeImages", "2016-11-15", &params);
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
        let input = DescribeImagesInputBuilder::default()
            .image_ids(None::<Vec<String>>)
            .owners(None::<Vec<String>>)
            .filters(None::<Vec<AwsFilter>>)
            .max_results(None::<i64>)
            .next_token(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_describe_images");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: DescribeImagesInput = serde_json::from_value(json).unwrap();
    }
}
