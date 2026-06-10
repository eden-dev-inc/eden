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

const API_INFO: ApiInfo<AwsApi, DataBrewCreateProjectInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DataBrewCreateProject, "databrew_create_project", ReqType::Write, true);

crate::aws_endpoint! {
    DataBrewCreateProject,
    API_INFO,
    struct {
        name: String,
        dataset_name: String,
        recipe_ref: serde_json::Value,
        role_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "Name": self.name,
            "DatasetName": self.dataset_name,
            "RecipeRef": self.recipe_ref,
            "RoleArn": self.role_arn
        });
        let result = client.execute("databrew", "POST", "/projects", None, Some(&body_val), None).await?;

        span.add_event("received result from aws databrew", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = DataBrewCreateProjectInputBuilder::default()
            .name("my-project")
            .dataset_name("my-dataset")
            .recipe_ref(serde_json::json!({"name": "my-recipe", "recipeVersion": "1.0"}))
            .role_arn("arn:aws:iam::123456789012:role/DataBrewRole")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "databrew_create_project");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-project",
            "dataset_name": "my-dataset",
            "recipe_ref": {"name": "my-recipe", "recipeVersion": "1.0"},
            "role_arn": "arn:aws:iam::123456789012:role/DataBrewRole"
        });
        let _: DataBrewCreateProjectInput = serde_json::from_value(json).unwrap();
    }
}
