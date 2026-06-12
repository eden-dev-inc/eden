use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, CreateGitCredentialInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::CreateGitCredential, "Create a Git credential", ReqType::Write);

crate::databricks_endpoint! {
    CreateGitCredential,
    API_INFO,
    struct {
        git_provider: String,
        git_username: String,
        personal_access_token: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let body = serde_json::json!({
            "git_provider": self.git_provider,
            "git_username": self.git_username,
            "personal_access_token": self.personal_access_token,
        });

        let value = client.post("/api/2.0/git-credentials", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created git credential on databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(DatabricksJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatabricksTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Databricks transaction support not implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_git_credential_builder_serde() {
        let input = CreateGitCredentialInputBuilder::default()
            .git_provider("github")
            .git_username("my-user")
            .personal_access_token("ghp_token123")
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "creategitcredential");
        assert_eq!(json["git_provider"], "github");
        assert_eq!(json["git_username"], "my-user");
        assert_eq!(json["personal_access_token"], "ghp_token123");
    }
}
