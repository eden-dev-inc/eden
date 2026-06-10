use crate::api::lib::GitlabApi;
use crate::output::GitlabJsonOutput;
use crate::request::GitlabRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use gitlab_core::{GitlabAsync, GitlabTx};
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<GitlabApi, CustomInput> =
    ApiInfo::new(EpKind::Gitlab, GitlabApi::Custom, "Execute a custom GitLab API request", ReqType::Read);

crate::gitlab_endpoint! {
    Custom,
    API_INFO,
    struct {
        method: String,
        path: String,
        body: Option<Value>
    }
}

impl_simple_operation!(SimpleInput, GitlabAsync, GitlabTx, GitlabApi, GitlabRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: GitlabAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("gitlab.{}.{}", API_INFO.api(), function_name!()));

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.request(&self.method, &self.path, self.body.clone()).await?;

        span.add_event("received result from gitlab", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(GitlabJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut GitlabTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("GitLab transaction support not implemented")
    }
}
