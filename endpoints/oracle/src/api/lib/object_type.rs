use crate::api::lib::OracleApi;
use crate::request::OracleRequest;
use crate::{ApiInfo, ReqType, RunOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use oracle_core::{OracleAsync, OracleTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<OracleApi, ObjectTypeInput> =
    ApiInfo::new(EpKind::Oracle, OracleApi::ObjectType, "Oracle execute named", ReqType::Write, true);

crate::oracle_endpoint! {
    struct ObjectTypeInput {
        name: String,
    }
}

impl_simple_operation!(SimpleInput, OracleAsync, OracleTx, OracleApi, OracleRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint(), API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::request)?;

        client.object_type(&self.name).map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from oracle",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Err(EpError::request("object type result output"))
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut OracleTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
