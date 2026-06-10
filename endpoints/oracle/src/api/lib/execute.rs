use crate::api::input::SqlParam;
use crate::api::lib::OracleApi;
use crate::api::output::U64Output;
use crate::request::OracleRequest;
use crate::{ApiInfo, ReqType, RunOutput};
use ep_core::{EpOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use oracle_client::sql_type::ToSql;
use oracle_core::{OracleAsync, OracleTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<OracleApi, ExecuteInput> = ApiInfo::new(EpKind::Oracle, OracleApi::Execute, "Oracle execute", ReqType::Write, true);

crate::oracle_endpoint! {
    struct ExecuteInput {
        sql: String,
        params: Vec<SqlParam>,
    }
}

impl_simple_operation!(SimpleInput, OracleAsync, OracleTx, OracleApi, OracleRequest);

impl SimpleInput {
    pub fn param_refs(&self) -> Vec<&dyn ToSql> {
        self.params.iter().map(|p| p as &dyn ToSql).collect::<Vec<&dyn ToSql>>()
    }
    #[named]
    async fn run_async_generic(&self, context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint(), API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::request)?;

        let result = client
            .execute(&self.sql, self.param_refs().as_slice())
            .map_err(EpError::request)?
            .row_count()
            .map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from oracle",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(U64Output(result).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut OracleTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
