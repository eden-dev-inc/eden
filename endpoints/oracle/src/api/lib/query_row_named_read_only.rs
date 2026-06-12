use crate::api::input::NamedParam;
use crate::api::lib::OracleApi;
use crate::api::output::{OracleRowOutput, RowWrapper};
use crate::request::OracleRequest;
use crate::{ApiInfo, ReqType, RunOutput};
use ep_core::{EpOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use oracle_client::Row;
use oracle_client::sql_type::ToSql;
use oracle_core::{OracleAsync, OracleTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<OracleApi, QueryRowNamedReadOnlyInput> = ApiInfo::new(
    EpKind::Oracle,
    OracleApi::QueryRowNamedReadOnly,
    "Oracle Read-Only Query Row Named",
    ReqType::Read,
    true,
);

crate::oracle_endpoint! {
    struct QueryRowNamedReadOnlyInput {
        query: String,
        params: Vec<NamedParam>
    }
}

impl_simple_operation!(SimpleInput, OracleAsync, OracleTx, OracleApi, OracleRequest);

impl SimpleInput {
    pub async fn run_query(&self, context: OracleAsync) -> ResultEP<Row> {
        let client = context.get().await.map_err(EpError::request)?;

        client
            .query_row_named(
                self.query(),
                self.params.iter().map(|n| (n.name.as_str(), &n.param as &dyn ToSql)).collect::<Vec<(&str, &dyn ToSql)>>().as_slice(),
            )
            .map_err(EpError::request)
    }

    #[named]
    async fn run_async_generic(&self, context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint(), API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let result = self.run_query(context).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from oracle",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(OracleRowOutput(RowWrapper::from_oracle_row(&result)?).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut OracleTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
