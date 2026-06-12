use crate::api::input::NamedParam;
use crate::api::lib::OracleApi;
use crate::api::output::{OracleRowsOutput, RowWrapper};
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

const API_INFO: ApiInfo<OracleApi, QueryNamedInput> =
    ApiInfo::new(EpKind::Oracle, OracleApi::QueryNamed, "Oracle Query", ReqType::Write, true);

crate::oracle_endpoint! {
    struct QueryNamedInput {
        query: String,
        params: Vec<NamedParam>
    }
}

impl_simple_operation!(SimpleInput, OracleAsync, OracleTx, OracleApi, OracleRequest);

impl SimpleInput {
    pub async fn run_query(&self, context: OracleAsync) -> ResultEP<Vec<Row>> {
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = vec![];

        for param in &self.params {
            params.push((&param.name as &str, &param.param as &dyn ToSql))
        }

        let mut rows = vec![];
        for result in client
            .query_named(
                self.query(),
                self.params.iter().map(|n| (n.name.as_str(), &n.param as &dyn ToSql)).collect::<Vec<(&str, &dyn ToSql)>>().as_slice(),
            )
            .map_err(EpError::request)?
        {
            rows.push(result.map_err(EpError::request)?);
        }

        Ok(rows)
    }

    #[named]
    async fn run_async_generic(&self, context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint(), API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let result = RowWrapper::from_oracle_rows(self.run_query(context).await?)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(OracleRowsOutput(result).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut OracleTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
