use super::api::wrapper::input::SqlParam;
use crate::api::lib::PostgresApi;
use crate::{EpRequest, Operation, PostgresOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use postgres_core::{PostgresAsync, PostgresTx};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

define_request!(EpKind::Postgres => Postgres, PostgresOperation, PostgresAsync, PostgresApi, PostgresTx);

impl ToSchema for PostgresRequest {}
impl PartialSchema for PostgresRequest {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("kind", PostgresKind::schema())
                .property("type", PostgresApi::schema())
                .property("query", String::schema())
                .property("execute", String::schema())
                .property("params", <Vec<SqlParam>>::schema())
                .required("kind")
                .required("type")
                .build(),
        ))
    }
}

#[allow(dead_code)]
#[derive(ToSchema)]
enum PostgresKind {
    Postgres,
}

define_request_serializer_stuff!(EpKind::Postgres => PostgresRequest);
