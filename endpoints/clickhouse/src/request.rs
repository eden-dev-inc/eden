use crate::api::lib::ClickhouseApi;
use crate::{ClickhouseOperation, EpRequest, Operation};
use clickhouse_core::{ClickhouseAsync, ClickhouseTx};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

define_request!(EpKind::Clickhouse => Clickhouse, ClickhouseOperation, ClickhouseAsync, ClickhouseApi, ClickhouseTx);

impl ToSchema for ClickhouseRequest {}
impl PartialSchema for ClickhouseRequest {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("kind", ClichouseKind::schema())
                .property("type", ClickhouseType::schema())
                .property("body", String::schema())
                .required("kind")
                .required("type")
                .build(),
        ))
    }
}

#[allow(dead_code)]
#[derive(ToSchema)]
enum ClichouseKind {
    Clickhouse,
}

#[allow(dead_code)]
#[derive(ToSchema)]
enum ClickhouseType {
    #[schema(rename = "read")]
    Read,
}

define_request_serializer_stuff!(EpKind::Clickhouse => ClickhouseRequest);
