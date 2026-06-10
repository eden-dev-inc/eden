use crate::api::lib::SnowflakeApi;
use crate::{EpRequest, Operation, SnowflakeOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use snowflake_core::{SnowflakeAsync, SnowflakeTx};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

define_request!(EpKind::Snowflake => Snowflake, SnowflakeOperation, SnowflakeAsync, SnowflakeApi, SnowflakeTx);

impl ToSchema for SnowflakeRequest {}
impl PartialSchema for SnowflakeRequest {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("kind", SnowflakeKind::schema())
                .property("type", SnowflakeType::schema())
                .property("body", String::schema())
                .required("kind")
                .required("type")
                .build(),
        ))
    }
}

#[derive(ToSchema)]
#[allow(dead_code)]
enum SnowflakeKind {
    Snowflake,
}

#[derive(ToSchema)]
#[allow(dead_code)]
enum SnowflakeType {
    #[schema(rename = "read")]
    Read,
}

define_request_serializer_stuff!(EpKind::Snowflake => SnowflakeRequest);
