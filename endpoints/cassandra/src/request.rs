use crate::EpRequest;
use ep_core::{define_request, define_request_serializer_stuff};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

use crate::{CassandraOperation, EpKind, Operation};

use super::api::lib::CassandraApi;
use cassandra_core::{CassandraAsync, CassandraTx};
pub use endpoint_types::request::EndpointRequestInput;

define_request!(EpKind::Cassandra => Cassandra, CassandraOperation, CassandraAsync, CassandraApi, CassandraTx);

impl ToSchema for CassandraRequest {}
impl PartialSchema for CassandraRequest {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("kind", String::schema())
                .property("type", String::schema())
                .property("database", String::schema())
                // todo!(),
                // .property("collection", String::schema())
                // .property(
                //     "pipeline",
                //     Schema::Array(ArrayBuilder::new().items(Object::default()).build()),
                // )
                // .property(
                //     "options",
                //     Schema::Array(ArrayBuilder::new().items(Object::default()).build()),
                // )
                .required("kind")
                .required("type")
                .required("database")
                .build(),
        ))
    }
}

define_request_serializer_stuff!(EpKind::Cassandra => CassandraRequest);
