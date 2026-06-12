use crate::api::lib::MongoApi;
use crate::{EpRequest, MongoOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use mongo_core::{MongoAsync, MongoTx};
use utoipa::openapi::{ArrayBuilder, Object, ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

define_request!(EpKind::Mongo => Mongo, MongoOperation, MongoAsync, MongoApi, MongoTx);

impl ToSchema for MongoRequest {}
impl PartialSchema for MongoRequest {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("kind", MongoKind::schema())
                .property("type", MongoType::schema())
                .property("database", String::schema())
                .property("collection", String::schema())
                .property("pipeline", Schema::Array(ArrayBuilder::new().items(Object::default()).build()))
                .property("options", Schema::Array(ArrayBuilder::new().items(Object::default()).build()))
                .required("kind")
                .required("type")
                .required("database")
                .build(),
        ))
    }
}

#[allow(dead_code)]
#[derive(ToSchema)]
enum MongoKind {
    Mongo,
}

#[allow(dead_code)]
#[derive(ToSchema)]
enum MongoType {
    #[schema(rename = "database_collection_aggregate")]
    DatabaseCollectionAggregate,
}

define_request_serializer_stuff!(EpKind::Mongo => MongoRequest);
