use ep_core::{EndpointOutput, EndpointResponse, EpOutput, ToOutput};
use error::{EpError, ProtocolError, ResultEP, SerdeError};
use format::endpoint::EpKind;
use mongodb::bson::{Bson, Document};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::gridfs::FilesCollectionDocument;
use mongodb::options::{ReadConcern, SelectionCriteria, WriteConcern};
use mongodb::results::{CollectionSpecification, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
use mongodb::{Client, ClientSession, Collection, Database, GridFsBucket, IndexModel, Namespace};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;
use utoipa::openapi::{Object, ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

pub trait OutputDowncast {
    fn downcast_ref<T: 'static>(&self) -> Option<&T>;
}

impl<T: EpOutput + ?Sized> OutputDowncast for T {
    fn downcast_ref<U: 'static>(&self) -> Option<&U> {
        self.as_any().downcast_ref::<U>()
    }
}

#[derive(ToSchema)]
#[allow(private_interfaces)]
pub enum MongoOutput {
    #[schema(title = "Mongo empty output")]
    MongoEmptyOutput(EmptyOutput),
    #[schema(title = "Mongo string output")]
    MongoStringOutput(StringOutput),
    #[schema(title = "Mongo database output")]
    MongoDatabaseOutput(DatabaseOutput),
    #[schema(title = "Mongo string array output")]
    MongoVecStringOutput(VecStringOutput),
    #[schema(title = "Mongo array of database specifications output")]
    MongoVecDatabaseSpecOutput(VecDatabaseSpecificationOutput),
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct EmptyOutput(pub ());

impl ToOutput for EmptyOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(Value::Null)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

pub struct DatabaseOutput(pub Database);

impl<'de> Deserialize<'de> for DatabaseOutput {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(Error::custom("cannot deserialize Database"))
    }
}

impl ToOutput for DatabaseOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::serde("serde serialize not implemented for Database"))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

impl ToSchema for DatabaseOutput {}
impl PartialSchema for DatabaseOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("client", Schema::Object(Object::default()))
                .property("name", String::schema())
                .property("selection_criteria", Schema::Object(Object::default()))
                .property("read_concern", Schema::Object(Object::default()))
                .property("write_concern", Schema::Object(Object::default()))
                .required("client")
                .required("name")
                .build(),
        ))
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct VecStringOutput(pub Vec<String>);

impl ToOutput for VecStringOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize)]
pub struct VecDatabaseSpecificationOutput(pub Vec<mongodb::results::DatabaseSpecification>);

impl ToOutput for VecDatabaseSpecificationOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for DatabaseSpecification"))
    }
}

impl ToSchema for VecDatabaseSpecificationOutput {}
impl PartialSchema for VecDatabaseSpecificationOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("name", String::schema())
                .property("size_on_disk", u64::schema())
                .property("empty", bool::schema())
                .property("shards", Schema::Object(Object::default()))
                .required("name")
                .required("size_on_disk")
                .required("empty")
                .build(),
        ))
    }
}

#[derive(Deserialize)]
pub(crate) struct ReadConcernOutput(pub Option<ReadConcern>);

impl ToOutput for ReadConcernOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for ReadConcern"))
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct SelectionCriteriaOutput(pub Option<SelectionCriteria>);

impl ToOutput for SelectionCriteriaOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::serde("serde serialize not implemented for SelectionCriteria"))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for SelectionCriteria"))
    }
}

#[allow(dead_code)]
pub(crate) struct ClientSessionOutput(pub ClientSession);

impl<'de> Deserialize<'de> for ClientSessionOutput {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(Error::custom("cannot deserialize ClientSession"))
    }
}

impl ToOutput for ClientSessionOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::serde("borsh serialize not implemented for ClientSession"))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for ClientSession"))
    }
}

#[allow(dead_code)]
pub(crate) struct ChangeStreamOutput(pub Mutex<ChangeStream<ChangeStreamEvent<Document>>>);

impl<'de> Deserialize<'de> for ChangeStreamOutput {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(Error::custom("cannot deserialize ChangeStream"))
    }
}

impl ToOutput for ChangeStreamOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::serde("serde serialize not implemented for ChangeStream"))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for ChangeStream"))
    }
}

#[derive(Deserialize, ToSchema)]
pub struct BoolOutput(pub bool);

impl ToOutput for BoolOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize)]
pub(crate) struct WriteConcernOutput(pub Option<WriteConcern>);

impl ToOutput for WriteConcernOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for WriteConcern"))
    }
}

#[derive(Deserialize)]
pub(crate) struct VecDocumentOutput(pub Vec<Document>);

impl ToOutput for VecDocumentOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Document"))
    }
}

#[allow(dead_code)]
pub(crate) struct CollectionDocumentOutput(pub Collection<Document>);

impl<'de> Deserialize<'de> for CollectionDocumentOutput {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(Error::custom("cannot deserialize Collection"))
    }
}

impl ToOutput for CollectionDocumentOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::serde("serde serialize not implemented for Collection"))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Collection"))
    }
}

#[allow(dead_code)]
pub(crate) struct GridfsBucketOutput(pub GridFsBucket);

impl<'de> Deserialize<'de> for GridfsBucketOutput {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(Error::custom("cannot deserialize GridFsBucket"))
    }
}

impl ToOutput for GridfsBucketOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::serde("serde serialize not implemented for GridFsBucket"))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for GridFsBucket"))
    }
}

#[derive(Deserialize)]
pub(crate) struct VecCollectionSpecificationOutput(pub Vec<CollectionSpecification>);

impl ToOutput for VecCollectionSpecificationOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for CollectionSpecification"))
    }
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct StringOutput(pub String);

impl ToOutput for StringOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize)]
pub(crate) struct DocumentOutput(pub Document);

impl ToOutput for DocumentOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Document"))
    }
}

#[derive(Deserialize)]
pub(crate) struct VecFilesCollectionDocumentOutput(pub Vec<FilesCollectionDocument>);

impl ToOutput for VecFilesCollectionDocumentOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Document"))
    }
}

#[allow(dead_code)]
pub(crate) struct ClientOutput(pub Client);

impl<'de> Deserialize<'de> for ClientOutput {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(Error::custom("cannot deserialize Client"))
    }
}

impl ToOutput for ClientOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Err(EpError::Serde(SerdeError::BorshNotImplemented))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::Serde(SerdeError::BorshNotImplemented))
    }
}

#[derive(Deserialize)]
pub(crate) struct U64Output(pub u64);

impl ToOutput for U64Output {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct DeleteResultOutput(pub DeleteResultWrapper);

#[derive(Deserialize, Serialize)]
pub(crate) struct DeleteResultWrapper {
    pub deleted_count: u64,
}

impl From<DeleteResult> for DeleteResultWrapper {
    fn from(delete: DeleteResult) -> Self {
        Self { deleted_count: delete.deleted_count }
    }
}

impl ToOutput for DeleteResultOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for DeleteResult"))
    }
}

#[derive(Deserialize)]
pub(crate) struct VecBsonOutput(pub Vec<Bson>);

impl ToOutput for VecBsonOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Bson"))
    }
}

#[derive(Deserialize)]
pub(crate) struct OptionDocumentOutput(pub Option<Document>);

impl ToOutput for OptionDocumentOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Document"))
    }
}

#[derive(Deserialize)]
pub(crate) struct InsertManyResultOutput(pub InsertManyResultWrapper);

#[derive(Deserialize, Serialize)]
pub(crate) struct InsertManyResultWrapper {
    pub inserted_ids: HashMap<usize, Bson>,
}

impl From<InsertManyResult> for InsertManyResultWrapper {
    fn from(value: InsertManyResult) -> Self {
        Self { inserted_ids: value.inserted_ids }
    }
}

impl ToOutput for InsertManyResultOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for InsertManyResult"))
    }
}

#[derive(Deserialize)]
pub(crate) struct InsertOneResultOutput(pub InsertOneResultWrapper);

#[derive(Deserialize, Serialize)]
pub(crate) struct InsertOneResultWrapper {
    pub inserted_id: Bson,
}

impl From<InsertOneResult> for InsertOneResultWrapper {
    fn from(value: InsertOneResult) -> Self {
        Self { inserted_id: value.inserted_id }
    }
}

impl ToOutput for InsertOneResultOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for InsertOneResult"))
    }
}

#[derive(Deserialize)]
pub(crate) struct VecIndexModelOutput(pub Vec<IndexModel>);

impl ToOutput for VecIndexModelOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for IndexModel"))
    }
}

#[derive(Deserialize)]
pub(crate) struct NamespaceOutput(pub Namespace);

impl ToOutput for NamespaceOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Namespace"))
    }
}

#[derive(Deserialize)]
pub(crate) struct UpdateResultOutput(pub UpdateResultWrapper);

#[derive(Deserialize, Serialize)]
pub struct UpdateResultWrapper {
    pub matched_count: u64,
    pub modified_count: u64,
    pub upserted_id: Option<Bson>,
}

impl From<UpdateResult> for UpdateResultWrapper {
    fn from(update: UpdateResult) -> Self {
        Self {
            matched_count: update.matched_count,
            modified_count: update.modified_count,
            upserted_id: update.upserted_id,
        }
    }
}

impl ToOutput for UpdateResultOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mongo, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for UpdateResult"))
    }
}
