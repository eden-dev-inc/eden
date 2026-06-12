use crate::api::lib::MongoApi;
use ep_core::{define_operation_types, implement_operation_registry};
use mongo_core::{MongoAsync, MongoTx};

define_operation_types!();

implement_operation_registry!(MongoOperation<MongoAsync, MongoApi, MongoTx>);
