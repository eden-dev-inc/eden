use crate::api::lib::WeaviateApi;
use ep_core::{define_operation_types, implement_operation_registry};
use weaviate_core::{WeaviateAsync, WeaviateTx};

define_operation_types!();

implement_operation_registry!(WeaviateOperation<WeaviateAsync, WeaviateApi, WeaviateTx>);
