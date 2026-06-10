use crate::api::lib::PineconeApi;
use ep_core::{define_operation_types, implement_operation_registry};
use pinecone_core::{PineconeAsync, PineconeTx};

define_operation_types!();

implement_operation_registry!(PineconeOperation<PineconeAsync, PineconeApi, PineconeTx>);
