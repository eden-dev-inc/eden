use crate::api::lib::HttpApi;
use ep_core::{define_operation_types, implement_operation_registry};
use http_core::{HttpAsync, HttpTx};

define_operation_types!();

implement_operation_registry!(HttpOperation<HttpAsync, HttpApi, HttpTx>);
