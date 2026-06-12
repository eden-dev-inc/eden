use crate::api::lib::FunctionApi;
use ep_core::{define_operation_types, implement_operation_registry};
use function_core::{FunctionAsync, FunctionTx};

define_operation_types!();

implement_operation_registry!(FunctionOperation<FunctionAsync, FunctionApi, FunctionTx>);
