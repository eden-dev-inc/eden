use crate::EpRequest;
use crate::api::lib::FunctionApi;
use crate::{FunctionOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use function_core::{FunctionAsync, FunctionTx};

define_request!(EpKind::Function => Function, FunctionOperation, FunctionAsync, FunctionApi, FunctionTx);

define_request_serializer_stuff!(EpKind::Function => FunctionRequest);
