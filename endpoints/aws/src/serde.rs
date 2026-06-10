use crate::api::lib::AwsApi;
use aws_core::{AwsAsync, AwsTx};
use ep_core::{define_operation_types, implement_operation_registry};

define_operation_types!();

implement_operation_registry!(AwsOperation<AwsAsync, AwsApi, AwsTx>);
