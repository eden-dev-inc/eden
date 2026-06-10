use crate::api::lib::S3Api;
use ep_core::{define_operation_types, implement_operation_registry};
use s3_core::{S3Async, S3Tx};

define_operation_types!();

implement_operation_registry!(S3Operation<S3Async, S3Api, S3Tx>);
