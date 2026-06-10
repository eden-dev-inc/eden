use crate::EpRequest;
use crate::api::lib::AwsApi;
use crate::{AwsOperation, Operation};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;

define_request!(EpKind::Aws => Aws, AwsOperation, AwsAsync, AwsApi, AwsTx);

define_request_serializer_stuff!(EpKind::Aws => AwsRequest);
