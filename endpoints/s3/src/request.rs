use crate::EpRequest;
use crate::api::lib::S3Api;
use crate::{Operation, S3Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use s3_core::{S3Async, S3Tx};

define_request!(EpKind::S3 => S3, S3Operation, S3Async, S3Api, S3Tx);

define_request_serializer_stuff!(EpKind::S3 => S3Request);
