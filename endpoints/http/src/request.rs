use crate::EpRequest;
use crate::api::lib::HttpApi;
use crate::{HttpOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use http_core::{HttpAsync, HttpTx};

define_request!(EpKind::Http => Http, HttpOperation, HttpAsync, HttpApi, HttpTx);

define_request_serializer_stuff!(EpKind::Http => HttpRequest);
