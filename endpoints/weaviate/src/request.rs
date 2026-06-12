use crate::api::lib::WeaviateApi;
use crate::{EpRequest, Operation, WeaviateOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use weaviate_core::{WeaviateAsync, WeaviateTx};

define_request!(EpKind::Weaviate => Weaviate, WeaviateOperation, WeaviateAsync, WeaviateApi, WeaviateTx);

define_request_serializer_stuff!(EpKind::Weaviate => WeaviateRequest);
