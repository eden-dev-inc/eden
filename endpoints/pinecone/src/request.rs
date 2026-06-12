use crate::api::lib::PineconeApi;
use crate::{EpRequest, Operation, PineconeOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use pinecone_core::{PineconeAsync, PineconeTx};

define_request!(EpKind::Pinecone => Pinecone, PineconeOperation, PineconeAsync, PineconeApi, PineconeTx);

define_request_serializer_stuff!(EpKind::Pinecone => PineconeRequest);
