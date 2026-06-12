use crate::EpRequest;
use crate::api::lib::LlmApi;
use crate::{LlmOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use llm_core::{LlmAsync, LlmTx};

define_request!(EpKind::Llm => Llm, LlmOperation, LlmAsync, LlmApi, LlmTx);

define_request_serializer_stuff!(EpKind::Llm => LlmRequest);
