use crate::api::lib::LlmApi;
use ep_core::{define_operation_types, implement_operation_registry};
use llm_core::{LlmAsync, LlmTx};

define_operation_types!();

implement_operation_registry!(LlmOperation<LlmAsync, LlmApi, LlmTx>);
