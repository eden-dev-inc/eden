use crate::api::lib::PosthogApi;
use ep_core::{define_operation_types, implement_operation_registry};
use posthog_core::{PosthogAsync, PosthogTx};

define_operation_types!();

implement_operation_registry!(PosthogOperation<PosthogAsync, PosthogApi, PosthogTx>);
