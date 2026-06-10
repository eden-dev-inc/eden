use crate::api::lib::DatadogApi;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{define_operation_types, implement_operation_registry};

define_operation_types!();

implement_operation_registry!(DatadogOperation<DatadogAsync, DatadogApi, DatadogTx>);
