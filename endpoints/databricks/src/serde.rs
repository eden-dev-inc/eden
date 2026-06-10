use crate::api::lib::DatabricksApi;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{define_operation_types, implement_operation_registry};

define_operation_types!();

implement_operation_registry!(DatabricksOperation<DatabricksAsync, DatabricksApi, DatabricksTx>);
