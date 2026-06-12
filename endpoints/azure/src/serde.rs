use crate::api::lib::AzureApi;
use azure_core::{AzureAsync, AzureTx};
use ep_core::{define_operation_types, implement_operation_registry};

define_operation_types!();

implement_operation_registry!(AzureOperation<AzureAsync, AzureApi, AzureTx>);
