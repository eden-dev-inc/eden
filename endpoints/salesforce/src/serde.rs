use crate::api::lib::SalesforceApi;
use ep_core::{define_operation_types, implement_operation_registry};
use salesforce_core::{SalesforceAsync, SalesforceTx};

define_operation_types!();

implement_operation_registry!(SalesforceOperation<SalesforceAsync, SalesforceApi, SalesforceTx>);
