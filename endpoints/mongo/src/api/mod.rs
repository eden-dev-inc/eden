// TODO: revisit once run_transaction_generic stubs are implemented with telemetry.
// #[named] is applied for future function_name!() use in telemetry spans.
#[allow(unused_macros)]
// Convention type aliases (OutputWrapper, ExpectedInput, etc.) are declared per
// operation module for consistency even when not yet referenced.
#[allow(dead_code)]
pub mod lib;
mod macros;
mod protocol;
pub mod wrapper;
