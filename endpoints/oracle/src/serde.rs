use crate::api::lib::OracleApi;
use ep_core::{define_operation_types, implement_operation_registry};
use oracle_core::{OracleAsync, OracleTx};

define_operation_types!();

implement_operation_registry!(OracleOperation<OracleAsync, OracleApi, OracleTx>);
