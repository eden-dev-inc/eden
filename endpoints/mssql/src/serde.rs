use crate::api::lib::MssqlApi;
use ep_core::{define_operation_types, implement_operation_registry};
use mssql_core::{MssqlAsync, MssqlTx};

define_operation_types!();

implement_operation_registry!(MssqlOperation<MssqlAsync, MssqlApi, MssqlTx>);
