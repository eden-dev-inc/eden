use crate::api::lib::MysqlApi;
use ep_core::{define_operation_types, implement_operation_registry};
use mysql_core::{MysqlAsync, MysqlTx};

define_operation_types!();

implement_operation_registry!(MysqlOperation<MysqlAsync, MysqlApi, MysqlTx>);
