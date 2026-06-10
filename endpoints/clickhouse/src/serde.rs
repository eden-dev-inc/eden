use {
    clickhouse_core::{ClickhouseAsync, ClickhouseTx},
    ep_core::{define_operation_types, implement_operation_registry},
};

use crate::api::lib::ClickhouseApi;

define_operation_types!();

implement_operation_registry!(ClickhouseOperation<ClickhouseAsync, ClickhouseApi, ClickhouseTx>);
