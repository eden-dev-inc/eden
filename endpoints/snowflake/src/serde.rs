use {
    ep_core::{define_operation_types, implement_operation_registry},
    snowflake_core::{SnowflakeAsync, SnowflakeTx},
};

use crate::api::lib::SnowflakeApi;

define_operation_types!();

implement_operation_registry!(SnowflakeOperation<SnowflakeAsync, SnowflakeApi, SnowflakeTx>);
