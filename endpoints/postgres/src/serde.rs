use crate::api::lib::PostgresApi;
use ep_core::{define_operation_types, implement_operation_registry};
use postgres_core::{PostgresAsync, PostgresTx};

define_operation_types!();

implement_operation_registry!(PostgresOperation<PostgresAsync, PostgresApi, PostgresTx>);
