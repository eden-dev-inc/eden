use crate::api::control_plane::RdsApi;
use ep_core::{define_operation_types, implement_operation_registry};
use postgres_core::{PostgresAsync, PostgresTx};

define_operation_types!();

implement_operation_registry!(RdsOperation<PostgresAsync, RdsApi, PostgresTx>);
