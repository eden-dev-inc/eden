use crate::api::lib::CassandraApi;
use cassandra_core::{CassandraAsync, CassandraTx};
use ep_core::{define_operation_types, implement_operation_registry};

define_operation_types!();

implement_operation_registry!(CassandraOperation<CassandraAsync, CassandraApi, CassandraTx>);
