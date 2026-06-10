pub mod api;
pub mod auth;
pub mod eden_node;
pub mod endpoint;
pub mod endpoint_group;
mod interlay;
pub mod organization;
pub mod pipeline;
pub mod robot;
pub mod snapshot;
pub mod template;
pub mod user;
pub mod workflow;

use eden_core::error::EpError;
use ep_core::database::schema::{FromRow, Row};

pub(crate) fn decode_schema_row<T>(row: impl Into<Row>) -> Result<T, EpError>
where
    T: FromRow,
{
    let row = row.into();
    T::from_row(&row)
}
