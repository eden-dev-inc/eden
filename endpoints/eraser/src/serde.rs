use crate::api::lib::EraserApi;
use ep_core::{define_operation_types, implement_operation_registry};
use eraser_core::{EraserAsync, EraserTx};

define_operation_types!();

implement_operation_registry!(EraserOperation<EraserAsync, EraserApi, EraserTx>);
