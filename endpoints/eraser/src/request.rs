use crate::EpRequest;
use crate::api::lib::EraserApi;
use crate::{EraserOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use eraser_core::{EraserAsync, EraserTx};
use format::endpoint::EpKind;

define_request!(EpKind::Eraser => Eraser, EraserOperation, EraserAsync, EraserApi, EraserTx);

define_request_serializer_stuff!(EpKind::Eraser => EraserRequest);
