use std::{collections::HashMap, time::Instant};

use crossbeam_channel::{Receiver, Sender};
use serde::Serialize;

use eden_core::format::hashtype::TxHash;
use eden_logger_internal::{ctx_with_trace, log_debug};
use function_name::named;

/// RSA public key (DER-encoded) extracted from the license token at startup.
/// Wrapped in a newtype so it can be registered as distinct Actix app_data.
#[derive(Clone, Debug)]
pub struct LicenseRsaPublicKey(pub Option<Vec<u8>>);

impl LicenseRsaPublicKey {
    pub fn key_bytes(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }
}

#[derive(Default)]
pub struct TxQueue(HashMap<TxHash, Status>);

#[derive(Clone, Debug)]
pub enum Status {
    AwaitingResponse(Sender<Vec<u8>>),
    Queued(Instant),
    Response(String),
}

impl Serialize for Status {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Status::AwaitingResponse(_) => serializer.serialize_str("AwaitingResponse"),
            Status::Queued(_) => serializer.serialize_str("Queued"),
            Status::Response(r) => r.serialize(serializer),
        }
    }
}

impl TxQueue {
    pub fn status(&self, tx_hash: &TxHash) -> Option<Status> {
        self.0.get(tx_hash).cloned()
    }

    #[named]
    pub fn await_response(&mut self, tx_hash: &TxHash) -> Receiver<Vec<u8>> {
        let _ctx = ctx_with_trace!().with_feature("txqueue").with_additional("tx_hash", tx_hash.to_string());

        let (snd, rcv) = crossbeam_channel::bounded(1);
        self.0.insert(*tx_hash, Status::AwaitingResponse(snd));
        log_debug!(_ctx, "Transaction queued to await response", audience = eden_logger_internal::LogAudience::Internal);
        rcv
    }

    #[named]
    pub fn set_queued(&mut self, tx_hash: &TxHash) {
        let _ctx = ctx_with_trace!().with_feature("txqueue").with_additional("tx_hash", tx_hash.to_string());

        _ = self.0.insert(*tx_hash, Status::Queued(Instant::now()));
        log_debug!(
            _ctx,
            "Transaction queued for deferred response handling",
            audience = eden_logger_internal::LogAudience::Internal
        );
    }

    pub fn remove(&mut self, tx_hash: &TxHash) {
        self.0.remove(tx_hash);
    }

    pub fn set_response(&mut self, tx_hash: &TxHash, response: String) {
        self.0.insert(*tx_hash, Status::Response(response));
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
