#![allow(dead_code)]

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::UnboundedSender;

pub struct AofStreamer {
    offset: Arc<AtomicU64>,
    sender: UnboundedSender<Bytes>,
}

impl AofStreamer {
    pub fn new(sender: UnboundedSender<Bytes>) -> Self {
        Self { offset: Arc::new(AtomicU64::new(0)), sender }
    }

    /// Stream write command to replica.
    /// Accepts Bytes to avoid allocation when the caller already has Bytes.
    pub fn stream_command(&self, command: Bytes) -> Result<(), String> {
        // Track offset (byte count) before sending
        let len = command.len();

        // Commands already in RESP format, just send
        self.sender.send(command).map_err(|_| "Failed to send to replica".to_string())?;

        self.offset.fetch_add(len as u64, Ordering::SeqCst);
        Ok(())
    }

    /// Handle REPLCONF GETACK from replica.
    pub fn handle_getack(&self) -> Bytes {
        let offset = self.offset.load(Ordering::SeqCst);
        Bytes::from(format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", offset.to_string().len(), offset))
    }

    pub fn get_offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}
