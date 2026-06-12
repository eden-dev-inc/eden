#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RedisPsyncHandler {
    replication_id: String,
    replication_offset: Arc<AtomicU64>,
}

impl RedisPsyncHandler {
    pub fn new() -> Self {
        Self {
            replication_id: generate_replication_id(),
            replication_offset: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Handle PSYNC command from replica.
    /// Returns a RESP response as Bytes (zero-copy friendly).
    pub fn handle_psync(&self, repl_id: Option<String>, offset: i64) -> Bytes {
        match (repl_id, offset) {
            (Some(id), off) if id == self.replication_id && off >= 0 => {
                // Partial resync
                let _current_offset = self.replication_offset.load(Ordering::SeqCst);
                Bytes::from(format!("+CONTINUE {}\r\n", self.replication_id))
            }
            _ => {
                // Full resync - send RDB then switch to replication stream
                let header = format!("+FULLRESYNC {} 0\r\n", self.replication_id);
                let rdb = self.generate_empty_rdb();

                let mut response = BytesMut::with_capacity(header.len() + rdb.len());
                response.extend_from_slice(header.as_bytes());
                response.extend_from_slice(&rdb);
                response.freeze()
            }
        }
    }

    /// Generate minimal empty RDB (Redis 9 format) as bulk string.
    fn generate_empty_rdb(&self) -> Bytes {
        // Minimal RDB data
        const RDB_DATA: &[u8] = &[
            // Magic string "REDIS"
            0x52, 0x45, 0x44, 0x49, 0x53, // Version "0009"
            0x30, 0x30, 0x30, 0x39, // EOF opcode
            0xFF, // CRC64 checksum (8 bytes zeros)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        // RDB must be sent as bulk string: $<length>\r\n<data>
        let prefix = format!("${}\r\n", RDB_DATA.len());
        let mut result = BytesMut::with_capacity(prefix.len() + RDB_DATA.len());
        result.extend_from_slice(prefix.as_bytes());
        result.extend_from_slice(RDB_DATA);
        result.freeze()
    }
}

fn generate_replication_id() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..40).map(|_| format!("{:x}", rng.random::<u8>() % 16)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoints::endpoint::ep_redis::api::{PsyncOutput, PsyncResponse};

    #[test]
    fn handle_psync_full_resync_for_unknown_replica() {
        let handler = RedisPsyncHandler::new();
        let response = handler.handle_psync(None, -1);

        let parsed = PsyncOutput::parse(&response).expect("parse FULLRESYNC");
        assert!(parsed.is_full_resync());
        // Full resync should carry an RDB payload after the header
        match parsed.response {
            PsyncResponse::FullResync { ref rdb_data, .. } => {
                assert!(!rdb_data.is_empty());
            }
            _ => panic!("expected full resync"),
        }
    }

    #[test]
    fn handle_psync_continue_for_matching_replica() {
        let handler = RedisPsyncHandler::new();
        let full_resync = handler.handle_psync(None, -1);
        let parsed = PsyncOutput::parse(&full_resync).expect("parse FULLRESYNC");
        let repl_id = parsed.replication_id().to_string();

        let continue_resp = handler.handle_psync(Some(repl_id.clone()), 0);
        let parsed_continue = PsyncOutput::parse(&continue_resp).expect("parse CONTINUE");

        assert_eq!(parsed_continue.replication_id(), repl_id);
        assert!(!parsed_continue.is_full_resync());
    }
}
