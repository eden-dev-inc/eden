mod redis;

pub use redis::RedisHandshake;

/// Protocol-specific handshake handler.
///
/// Implementations know how to parse commands from a byte buffer,
/// identify handshake verbs, and generate mock responses.
pub trait Handshake: Send {
    /// Try to parse one command from `buf`.
    /// Returns `(args, bytes_consumed)` or `None` if the buffer is incomplete.
    fn parse_command<'a>(&self, buf: &'a [u8]) -> Option<(Vec<&'a [u8]>, usize)>;

    /// Returns true if `verb` (already uppercased) is a handshake/admin command
    /// that should be answered with a mock response rather than PCAP data.
    fn is_handshake_verb(&self, verb: &[u8]) -> bool;

    /// Generate a mock response for `cmd`.
    /// Returns `(response_bytes, should_close)`.
    fn mock_response(&self, cmd: &[&[u8]]) -> (Vec<u8>, bool);

    /// Quick probe: return true if the first bytes of `buf` look like this
    /// protocol. Used for auto-detection.
    fn probe(buf: &[u8]) -> bool
    where
        Self: Sized;
}

/// Known protocol types.
#[derive(Clone, Copy, Debug)]
pub enum Protocol {
    Redis,
}

/// Try to detect the protocol from the first bytes of a connection.
pub fn detect_protocol(buf: &[u8]) -> Option<Protocol> {
    if RedisHandshake::probe(buf) {
        return Some(Protocol::Redis);
    }
    // Future: add Mongo, Postgres probes here.
    None
}

/// Return a boxed Handshake implementation for the given protocol.
pub fn handshake_for(proto: &Protocol) -> Box<dyn Handshake> {
    match proto {
        Protocol::Redis => Box::new(RedisHandshake),
    }
}
