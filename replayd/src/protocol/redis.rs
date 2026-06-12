use super::Handshake;

pub struct RedisHandshake;

impl Handshake for RedisHandshake {
    fn parse_command<'a>(&self, buf: &'a [u8]) -> Option<(Vec<&'a [u8]>, usize)> {
        parse_command(buf)
    }

    fn is_handshake_verb(&self, verb: &[u8]) -> bool {
        matches!(verb, b"HELLO" | b"PING" | b"AUTH" | b"CLIENT" | b"COMMAND" | b"INFO" | b"CONFIG" | b"QUIT")
    }

    fn mock_response(&self, cmd: &[&[u8]]) -> (Vec<u8>, bool) {
        redis_response(cmd)
    }

    fn probe(buf: &[u8]) -> bool {
        if buf.is_empty() {
            return false;
        }
        // RESP commands start with '*'; inline commands are printable ASCII.
        buf[0] == b'*' || buf[0].is_ascii_alphabetic()
    }
}

/// Find `\r\n` starting from `start`. Returns index of `\r`.
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if buf.len() < start + 2 {
        return None;
    }
    buf[start..].windows(2).position(|w| w == b"\r\n").map(|i| start + i)
}

/// Parse one RESP array command from `buf`.
fn parse_resp_command(buf: &[u8]) -> Option<(Vec<&[u8]>, usize)> {
    if buf.is_empty() || buf[0] != b'*' {
        return None;
    }
    let mut pos = 1;

    let crlf = find_crlf(buf, pos)?;
    let count: usize = std::str::from_utf8(&buf[pos..crlf]).ok()?.parse().ok()?;
    pos = crlf + 2;

    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= buf.len() || buf[pos] != b'$' {
            return None;
        }
        pos += 1;

        let crlf = find_crlf(buf, pos)?;
        let len: usize = std::str::from_utf8(&buf[pos..crlf]).ok()?.parse().ok()?;
        pos = crlf + 2;

        if pos + len + 2 > buf.len() {
            return None;
        }
        args.push(&buf[pos..pos + len]);
        pos = pos + len + 2;
    }

    Some((args, pos))
}

/// Parse one inline command (plain text terminated by `\r\n`).
fn parse_inline_command(buf: &[u8]) -> Option<(Vec<&[u8]>, usize)> {
    let crlf = find_crlf(buf, 0)?;
    let line = &buf[..crlf];
    if line.is_empty() {
        return None;
    }
    let args: Vec<&[u8]> = line.split(|&b| b == b' ').filter(|s| !s.is_empty()).collect();
    if args.is_empty() {
        return None;
    }
    Some((args, crlf + 2))
}

/// Parse the next command from the buffer (RESP or inline).
fn parse_command(buf: &[u8]) -> Option<(Vec<&[u8]>, usize)> {
    if buf.is_empty() {
        return None;
    }
    if buf[0] == b'*' {
        parse_resp_command(buf)
    } else {
        parse_inline_command(buf)
    }
}

/// Generate a RESP response for a parsed Redis command.
fn redis_response(cmd: &[&[u8]]) -> (Vec<u8>, bool) {
    if cmd.is_empty() {
        return (b"+OK\r\n".to_vec(), false);
    }

    let verb: Vec<u8> = cmd[0].to_ascii_uppercase();
    match verb.as_slice() {
        b"PING" => (b"+PONG\r\n".to_vec(), false),
        b"HELLO" => {
            let proto_ver = cmd.get(1).and_then(|v| std::str::from_utf8(v).ok()).and_then(|v| v.parse::<u8>().ok()).unwrap_or(2);

            let resp = if proto_ver >= 3 {
                concat!(
                    "%7\r\n",
                    "$6\r\nserver\r\n",
                    "$5\r\nredis\r\n",
                    "$7\r\nversion\r\n",
                    "$5\r\n7.0.0\r\n",
                    "$5\r\nproto\r\n",
                    ":3\r\n",
                    "$2\r\nid\r\n",
                    ":1\r\n",
                    "$4\r\nmode\r\n",
                    "$10\r\nstandalone\r\n",
                    "$4\r\nrole\r\n",
                    "$6\r\nmaster\r\n",
                    "$7\r\nmodules\r\n",
                    "*0\r\n",
                )
            } else {
                concat!(
                    "*14\r\n",
                    "$6\r\nserver\r\n",
                    "$5\r\nredis\r\n",
                    "$7\r\nversion\r\n",
                    "$5\r\n7.0.0\r\n",
                    "$5\r\nproto\r\n",
                    ":2\r\n",
                    "$2\r\nid\r\n",
                    ":1\r\n",
                    "$4\r\nmode\r\n",
                    "$10\r\nstandalone\r\n",
                    "$4\r\nrole\r\n",
                    "$6\r\nmaster\r\n",
                    "$7\r\nmodules\r\n",
                    "*0\r\n",
                )
            };
            (resp.as_bytes().to_vec(), false)
        }
        b"AUTH" => (b"+OK\r\n".to_vec(), false),
        b"CLIENT" => (b"+OK\r\n".to_vec(), false),
        b"COMMAND" => {
            let sub = cmd.get(1).map(|s| s.to_ascii_uppercase());
            match sub.as_deref() {
                Some(b"COUNT") => (b":0\r\n".to_vec(), false),
                _ => (b"*0\r\n".to_vec(), false),
            }
        }
        b"INFO" => {
            let info = "# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\n";
            let resp = format!("${}\r\n{}\r\n", info.len(), info);
            (resp.into_bytes(), false)
        }
        b"CONFIG" => (b"*0\r\n".to_vec(), false),
        b"QUIT" => (b"+OK\r\n".to_vec(), true),
        _ => (b"+OK\r\n".to_vec(), false),
    }
}
