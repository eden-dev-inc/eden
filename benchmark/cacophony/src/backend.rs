use std::io;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const OK: &[u8] = b"+OK\r\n";
const PONG: &[u8] = b"+PONG\r\n";
const ERROR: &[u8] = b"-ERR unsupported synthetic command\r\n";

#[derive(Clone)]
pub struct RespBackendConfig {
    pub listen: String,
    pub payload_size: usize,
    pub payload_byte: u8,
}

pub async fn serve_resp_backend(config: RespBackendConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&config.listen).await?;
    let response = Arc::new(bulk_response(config.payload_size, config.payload_byte));

    eprintln!(
        "synthetic RESP backend: listening on {} payload_size={} response_wire_bytes={}",
        config.listen,
        config.payload_size,
        response.len()
    );

    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let response = response.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_connection(stream, response).await {
                eprintln!("synthetic RESP backend: connection {peer} closed: {e}");
            }
        });
    }
}

fn bulk_response(payload_size: usize, payload_byte: u8) -> Vec<u8> {
    let mut response = Vec::with_capacity(payload_size + 32);
    response.extend_from_slice(format!("${payload_size}\r\n").as_bytes());
    response.resize(response.len() + payload_size, payload_byte);
    response.extend_from_slice(b"\r\n");
    response
}

async fn serve_connection(stream: TcpStream, get_response: Arc<Vec<u8>>) -> io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = Vec::with_capacity(128);
    let mut arg = Vec::with_capacity(1024);

    loop {
        match read_command_name(&mut reader, &mut line, &mut arg).await? {
            Some(CommandName::Get) => write_half.write_all(&get_response).await?,
            Some(CommandName::Set) => write_half.write_all(OK).await?,
            Some(CommandName::Ping) => write_half.write_all(PONG).await?,
            Some(CommandName::Other) => write_half.write_all(ERROR).await?,
            None => return Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandName {
    Get,
    Set,
    Ping,
    Other,
}

async fn read_command_name<R>(reader: &mut BufReader<R>, line: &mut Vec<u8>, arg: &mut Vec<u8>) -> io::Result<Option<CommandName>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    line.clear();
    let n = reader.read_until(b'\n', line).await?;
    if n == 0 {
        return Ok(None);
    }

    if !line.starts_with(b"*") {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected RESP array"));
    }

    let argc = parse_line_usize(&line[1..])?;
    let mut command = CommandName::Other;

    for index in 0..argc {
        line.clear();
        reader.read_until(b'\n', line).await?;
        if !line.starts_with(b"$") {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected RESP bulk string"));
        }

        let len = parse_line_usize(&line[1..])?;
        arg.resize(len + 2, 0);
        reader.read_exact(arg.as_mut_slice()).await?;

        if index == 0 {
            let name = &arg[..len];
            command = if name.eq_ignore_ascii_case(b"GET") {
                CommandName::Get
            } else if name.eq_ignore_ascii_case(b"SET") {
                CommandName::Set
            } else if name.eq_ignore_ascii_case(b"PING") {
                CommandName::Ping
            } else {
                CommandName::Other
            };
        }
    }

    Ok(Some(command))
}

fn parse_line_usize(line: &[u8]) -> io::Result<usize> {
    let line = trim_crlf(line);
    if line.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty RESP integer"));
    }

    let mut value = 0usize;
    for &byte in line {
        if !byte.is_ascii_digit() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid RESP integer"));
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add((byte - b'0') as usize))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP integer overflow"))?;
    }
    Ok(value)
}

fn trim_crlf(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\r' | b'\n')) {
        line = &line[..line.len() - 1];
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parses_pipelined_commands() {
        let bytes = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
        let mut reader = BufReader::new(&bytes[..]);
        let mut line = Vec::new();
        let mut arg = Vec::new();

        assert_eq!(read_command_name(&mut reader, &mut line, &mut arg).await.unwrap(), Some(CommandName::Ping));
        assert_eq!(read_command_name(&mut reader, &mut line, &mut arg).await.unwrap(), Some(CommandName::Get));
        assert_eq!(read_command_name(&mut reader, &mut line, &mut arg).await.unwrap(), Some(CommandName::Set));
        assert_eq!(read_command_name(&mut reader, &mut line, &mut arg).await.unwrap(), None);
    }

    #[test]
    fn builds_bulk_response() {
        assert_eq!(bulk_response(3, b'x'), b"$3\r\nxxx\r\n");
    }
}
