use std::io::{self, Read};
use std::net::TcpStream;
use std::time::Duration;

pub fn hexdump(data: &[u8]) {
    for (i, chunk) in data.chunks(16).enumerate() {
        eprint!("{:08x}  ", i * 16);
        for (j, b) in chunk.iter().enumerate() {
            if j == 8 {
                eprint!(" ");
            }
            eprint!("{b:02x} ");
        }
        for j in chunk.len()..16 {
            if j == 8 {
                eprint!(" ");
            }
            eprint!("   ");
        }
        eprint!(" |");
        for b in chunk {
            let c = *b as char;
            if b.is_ascii_graphic() || *b == b' ' {
                eprint!("{c}");
            } else {
                eprint!(".");
            }
        }
        eprintln!("|");
    }
}

pub fn hexdump_region(data: &[u8], offset: usize) {
    let start = offset.saturating_sub(16) & !0xf;
    let end = (offset + 16).min(data.len());
    if start < end {
        hexdump(&data[start..end]);
    }
}

pub fn first_diff(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).position(|(x, y)| x != y).unwrap_or(a.len().min(b.len()))
}

pub fn read_n(r: &mut impl Read, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Like `read_n` but returns `None` on clean EOF (no bytes read).
pub fn read_or_eof(r: &mut impl Read, n: usize) -> io::Result<Option<Vec<u8>>> {
    let mut buf = vec![0u8; n];
    if r.read(&mut buf[..1])? == 0 {
        return Ok(None);
    }
    if n > 1 {
        r.read_exact(&mut buf[1..])?;
    }
    Ok(Some(buf))
}

/// Read exactly `n` bytes from a TcpStream with a deadline.
pub fn read_exact_timeout(stream: &mut TcpStream, n: usize, timeout: Duration) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    let mut filled = 0;
    let deadline = std::time::Instant::now() + timeout;

    while filled < n {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(io::ErrorKind::TimedOut, format!("timeout after reading {filled}/{n} bytes")));
        }
        stream.set_read_timeout(Some(remaining))?;
        match stream.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, format!("EOF after reading {filled}/{n} bytes")));
            }
            Ok(got) => filled += got,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                return Err(io::Error::new(io::ErrorKind::TimedOut, format!("timeout after reading {filled}/{n} bytes")));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(buf)
}
