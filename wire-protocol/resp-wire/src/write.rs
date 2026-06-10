//! RESP protocol encoding helpers.

use std::io::{self, Write};

/// Write a RESP bulk string: $<len>\r\n<data>\r\n
#[inline]
pub fn write_bulk_string(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    write!(w, "${}\r\n", data.len())?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

/// Write a RESP array header: *<count>\r\n
#[inline]
pub fn write_array_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "*{}\r\n", count)
}

/// Write a RESP integer: :<value>\r\n
#[inline]
pub fn write_integer(w: &mut impl Write, value: i64) -> io::Result<()> {
    write!(w, ":{}\r\n", value)
}

/// Write a RESP simple string: +<data>\r\n
#[inline]
pub fn write_simple_string(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(b"+")?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

/// Write a RESP simple error: -<data>\r\n
#[inline]
pub fn write_simple_error(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(b"-")?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

/// Write RESP2 null bulk string: $-1\r\n
#[inline]
pub fn write_null_bulk(w: &mut impl Write) -> io::Result<()> {
    w.write_all(b"$-1\r\n")
}

/// Write RESP2 null array: *-1\r\n
#[inline]
pub fn write_null_array(w: &mut impl Write) -> io::Result<()> {
    w.write_all(b"*-1\r\n")
}

// ============================================================================
// RESP3 types
// ============================================================================

/// Write RESP3 null: _\r\n
#[inline]
pub fn write_null(w: &mut impl Write) -> io::Result<()> {
    w.write_all(b"_\r\n")
}

/// Write RESP3 boolean: #t\r\n or #f\r\n
#[inline]
pub fn write_boolean(w: &mut impl Write, value: bool) -> io::Result<()> {
    w.write_all(if value { b"#t\r\n" } else { b"#f\r\n" })
}

/// Write RESP3 double: ,<value>\r\n
#[inline]
pub fn write_double(w: &mut impl Write, value: f64) -> io::Result<()> {
    // Handle special cases
    if value.is_infinite() {
        if value.is_sign_positive() {
            w.write_all(b",inf\r\n")
        } else {
            w.write_all(b",-inf\r\n")
        }
    } else if value.is_nan() {
        w.write_all(b",nan\r\n")
    } else {
        write!(w, ",{}\r\n", value)
    }
}

/// Write RESP3 big number: (<data>\r\n
#[inline]
pub fn write_big_number(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(b"(")?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

/// Write RESP3 blob error: !<len>\r\n<data>\r\n
#[inline]
pub fn write_blob_error(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    write!(w, "!{}\r\n", data.len())?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

/// Write RESP3 verbatim string: =<len>\r\n<format>:<data>\r\n
#[inline]
pub fn write_verbatim_string(w: &mut impl Write, format: &[u8; 3], data: &[u8]) -> io::Result<()> {
    write!(w, "={}\r\n", data.len() + 4)?; // +4 for "fmt:"
    w.write_all(format)?;
    w.write_all(b":")?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

/// Write RESP3 map header: %<count>\r\n
#[inline]
pub fn write_map_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "%{}\r\n", count)
}

/// Write RESP3 set header: ~<count>\r\n
#[inline]
pub fn write_set_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "~{}\r\n", count)
}

/// Write RESP3 push header: ><count>\r\n
#[inline]
pub fn write_push_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, ">{}\r\n", count)
}

/// Write RESP3 attribute header: |<count>\r\n
#[inline]
pub fn write_attribute_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "|{}\r\n", count)
}

// ============================================================================
// Command builder helper
// ============================================================================

/// Builder for constructing RESP commands.
pub struct CommandBuilder {
    buf: Vec<u8>,
    arg_count: usize,
}

impl CommandBuilder {
    /// Create a new command builder with the given command name.
    pub fn new(command: &str) -> Self {
        let mut builder = Self { buf: Vec::with_capacity(64), arg_count: 1 };
        // Reserve space for array header (will be written at finish)
        builder.buf.extend_from_slice(b"*000000000\r\n");
        // Write command
        write_bulk_string(&mut builder.buf, command.as_bytes()).expect("Write Error");
        builder
    }

    /// Add a bulk string argument.
    #[inline]
    pub fn arg_bulk(mut self, data: &[u8]) -> Self {
        write_bulk_string(&mut self.buf, data).expect("Write Error");
        self.arg_count += 1;
        self
    }

    /// Add a string argument (convenience for &str).
    #[inline]
    pub fn arg(self, s: &str) -> Self {
        self.arg_bulk(s.as_bytes())
    }

    /// Add an integer argument.
    #[inline]
    pub fn arg_int(mut self, value: i64) -> Self {
        write_integer(&mut self.buf, value).expect("Write Error");
        self.arg_count += 1;
        self
    }

    /// Finish building and return the command bytes.
    pub fn build(mut self) -> Vec<u8> {
        // Write actual array header at start
        let header = format!("*{}\r\n", self.arg_count);
        let header_bytes = header.as_bytes();

        // If header fits in reserved space, overwrite; otherwise rebuild
        if header_bytes.len() <= 12 {
            self.buf[..header_bytes.len()].copy_from_slice(header_bytes);
            // Shift content if header is shorter than reserved
            if header_bytes.len() < 12 {
                let remaining = self.buf[12..].to_vec();
                self.buf.truncate(header_bytes.len());
                self.buf.extend_from_slice(&remaining);
            }
        } else {
            // Very large arg count - rebuild (rare)
            let mut new_buf = Vec::with_capacity(self.buf.len() + header_bytes.len());
            new_buf.extend_from_slice(header_bytes);
            new_buf.extend_from_slice(&self.buf[12..]);
            self.buf = new_buf;
        }

        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_bulk_string() {
        let mut buf = Vec::new();
        write_bulk_string(&mut buf, b"hello").expect("Write Error");
        assert_eq!(buf, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_write_empty_bulk_string() {
        let mut buf = Vec::new();
        write_bulk_string(&mut buf, b"").expect("Write Error");
        assert_eq!(buf, b"$0\r\n\r\n");
    }

    #[test]
    fn test_write_array_header() {
        let mut buf = Vec::new();
        write_array_header(&mut buf, 3).expect("Write Error");
        assert_eq!(buf, b"*3\r\n");
    }

    #[test]
    fn test_write_integer() {
        let mut buf = Vec::new();
        write_integer(&mut buf, 42).expect("Write Error");
        assert_eq!(buf, b":42\r\n");

        let mut buf = Vec::new();
        write_integer(&mut buf, -123).expect("Write Error");
        assert_eq!(buf, b":-123\r\n");
    }

    #[test]
    fn test_write_simple_string() {
        let mut buf = Vec::new();
        write_simple_string(&mut buf, b"OK").expect("Write Error");
        assert_eq!(buf, b"+OK\r\n");
    }

    #[test]
    fn test_write_simple_error() {
        let mut buf = Vec::new();
        write_simple_error(&mut buf, b"ERR unknown command").expect("Write Error");
        assert_eq!(buf, b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_write_null_bulk() {
        let mut buf = Vec::new();
        write_null_bulk(&mut buf).expect("Write Error");
        assert_eq!(buf, b"$-1\r\n");
    }

    #[test]
    fn test_write_null() {
        let mut buf = Vec::new();
        write_null(&mut buf).expect("Write Error");
        assert_eq!(buf, b"_\r\n");
    }

    #[test]
    fn test_write_boolean() {
        let mut buf = Vec::new();
        write_boolean(&mut buf, true).expect("Write Error");
        assert_eq!(buf, b"#t\r\n");

        let mut buf = Vec::new();
        write_boolean(&mut buf, false).expect("Write Error");
        assert_eq!(buf, b"#f\r\n");
    }

    #[test]
    fn test_write_double() {
        let mut buf = Vec::new();
        write_double(&mut buf, 1.23).expect("Write Error");
        assert_eq!(buf, b",1.23\r\n");

        let mut buf = Vec::new();
        write_double(&mut buf, f64::INFINITY).expect("Write Error");
        assert_eq!(buf, b",inf\r\n");

        let mut buf = Vec::new();
        write_double(&mut buf, f64::NEG_INFINITY).expect("Write Error");
        assert_eq!(buf, b",-inf\r\n");
    }

    #[test]
    fn test_write_map_header() {
        let mut buf = Vec::new();
        write_map_header(&mut buf, 2).expect("Write Error");
        assert_eq!(buf, b"%2\r\n");
    }

    #[test]
    fn test_write_verbatim_string() {
        let mut buf = Vec::new();
        write_verbatim_string(&mut buf, b"txt", b"hello").expect("Write Error");
        assert_eq!(buf, b"=9\r\ntxt:hello\r\n");
    }

    #[test]
    fn test_command_builder() {
        let cmd = CommandBuilder::new("SET").arg("mykey").arg("myvalue").build();
        assert_eq!(cmd.to_vec(), b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
    }

    #[test]
    fn test_command_builder_get() {
        let cmd = CommandBuilder::new("GET").arg("mykey").build();
        assert_eq!(cmd.to_vec(), b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
    }

    #[test]
    fn test_command_builder_ping() {
        let cmd = CommandBuilder::new("PING").build();
        assert_eq!(cmd.to_vec(), b"*1\r\n$4\r\nPING\r\n");
    }
}
