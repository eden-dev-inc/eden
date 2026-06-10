//! A minimal, zero-overhead `WireRead` implementation for `&[u8]`.
//!
//! Unlike streaming implementations which use `BorrowTracker` for safety,
//! `SliceStream` is designed for the common case where you have a complete
//! buffer and parse synchronously. It avoids all allocation overhead.

use crate::read::{WireRead, WireReadSync};
use std::cell::Cell;
use std::marker::PhantomData;

/// A lightweight `WireRead` implementation over a byte slice.
///
/// This is optimized for the case where:
/// - You have a complete message in a buffer
/// - You parse synchronously (no async yields while borrowing)
/// - You want zero allocation overhead
///
/// # Example
///
/// ```rust
/// use wire_stream::{SliceStream, WireReadSync, WireReadSyncExt};
///
/// let data = b"+OK\r\n";
/// let stream = SliceStream::new(data);
///
/// // Check first byte
/// assert!(stream.expect_byte_sync(b'+').unwrap().is_ok());
///
/// // Read until CRLF
/// let line = stream.read_to_crlf_sync(None).unwrap().unwrap();
/// assert_eq!(&*line, b"OK");
/// ```
pub struct SliceStream<'buf> {
    buffer: &'buf [u8],
    cursor: Cell<usize>,
}

impl<'buf> SliceStream<'buf> {
    #[inline]
    pub fn new(buffer: &'buf [u8]) -> Self {
        Self { buffer, cursor: Cell::new(0) }
    }

    /// Returns the number of bytes consumed so far.
    #[inline]
    pub fn consumed(&self) -> usize {
        self.cursor.get()
    }

    /// Returns the remaining unread bytes.
    #[inline]
    pub fn remaining(&self) -> &'buf [u8] {
        &self.buffer[self.cursor.get()..]
    }

    /// Returns true if all bytes have been consumed.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cursor.get() >= self.buffer.len()
    }

    /// Returns the total length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

#[derive(Clone, Eq, PartialEq, Debug, thiserror::Error)]
pub enum SliceReadError {
    #[error("invalid cursor for offset_from")]
    InvalidCursor,

    #[error("not enough data")]
    NotEnoughData,

    #[error("invalid subslice range")]
    InvalidSubslice,

    #[error("invalid accept position")]
    InvalidAccept,

    #[error("invalid unaccept position")]
    InvalidUnaccept,
}

/// A cursor position in the stream. Just wraps an offset.
#[derive(Clone, Debug)]
pub struct SliceCursor<'a> {
    position: usize,
    _phantom: PhantomData<&'a ()>,
}

/// A borrowed slice from the stream.
#[derive(Clone, Debug)]
pub struct SliceBorrow<'a> {
    start: usize,
    data: &'a [u8],
}

impl<'a> std::ops::Deref for SliceBorrow<'a> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

/// A borrowed slice with a compile-time known length.
#[derive(Clone, Debug)]
pub struct SliceBorrowConst<'a, const N: usize> {
    start: usize,
    data: &'a [u8; N],
}

impl<'a, const N: usize> std::ops::Deref for SliceBorrowConst<'a, N> {
    type Target = [u8; N];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'buf> WireReadSync for SliceStream<'buf> {
    type ReadError = SliceReadError;
    type ReadCursor<'a>
        = SliceCursor<'a>
    where
        Self: 'a;
    type ReadBorrow<'a>
        = SliceBorrow<'a>
    where
        Self: 'a;
    type ReadBorrowConst<'a, const N: usize>
        = SliceBorrowConst<'a, N>
    where
        Self: 'a;

    #[inline]
    fn position<'a>(&'a self) -> Self::ReadCursor<'a> {
        SliceCursor { position: self.cursor.get(), _phantom: PhantomData }
    }

    #[inline]
    fn offset_from<'a>(&'a self, base: &'_ Self::ReadCursor<'a>) -> Result<usize, Self::ReadError> {
        self.cursor.get().checked_sub(base.position).ok_or(SliceReadError::InvalidCursor)
    }

    #[inline]
    fn restore_to<'a>(&'a self, cursor: &'_ Self::ReadCursor<'a>) -> Result<(), Self::ReadError> {
        if cursor.position <= self.buffer.len() {
            self.cursor.set(cursor.position);
            Ok(())
        } else {
            Err(SliceReadError::InvalidCursor)
        }
    }

    #[inline]
    fn advance_by(&self, distance: usize) -> Result<(), Self::ReadError> {
        let new_pos = self.cursor.get().checked_add(distance).ok_or(SliceReadError::NotEnoughData)?;
        if new_pos <= self.buffer.len() {
            self.cursor.set(new_pos);
            Ok(())
        } else {
            Err(SliceReadError::NotEnoughData)
        }
    }

    #[inline]
    fn peek<'a>(&'a self, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let pos = self.cursor.get();
        let rest = &self.buffer[pos..];
        let data = match limit {
            Some(n) if n < rest.len() => &rest[..n],
            _ => rest,
        };
        Ok(SliceBorrow { start: pos, data })
    }

    #[inline]
    fn peek_exactly<'a, const N: usize>(&'a self) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        let pos = self.cursor.get();
        let rest = &self.buffer[pos..];
        let arr: &[u8; N] = rest.get(..N).and_then(|s| s.try_into().ok()).ok_or(SliceReadError::NotEnoughData)?;
        Ok(SliceBorrowConst { start: pos, data: arr })
    }

    #[inline]
    fn upcast<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        Ok(SliceBorrow { start: borrow.start, data: borrow.data.as_slice() })
    }

    #[inline]
    fn subslice<'a>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let slice_start = start.unwrap_or(0);
        let slice_end = end.unwrap_or(borrow.data.len());

        if slice_start > slice_end || slice_end > borrow.data.len() {
            return Err(SliceReadError::InvalidSubslice);
        }

        Ok(SliceBorrow {
            start: borrow.start + slice_start,
            data: &borrow.data[slice_start..slice_end],
        })
    }

    #[inline]
    fn subslice_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        index: Option<usize>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        let idx = index.unwrap_or(0);
        let arr: &[u8; N] = borrow.data.get(idx..idx + N).and_then(|s| s.try_into().ok()).ok_or(SliceReadError::InvalidSubslice)?;
        Ok(SliceBorrowConst { start: borrow.start + idx, data: arr })
    }

    #[inline]
    fn extend<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let available = &self.buffer[borrow.start..];
        let data = match limit {
            Some(n) if n < available.len() => &available[..n],
            _ => available,
        };
        Ok(SliceBorrow { start: borrow.start, data })
    }

    #[inline]
    fn extend_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        let available = &self.buffer[borrow.start..];
        let arr: &[u8; N] = available.get(..N).and_then(|s| s.try_into().ok()).ok_or(SliceReadError::NotEnoughData)?;
        Ok(SliceBorrowConst { start: borrow.start, data: arr })
    }

    #[inline]
    fn accept<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let borrow_end = borrow.start + borrow.data.len();
        let pos = self.cursor.get();

        if pos < borrow.start || pos > borrow_end {
            return Err(SliceReadError::InvalidAccept);
        }

        let new_pos = match limit {
            Some(n) if borrow.start + n < borrow_end => borrow.start + n,
            _ => borrow_end,
        };

        if new_pos > pos {
            self.cursor.set(new_pos);
        }

        let consumed_len = new_pos - borrow.start;
        Ok(SliceBorrow {
            start: borrow.start,
            data: &borrow.data[..consumed_len.min(borrow.data.len())],
        })
    }

    #[inline]
    fn accept_exactly<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<(), Self::ReadError> {
        let borrow_end = borrow.start + N;
        let pos = self.cursor.get();

        if pos < borrow.start || pos > borrow_end {
            return Err(SliceReadError::InvalidAccept);
        }

        if borrow_end > pos {
            self.cursor.set(borrow_end);
            Ok(())
        } else {
            Err(SliceReadError::InvalidAccept)
        }
    }

    #[inline]
    fn unaccept<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let borrow_end = borrow.start + borrow.data.len();
        let pos = self.cursor.get();

        if pos < borrow.start || pos > borrow_end {
            return Err(SliceReadError::InvalidUnaccept);
        }

        let new_pos = borrow.start + limit.unwrap_or(0);
        if new_pos <= pos {
            self.cursor.set(new_pos);
            let kept_len = new_pos - borrow.start;
            Ok(SliceBorrow { start: borrow.start, data: &borrow.data[..kept_len] })
        } else {
            Err(SliceReadError::InvalidUnaccept)
        }
    }

    #[inline]
    fn unaccept_exactly<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<(), Self::ReadError> {
        let borrow_end = borrow.start + N;
        let pos = self.cursor.get();

        if pos == borrow_end {
            self.cursor.set(borrow.start);
            Ok(())
        } else {
            Err(SliceReadError::InvalidUnaccept)
        }
    }
}

/// Async implementation - for SliceStream, just delegates to sync since all data is available.
#[allow(clippy::needless_lifetimes)]
impl<'buf> WireRead for SliceStream<'buf> {
    #[inline]
    async fn peek_read<'a>(&'a self, min: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let borrow = self.peek(None)?;
        if let Some(min) = min
            && borrow.data.len() < min
        {
            return Err(SliceReadError::NotEnoughData);
        }
        Ok(borrow)
    }

    #[inline]
    async fn peek_read_exactly<'a, const N: usize>(&'a self) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        self.peek_exactly()
    }

    #[inline]
    async fn extend_read<'a>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        min: Option<usize>,
    ) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let extended = self.extend(borrow, None)?;
        if let Some(min) = min
            && extended.data.len() < min
        {
            return Err(SliceReadError::NotEnoughData);
        }
        Ok(extended)
    }

    #[inline]
    async fn extend_read_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        self.extend_exactly(borrow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireReadSyncExt;

    #[test]
    fn test_basic_operations() {
        let data = b"+OK\r\n";
        let stream = SliceStream::new(data);

        // Peek
        let borrow = stream.peek(None).expect("Should be able to peek data");
        assert_eq!(&*borrow, b"+OK\r\n");

        // Cursor hasn't moved
        assert_eq!(stream.consumed(), 0);

        // Accept some bytes
        stream.accept(&borrow, Some(3)).expect("should accept");
        assert_eq!(stream.consumed(), 3);

        // Remaining
        assert_eq!(stream.remaining(), b"\r\n");
    }

    #[test]
    fn test_peek_exactly() {
        let data = b":42\r\n";
        let stream = SliceStream::new(data);

        let tag: SliceBorrowConst<'_, 1> = stream.peek_exactly().expect("should peek exactly");
        assert_eq!(*tag, [b':']);

        stream.accept_exactly(&tag).expect("should accept");
        assert_eq!(stream.consumed(), 1);
    }

    #[test]
    fn test_read_to_crlf_sync() {
        let data = b"+Hello\r\n";
        let stream = SliceStream::new(data);

        // Skip the tag
        stream.advance_by(1).expect("should advance");

        let result = stream.read_to_crlf_sync(None).expect("should read");
        assert!(result.is_ok());
        let line = result.expect("should read");
        assert_eq!(&*line, b"Hello");

        // Stream should be fully consumed
        assert_eq!(stream.consumed(), data.len());
    }

    #[test]
    fn test_expect_byte_sync() {
        let data = b"+OK\r\n";
        let stream = SliceStream::new(data);

        let result = stream.expect_byte_sync(b'+').expect("should read");
        assert!(result.is_ok());
        assert_eq!(stream.consumed(), 1);

        let result = stream.expect_byte_sync(b'-').expect("should read");
        assert!(result.is_err());
    }

    #[test]
    fn test_read_integers() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let stream = SliceStream::new(&data);

        let value = stream.read_i32_le_sync().expect("should read");
        assert_eq!(value, 0x04030201);

        let value = stream.read_i32_le_sync().expect("should read");
        assert_eq!(value, 0x08070605);
    }

    #[test]
    fn test_read_cstring() {
        let data = b"hello\0world";
        let stream = SliceStream::new(data);

        let result = stream.read_cstring_sync().expect("should read");
        assert!(result.is_ok());
        let s = result.expect("should read");
        assert_eq!(&*s, b"hello");
        assert_eq!(stream.consumed(), 6); // "hello" + null byte
    }
}
