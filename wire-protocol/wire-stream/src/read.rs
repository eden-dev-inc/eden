//! Sync-first wire stream reading traits.
//!
//! This module provides both synchronous and asynchronous traits for reading
//! wire protocol data. The sync trait is optimized for parsing complete buffers,
//! while the async trait extends it for streaming I/O.

use std::ops::Deref;

/// Find CRLF in a byte slice using SIMD-optimized memchr.
/// Returns the index of '\r' if "\r\n" is found, None otherwise.
#[inline]
pub fn find_crlf(data: &[u8]) -> Option<usize> {
    let mut start = 0;
    while let Some(pos) = memchr::memchr(b'\r', &data[start..]) {
        let abs_pos = start + pos;
        if abs_pos + 1 < data.len() && data[abs_pos + 1] == b'\n' {
            return Some(abs_pos);
        }
        start = abs_pos + 1;
    }
    None
}

/// Result of scanning for CRLF.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum CrlfResult {
    /// CRLF found at this offset (offset points to '\r')
    Found(usize),
    /// CRLF not found, scanned this many bytes
    /// If last byte is '\r', this is len-1 (might be incomplete CRLF)
    NotFound(usize),
}

impl CrlfResult {
    /// Scan for CRLF using memchr (SIMD-optimized).
    #[inline]
    pub fn scan(slice: &[u8]) -> Self {
        match find_crlf(slice) {
            Some(pos) => CrlfResult::Found(pos),
            None => {
                // Special case: last byte is '\r', might be incomplete CRLF
                if slice.last() == Some(&b'\r') {
                    CrlfResult::NotFound(slice.len() - 1)
                } else {
                    CrlfResult::NotFound(slice.len())
                }
            }
        }
    }

    #[inline]
    pub fn found(&self) -> bool {
        matches!(self, CrlfResult::Found(_))
    }

    #[inline]
    pub fn offset(&self) -> usize {
        match *self {
            CrlfResult::Found(n) | CrlfResult::NotFound(n) => n,
        }
    }
}

/// Synchronous wire stream reading trait.
///
/// This is the core trait for zero-copy parsing from complete buffers.
/// All methods are synchronous and designed for maximum performance.
///
/// # Associated Types
///
/// - `ReadError` - Error type for stream operations
/// - `ReadCursor` - Opaque position marker for save/restore
/// - `ReadBorrow` - Borrowed slice of bytes (dynamic length)
/// - `ReadBorrowConst` - Borrowed slice with compile-time known length
///
/// # Design Philosophy
///
/// The trait is designed around the concept of "borrows" - references to
/// regions of the underlying buffer that can be:
/// - Peeked without consuming
/// - Extended to include more data
/// - Accepted (consumed) to advance the stream position
/// - Subsliced for partial access
#[allow(clippy::needless_lifetimes)]
pub trait WireReadSync {
    type ReadError: std::error::Error;
    type ReadCursor<'a>: 'a
    where
        Self: 'a;
    type ReadBorrow<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;
    type ReadBorrowConst<'a, const N: usize>: Deref<Target = [u8; N]> + 'a
    where
        Self: 'a;

    /// Get current position as a cursor.
    fn position<'a>(&'a self) -> Self::ReadCursor<'a>;

    /// Calculate offset from a base cursor.
    fn offset_from<'a>(&'a self, base: &'_ Self::ReadCursor<'a>) -> Result<usize, Self::ReadError>;

    /// Restore position to a cursor.
    fn restore_to<'a>(&'a self, cursor: &'_ Self::ReadCursor<'a>) -> Result<(), Self::ReadError>;

    /// Advance position by N bytes.
    fn advance_by(&self, distance: usize) -> Result<(), Self::ReadError>;

    /// Peek at available data (up to limit).
    fn peek<'a>(&'a self, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Peek exactly N bytes.
    fn peek_exactly<'a, const N: usize>(&'a self) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError>;

    /// Upcast a const borrow to a dynamic borrow.
    fn upcast<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Get a subslice of a borrow.
    fn subslice<'a>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Get an exact subslice of a borrow.
    fn subslice_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        index: Option<usize>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError>;

    /// Extend a borrow to include more data (up to limit).
    fn extend<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Extend a borrow to exactly N bytes.
    fn extend_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError>;

    /// Accept (consume) bytes from a borrow.
    fn accept<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Accept exactly a const borrow.
    fn accept_exactly<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<(), Self::ReadError>;

    /// Unaccept (unconsume) bytes from a borrow.
    fn unaccept<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Unaccept exactly a const borrow.
    fn unaccept_exactly<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<(), Self::ReadError>;
}

/// Extension methods for sync reading.
///
/// These provide common parsing operations built on top of the core trait.
pub trait WireReadSyncExt: WireReadSync {
    /// Check for expected byte and consume it.
    #[inline]
    fn expect_byte_sync(&self, expected: u8) -> Result<Result<(), u8>, Self::ReadError> {
        let borrow = self.peek_exactly::<1>()?;
        let [encountered] = *borrow;

        Ok(if encountered != expected {
            Err(encountered)
        } else {
            self.accept_exactly(&borrow)?;
            Ok(())
        })
    }

    /// Read until CRLF is found (sync version using memchr).
    ///
    /// Returns `Ok(Ok(line))` if CRLF found within limit, consuming up to and including CRLF.
    /// Returns `Ok(Err(partial))` if CRLF not found or would exceed limit.
    #[inline]
    fn read_to_crlf_sync(&self, limit: Option<usize>) -> Result<Result<Self::ReadBorrow<'_>, Self::ReadBorrow<'_>>, Self::ReadError> {
        let borrow = self.peek(limit)?;

        match CrlfResult::scan(&borrow) {
            CrlfResult::Found(pos) => {
                if let Some(limit) = limit
                    && pos + 2 > limit
                {
                    let line = self.subslice(&borrow, None, Some(pos))?;
                    return Ok(Err(line));
                }
                let line = self.subslice(&borrow, None, Some(pos))?;
                self.accept(&borrow, Some(pos + 2))?;
                Ok(Ok(line))
            }
            CrlfResult::NotFound(_) => Ok(Err(borrow)),
        }
    }

    /// Read a little-endian i32 (4 bytes).
    #[inline]
    fn read_i32_le_sync(&self) -> Result<i32, Self::ReadError> {
        let borrow = self.peek_exactly::<4>()?;
        let value = i32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow)?;
        Ok(value)
    }

    /// Read a little-endian u32 (4 bytes).
    #[inline]
    fn read_u32_le_sync(&self) -> Result<u32, Self::ReadError> {
        let borrow = self.peek_exactly::<4>()?;
        let value = u32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow)?;
        Ok(value)
    }

    /// Read a little-endian i64 (8 bytes).
    #[inline]
    fn read_i64_le_sync(&self) -> Result<i64, Self::ReadError> {
        let borrow = self.peek_exactly::<8>()?;
        let value = i64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow)?;
        Ok(value)
    }

    /// Read exactly N bytes and consume them.
    #[inline]
    fn read_exact_sync<const N: usize>(&self) -> Result<Self::ReadBorrowConst<'_, N>, Self::ReadError> {
        let borrow = self.peek_exactly::<N>()?;
        self.accept_exactly(&borrow)?;
        Ok(borrow)
    }

    /// Read up to `len` bytes and consume them.
    #[inline]
    fn read_bytes_sync(&self, len: usize) -> Result<Self::ReadBorrow<'_>, Self::ReadError> {
        let borrow = self.peek(Some(len))?;
        self.accept(&borrow, None)?;
        Ok(borrow)
    }

    /// Read until a null byte is found (for C-strings).
    /// Returns the bytes before the null, consuming up to and including the null.
    #[inline]
    fn read_cstring_sync(&self) -> Result<Result<Self::ReadBorrow<'_>, Self::ReadBorrow<'_>>, Self::ReadError> {
        let borrow = self.peek(None)?;

        if let Some(pos) = memchr::memchr(0, &borrow) {
            let content = self.subslice(&borrow, None, Some(pos))?;
            self.accept(&borrow, Some(pos + 1))?;
            Ok(Ok(content))
        } else {
            Ok(Err(borrow))
        }
    }
}

impl<T: WireReadSync + ?Sized> WireReadSyncExt for T {}

/// Async wire stream reading trait.
///
/// Extends `WireReadSync` with async methods for streaming I/O.
/// Use this when parsing from a network stream where you may need
/// to await more data.
#[allow(clippy::needless_lifetimes)]
pub trait WireRead: WireReadSync {
    /// Peek with async read if needed.
    async fn peek_read<'a>(&'a self, min: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Peek exactly N bytes with async read if needed.
    async fn peek_read_exactly<'a, const N: usize>(&'a self) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError>;

    /// Extend borrow with async read if needed.
    async fn extend_read<'a>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        min: Option<usize>,
    ) -> Result<Self::ReadBorrow<'a>, Self::ReadError>;

    /// Extend borrow to exactly N bytes with async read if needed.
    async fn extend_read_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError>;
}

/// Extension methods for async reading.
pub trait WireReadExt: WireRead {
    /// Check for expected byte and consume it (async version).
    async fn expect_byte(&self, expected: u8) -> Result<Result<(), u8>, Self::ReadError> {
        let borrow = self.peek_read_exactly::<1>().await?;
        let [encountered] = *borrow;

        Ok(if encountered != expected {
            Err(encountered)
        } else {
            self.accept_exactly(&borrow)?;
            Ok(())
        })
    }

    /// Read until CRLF is found (async version with streaming support).
    async fn read_to_crlf(&self, limit: Option<usize>) -> Result<Result<Self::ReadBorrow<'_>, Self::ReadBorrow<'_>>, Self::ReadError> {
        let mut borrow = self.peek(limit)?;
        let mut scanned = 0;

        loop {
            match CrlfResult::scan(&borrow[scanned..]) {
                CrlfResult::Found(pos) => {
                    let abs_pos = scanned + pos;
                    if let Some(limit) = limit
                        && abs_pos + 2 > limit
                    {
                        let line = self.subslice(&borrow, None, Some(abs_pos))?;
                        return Ok(Err(line));
                    }
                    let line = self.subslice(&borrow, None, Some(abs_pos))?;
                    self.accept(&borrow, Some(abs_pos + 2))?;
                    return Ok(Ok(line));
                }
                CrlfResult::NotFound(len) => {
                    scanned += len;
                    if let Some(limit) = limit
                        && scanned >= limit
                    {
                        return Ok(Err(borrow));
                    }
                    // Need more data - async read
                    borrow = self.extend_read(&borrow, None).await?;
                }
            }
        }
    }

    /// Read a little-endian i32 (async version).
    async fn read_i32_le(&self) -> Result<i32, Self::ReadError> {
        let borrow = self.peek_read_exactly::<4>().await?;
        let value = i32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow)?;
        Ok(value)
    }

    /// Read a little-endian u32 (async version).
    async fn read_u32_le(&self) -> Result<u32, Self::ReadError> {
        let borrow = self.peek_read_exactly::<4>().await?;
        let value = u32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow)?;
        Ok(value)
    }

    /// Read a little-endian i64 (async version).
    async fn read_i64_le(&self) -> Result<i64, Self::ReadError> {
        let borrow = self.peek_read_exactly::<8>().await?;
        let value = i64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow)?;
        Ok(value)
    }
}

impl<T: WireRead + ?Sized> WireReadExt for T {}
