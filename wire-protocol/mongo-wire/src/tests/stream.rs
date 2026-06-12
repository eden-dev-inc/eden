//! Test stream implementation for mongo-wire tests.

use std::cell::Cell;
use std::ops::Deref;
use wire_stream::{BorrowTracker, PositionBorrow, SpanBorrow, WireRead, WireReadSync};

/// A simple in-memory stream for testing.
pub struct TestStream<T: AsRef<[u8]>> {
    buffer: T,
    cursor: Cell<usize>,
    borrow_tracker: BorrowTracker,
}

impl<T: AsRef<[u8]>> TestStream<T> {
    #[allow(dead_code)]
    pub fn new(buffer: T) -> Self {
        Self {
            buffer,
            cursor: Cell::new(0),
            borrow_tracker: BorrowTracker::new(),
        }
    }

    #[allow(dead_code)]
    pub fn position(&self) -> usize {
        self.cursor.get()
    }

    #[allow(dead_code)]
    pub fn remaining(&self) -> usize {
        self.buffer.as_ref().len() - self.cursor.get()
    }
}

#[derive(Clone, Eq, PartialEq, Debug, thiserror::Error)]
pub enum TestStreamError {
    #[error("invalid base cursor")]
    InvalidBaseCursor,
    #[error("invalid restore cursor")]
    InvalidRestoreCursor,
    #[error("invalid advance: need {requested}, have {available}")]
    InvalidAdvance { requested: usize, available: usize },
    #[error("not enough data: need {needed}, have {available}")]
    NotEnoughData { needed: usize, available: usize },
    #[error("invalid subslice")]
    InvalidSubslice,
    #[error("invalid accept")]
    InvalidAccept,
    #[error("invalid unaccept")]
    InvalidUnaccept,
}

impl From<TestStreamError> for crate::MongoWireError {
    fn from(e: TestStreamError) -> Self {
        crate::MongoWireError::Stream(e.to_string())
    }
}

#[derive(Debug)]
pub struct TestCursor<'a>(PositionBorrow<'a>);

#[derive(Debug)]
pub struct TestBorrow<'a>(SpanBorrow<'a>);

impl Deref for TestBorrow<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct TestBorrowConst<'a, const N: usize>(SpanBorrow<'a>);

impl<const N: usize> Deref for TestBorrowConst<'_, N> {
    type Target = [u8; N];
    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.0.deref().as_ptr() as *const [u8; N]) }
    }
}

#[allow(clippy::needless_lifetimes)]
impl<T: AsRef<[u8]>> WireReadSync for TestStream<T> {
    type ReadError = TestStreamError;
    type ReadCursor<'a>
        = TestCursor<'a>
    where
        Self: 'a;
    type ReadBorrow<'a>
        = TestBorrow<'a>
    where
        Self: 'a;
    type ReadBorrowConst<'a, const N: usize>
        = TestBorrowConst<'a, N>
    where
        Self: 'a;

    fn position<'a>(&'a self) -> Self::ReadCursor<'a> {
        TestCursor(self.borrow_tracker.borrow_position(self.cursor.get()))
    }

    fn offset_from<'a>(&'a self, base: &'_ Self::ReadCursor<'a>) -> Result<usize, Self::ReadError> {
        self.cursor.get().checked_sub(*base.0).ok_or(TestStreamError::InvalidBaseCursor)
    }

    fn restore_to<'a>(&'a self, cursor: &'_ Self::ReadCursor<'a>) -> Result<(), Self::ReadError> {
        if *cursor.0 <= self.buffer.as_ref().len() {
            self.cursor.set(*cursor.0);
            Ok(())
        } else {
            Err(TestStreamError::InvalidRestoreCursor)
        }
    }

    fn advance_by(&self, distance: usize) -> Result<(), Self::ReadError> {
        let current = self.cursor.get();
        let new_position = current + distance;
        let len = self.buffer.as_ref().len();

        if new_position <= len {
            self.cursor.set(new_position);
            Ok(())
        } else {
            Err(TestStreamError::InvalidAdvance { requested: distance, available: len - current })
        }
    }

    fn peek<'a>(&'a self, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let pos = self.cursor.get();
        let rest = &self.buffer.as_ref()[pos..];

        let data = match limit {
            Some(limit) if rest.len() > limit => &rest[..limit],
            _ => rest,
        };

        Ok(TestBorrow(self.borrow_tracker.borrow_span(pos, data)))
    }

    fn peek_exactly<'a, const N: usize>(&'a self) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        let pos = self.cursor.get();
        let rest = &self.buffer.as_ref()[pos..];

        if rest.len() >= N {
            Ok(TestBorrowConst(self.borrow_tracker.borrow_span(pos, &rest[..N])))
        } else {
            Err(TestStreamError::NotEnoughData { needed: N, available: rest.len() })
        }
    }

    fn upcast<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let (start, end) = self.borrow_tracker.get_span(&borrow.0);
        let data = &self.buffer.as_ref()[start..end];
        Ok(TestBorrow(self.borrow_tracker.borrow_span(start, data)))
    }

    fn subslice<'a>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let (borrow_start, borrow_end) = self.borrow_tracker.get_span(&borrow.0);

        let slice_start = borrow_start + start.unwrap_or(0);
        let slice_end = end.map(|e| borrow_start + e).unwrap_or(borrow_end);

        if slice_start > borrow_end || slice_end > borrow_end || slice_start > slice_end {
            return Err(TestStreamError::InvalidSubslice);
        }

        let data = &self.buffer.as_ref()[slice_start..slice_end];
        Ok(TestBorrow(self.borrow_tracker.borrow_span(slice_start, data)))
    }

    fn subslice_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        index: Option<usize>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        let (borrow_start, borrow_end) = self.borrow_tracker.get_span(&borrow.0);
        let slice_start = borrow_start + index.unwrap_or(0);
        let slice_end = slice_start + N;

        if slice_end > borrow_end {
            return Err(TestStreamError::InvalidSubslice);
        }

        let data = &self.buffer.as_ref()[slice_start..slice_end];
        Ok(TestBorrowConst(self.borrow_tracker.borrow_span(slice_start, data)))
    }

    fn extend<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let (borrow_start, _) = self.borrow_tracker.get_span(&borrow.0);
        let rest = &self.buffer.as_ref()[borrow_start..];

        let data = match limit {
            Some(limit) if limit < rest.len() => &rest[..limit],
            _ => rest,
        };

        Ok(TestBorrow(self.borrow_tracker.borrow_span(borrow_start, data)))
    }

    fn extend_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        let (borrow_start, _) = self.borrow_tracker.get_span(&borrow.0);
        let rest = &self.buffer.as_ref()[borrow_start..];

        if rest.len() >= N {
            Ok(TestBorrowConst(self.borrow_tracker.borrow_span(borrow_start, &rest[..N])))
        } else {
            Err(TestStreamError::NotEnoughData { needed: N, available: rest.len() })
        }
    }

    fn accept<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let (borrow_start, borrow_end) = self.borrow_tracker.get_span(&borrow.0);
        let pos = self.cursor.get();

        if !(borrow_start..=borrow_end).contains(&pos) {
            return Err(TestStreamError::InvalidAccept);
        }

        let new_pos = match limit {
            Some(limit) => (borrow_start + limit).min(borrow_end),
            None => borrow_end,
        };

        if new_pos > pos {
            self.cursor.set(new_pos);
        }

        let data = &self.buffer.as_ref()[borrow_start..new_pos];
        Ok(TestBorrow(self.borrow_tracker.borrow_span(borrow_start, data)))
    }

    fn accept_exactly<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<(), Self::ReadError> {
        let (borrow_start, borrow_end) = self.borrow_tracker.get_span(&borrow.0);
        let pos = self.cursor.get();

        if !(borrow_start..=borrow_end).contains(&pos) {
            return Err(TestStreamError::InvalidAccept);
        }

        if borrow_end > pos {
            self.cursor.set(borrow_end);
        }

        Ok(())
    }

    fn unaccept<'a>(&'a self, borrow: &'_ Self::ReadBorrow<'a>, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        let (borrow_start, borrow_end) = self.borrow_tracker.get_span(&borrow.0);
        let pos = self.cursor.get();

        if !(borrow_start..=borrow_end).contains(&pos) {
            return Err(TestStreamError::InvalidUnaccept);
        }

        let new_pos = borrow_start + limit.unwrap_or(0);
        if new_pos > pos {
            return Err(TestStreamError::InvalidUnaccept);
        }

        self.cursor.set(new_pos);
        let data = &self.buffer.as_ref()[borrow_start..new_pos];
        Ok(TestBorrow(self.borrow_tracker.borrow_span(borrow_start, data)))
    }

    fn unaccept_exactly<'a, const N: usize>(&'a self, borrow: &'_ Self::ReadBorrowConst<'a, N>) -> Result<(), Self::ReadError> {
        let (borrow_start, borrow_end) = self.borrow_tracker.get_span(&borrow.0);
        let pos = self.cursor.get();

        if pos != borrow_end {
            return Err(TestStreamError::InvalidUnaccept);
        }

        self.cursor.set(borrow_start);
        Ok(())
    }
}

#[allow(clippy::needless_lifetimes)]
impl<T: AsRef<[u8]>> WireRead for TestStream<T> {
    async fn peek_read<'a>(&'a self, limit: Option<usize>) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        self.peek(limit)
    }

    async fn peek_read_exactly<'a, const N: usize>(&'a self) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        self.peek_exactly()
    }

    async fn extend_read<'a>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
        limit: Option<usize>,
    ) -> Result<Self::ReadBorrow<'a>, Self::ReadError> {
        self.extend(borrow, limit)
    }

    async fn extend_read_exactly<'a, const N: usize>(
        &'a self,
        borrow: &'_ Self::ReadBorrow<'a>,
    ) -> Result<Self::ReadBorrowConst<'a, N>, Self::ReadError> {
        self.extend_exactly(borrow)
    }
}
