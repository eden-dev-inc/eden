//! Runtime borrow tracking for streaming safety.
//!
//! This module provides `BorrowTracker`, which enables safe borrowing from
//! streaming buffers where data may be invalidated when the buffer is refilled.
//!
//! The tracker maintains sorted lists of active borrows by their positions,
//! enabling efficient queries about which regions are currently borrowed.

use elsa::FrozenVec;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr::NonNull;

trait Bounded {
    fn low_bound(&self) -> usize;
    fn high_bound(&self) -> usize;
}

/// A pair of adjacent slot pointers (prev, next) for linked list navigation.
type SlotNeighbors<T> = (Option<NonNull<Slot<T>>>, Option<NonNull<Slot<T>>>);

#[derive(Copy, Clone)]
struct FreeSlot<T: Copy + Bounded> {
    prev_free: Option<NonNull<Slot<T>>>,
    next_free: Option<NonNull<Slot<T>>>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct ActiveSlot<T: Copy + Bounded> {
    prev_lowest: Option<NonNull<Slot<T>>>,
    next_lowest: Option<NonNull<Slot<T>>>,
    prev_highest: Option<NonNull<Slot<T>>>,
    next_highest: Option<NonNull<Slot<T>>>,
    data: T,
}

#[derive(Copy, Clone)]
union Slot<T: Copy + Bounded> {
    free: FreeSlot<T>,
    active: ActiveSlot<T>,
}

impl<T: Copy + Bounded> Default for Slot<T> {
    fn default() -> Self {
        Self { free: FreeSlot { prev_free: None, next_free: None } }
    }
}

struct Arena<T: Copy + Bounded> {
    arena: FrozenVec<Box<UnsafeCell<[Slot<T>; 32]>>>,
    lowest_low: Option<NonNull<Slot<T>>>,
    highest_low: Option<NonNull<Slot<T>>>,
    lowest_high: Option<NonNull<Slot<T>>>,
    highest_high: Option<NonNull<Slot<T>>>,
    free_head: Option<NonNull<Slot<T>>>,
}

impl<T: Copy + Bounded> Arena<T> {
    fn new() -> Self {
        Self {
            arena: FrozenVec::new(),
            lowest_low: None,
            highest_low: None,
            lowest_high: None,
            highest_high: None,
            free_head: None,
        }
    }

    fn lowest_low(&self) -> Option<usize> {
        self.lowest_low.map(|slot| unsafe { (*slot.as_ptr()).active.data.low_bound() })
    }

    #[allow(dead_code)]
    fn highest_low(&self) -> Option<usize> {
        self.highest_low.map(|slot| unsafe { (*slot.as_ptr()).active.data.low_bound() })
    }

    #[allow(dead_code)]
    fn lowest_high(&self) -> Option<usize> {
        self.lowest_high.map(|slot| unsafe { (*slot.as_ptr()).active.data.high_bound() })
    }

    fn highest_high(&self) -> Option<usize> {
        self.highest_high.map(|slot| unsafe { (*slot.as_ptr()).active.data.high_bound() })
    }

    fn is_empty(&self) -> bool {
        matches!(
            self,
            Arena {
                lowest_low: None,
                highest_low: None,
                lowest_high: None,
                highest_high: None,
                ..
            }
        )
    }

    fn alloc(&mut self) -> NonNull<Slot<T>> {
        unsafe {
            let free_head = self.free_head.take().unwrap_or_else(|| {
                let slice = self.arena.push_get(Default::default()).get();

                for i in 1..(*slice).len() {
                    let prev_free = &raw mut (*slice)[i - 1];
                    let next_free = &raw mut (*slice)[i];
                    (*prev_free).free.next_free = Some(NonNull::new_unchecked(next_free));
                    (*next_free).free.prev_free = Some(NonNull::new_unchecked(prev_free));
                }

                NonNull::new_unchecked(&raw mut (*slice)[0])
            });

            self.free_head = (*free_head.as_ptr()).free.next_free;

            free_head
        }
    }

    unsafe fn free(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            let next_free = self.free_head.replace(slot);
            (*slot.as_ptr()).free = FreeSlot { prev_free: None, next_free };
            let Some(next_free) = next_free else { return };
            (*next_free.as_ptr()).free.prev_free = Some(slot);
        }
    }

    unsafe fn find_new_lowest_neighbors(
        low_bound: usize,
        mut prev_lowest: Option<NonNull<Slot<T>>>,
        mut next_lowest: Option<NonNull<Slot<T>>>,
    ) -> SlotNeighbors<T> {
        unsafe {
            while let Some(next_lowest_ptr) = next_lowest.map(NonNull::as_ptr)
                && (*next_lowest_ptr).active.data.low_bound() > low_bound
            {
                (prev_lowest, next_lowest) = (next_lowest, (*next_lowest_ptr).active.next_lowest);
            }

            while let Some(prev_lowest_ptr) = prev_lowest.map(NonNull::as_ptr)
                && (*prev_lowest_ptr).active.data.low_bound() < low_bound
            {
                (next_lowest, prev_lowest) = (prev_lowest, (*prev_lowest_ptr).active.prev_lowest);
            }

            (prev_lowest, next_lowest)
        }
    }

    fn find_lowest_neighbors(&self, low_bound: usize) -> SlotNeighbors<T> {
        unsafe { Self::find_new_lowest_neighbors(low_bound, self.lowest_low, None) }
    }

    unsafe fn find_new_highest_neighbors(
        high_bound: usize,
        mut prev_highest: Option<NonNull<Slot<T>>>,
        mut next_highest: Option<NonNull<Slot<T>>>,
    ) -> SlotNeighbors<T> {
        unsafe {
            while let Some(next_highest_ptr) = next_highest.map(NonNull::as_ptr)
                && (*next_highest_ptr).active.data.high_bound() < high_bound
            {
                (prev_highest, next_highest) = (next_highest, (*next_highest_ptr).active.next_highest);
            }

            while let Some(prev_highest_ptr) = prev_highest.map(NonNull::as_ptr)
                && (*prev_highest_ptr).active.data.high_bound() > high_bound
            {
                (next_highest, prev_highest) = (prev_highest, (*prev_highest_ptr).active.prev_highest);
            }

            (prev_highest, next_highest)
        }
    }

    fn find_highest_neighbors(&self, high_bound: usize) -> SlotNeighbors<T> {
        unsafe { Self::find_new_highest_neighbors(high_bound, self.highest_high, None) }
    }

    fn find_neighbors(&self, data: T) -> ActiveSlot<T> {
        let (prev_lowest, next_lowest) = self.find_lowest_neighbors(data.low_bound());
        let (prev_highest, next_highest) = self.find_highest_neighbors(data.high_bound());

        ActiveSlot { prev_lowest, next_lowest, prev_highest, next_highest, data }
    }

    unsafe fn install_lowest_neighbors(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            let ActiveSlot { prev_lowest, next_lowest, .. } = (*slot.as_ptr()).active;

            if let Some(prev_lowest_ptr) = prev_lowest.map(NonNull::as_ptr) {
                (*prev_lowest_ptr).active.next_lowest = Some(slot);
            } else {
                self.highest_low = Some(slot);
            }

            if let Some(next_lowest_ptr) = next_lowest.map(NonNull::as_ptr) {
                (*next_lowest_ptr).active.prev_lowest = Some(slot);
            } else {
                self.lowest_low = Some(slot);
            }
        }
    }

    unsafe fn install_highest_neighbors(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            let ActiveSlot { prev_highest, next_highest, .. } = (*slot.as_ptr()).active;

            if let Some(prev_highest_ptr) = prev_highest.map(NonNull::as_ptr) {
                (*prev_highest_ptr).active.next_highest = Some(slot);
            } else {
                self.lowest_high = Some(slot);
            }

            if let Some(next_highest_ptr) = next_highest.map(NonNull::as_ptr) {
                (*next_highest_ptr).active.prev_highest = Some(slot);
            } else {
                self.highest_high = Some(slot);
            }
        }
    }

    unsafe fn install_neighbors(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            self.install_lowest_neighbors(slot);
            self.install_highest_neighbors(slot);
        }
    }

    unsafe fn uninstall_lowest_neighbors(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            let ActiveSlot { prev_lowest, next_lowest, .. } = (*slot.as_ptr()).active;

            if let Some(prev_lowest_ptr) = prev_lowest.map(NonNull::as_ptr) {
                (*prev_lowest_ptr).active.next_lowest = next_lowest;
            } else {
                self.highest_low = next_lowest;
            }

            if let Some(next_lowest_ptr) = next_lowest.map(NonNull::as_ptr) {
                (*next_lowest_ptr).active.prev_lowest = prev_lowest;
            } else {
                self.lowest_low = prev_lowest;
            }
        }
    }

    unsafe fn uninstall_highest_neighbors(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            let ActiveSlot { prev_highest, next_highest, .. } = (*slot.as_ptr()).active;

            if let Some(prev_highest_ptr) = prev_highest.map(NonNull::as_ptr) {
                (*prev_highest_ptr).active.next_highest = next_highest;
            } else {
                self.lowest_high = next_highest;
            }

            if let Some(next_highest_ptr) = next_highest.map(NonNull::as_ptr) {
                (*next_highest_ptr).active.prev_highest = prev_highest;
            } else {
                self.highest_high = prev_highest;
            }
        }
    }

    unsafe fn uninstall_neighbors(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            self.uninstall_lowest_neighbors(slot);
            self.uninstall_highest_neighbors(slot);
        }
    }

    fn insert(&mut self, data: T) -> NonNull<Slot<T>> {
        unsafe {
            let slot = self.alloc();
            (*slot.as_ptr()).active = self.find_neighbors(data);
            self.install_neighbors(slot);
            slot
        }
    }

    unsafe fn adjust(&mut self, slot: NonNull<Slot<T>>, new_data: T) {
        unsafe {
            let prev_low_bound = (*slot.as_ptr()).active.data.low_bound();
            let prev_high_bound = (*slot.as_ptr()).active.data.high_bound();
            let next_low_bound = new_data.low_bound();
            let next_high_bound = new_data.high_bound();

            let ActiveSlot { prev_lowest, next_lowest, prev_highest, next_highest, .. } = (*slot.as_ptr()).active;

            let (prev_lowest, next_lowest) = if next_low_bound != prev_low_bound {
                self.uninstall_lowest_neighbors(slot);
                Self::find_new_lowest_neighbors(next_low_bound, prev_lowest, next_lowest)
            } else {
                (prev_lowest, next_lowest)
            };

            let (prev_highest, next_highest) = if next_high_bound != prev_high_bound {
                self.uninstall_highest_neighbors(slot);
                Self::find_new_highest_neighbors(next_high_bound, prev_highest, next_highest)
            } else {
                (prev_highest, next_highest)
            };

            (*slot.as_ptr()).active = ActiveSlot {
                prev_lowest,
                next_lowest,
                prev_highest,
                next_highest,
                data: new_data,
            };

            if next_low_bound != prev_low_bound {
                self.install_lowest_neighbors(slot);
            }

            if next_high_bound != prev_high_bound {
                self.install_highest_neighbors(slot);
            }
        }
    }

    unsafe fn remove(&mut self, slot: NonNull<Slot<T>>) {
        unsafe {
            self.uninstall_neighbors(slot);
            self.free(slot);
        }
    }
}

#[derive(Debug)]
struct SlotBorrow<'a, T: Copy + Bounded> {
    slot: NonNull<Slot<T>>,
    arena: NonNull<Arena<T>>,
    phantom: PhantomData<&'a BorrowTracker>,
}

impl<'a, T: Copy + Bounded> SlotBorrow<'a, T> {
    unsafe fn from_raw(borrow_tracker: &'a BorrowTracker, arena: NonNull<Arena<T>>, slot: NonNull<Slot<T>>) -> Self {
        let _ = borrow_tracker;

        Self { slot, arena, phantom: PhantomData }
    }

    unsafe fn new(borrow_tracker: &'a BorrowTracker, arena: NonNull<Arena<T>>, data: T) -> Self {
        unsafe {
            let slot = (*arena.as_ptr()).insert(data);
            Self::from_raw(borrow_tracker, arena, slot)
        }
    }
}

impl<'a> SlotBorrow<'a, Position> {
    unsafe fn new_position(borrow_tracker: &'a BorrowTracker, position: Position) -> Self {
        unsafe {
            let arena = NonNull::new_unchecked(borrow_tracker.positions.get());
            Self::new(borrow_tracker, arena, position)
        }
    }
}

impl<'a> SlotBorrow<'a, Span> {
    unsafe fn new_span(borrow_tracker: &'a BorrowTracker, span: Span) -> Self {
        unsafe {
            let arena = NonNull::new_unchecked(borrow_tracker.spans.get());
            Self::new(borrow_tracker, arena, span)
        }
    }
}

impl<'a, T: Copy + Bounded> SlotBorrow<'a, T> {
    fn adjust(&mut self, new_data: T) {
        unsafe { (*self.arena.as_ptr()).adjust(self.slot, new_data) }
    }
}

impl<'a, T: Copy + Bounded> std::ops::Deref for SlotBorrow<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.slot.as_ptr()).active.data }
    }
}

impl<'a, T: Copy + Bounded> Drop for SlotBorrow<'a, T> {
    fn drop(&mut self) {
        unsafe { (*self.arena.as_ptr()).remove(self.slot) }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Position(usize);

impl Bounded for Position {
    fn low_bound(&self) -> usize {
        self.0
    }
    fn high_bound(&self) -> usize {
        self.0
    }
}

/// A borrowed position in a stream buffer.
///
/// This represents a saved cursor position that tracks its location
/// in the borrow tracker. When dropped, it automatically unregisters.
#[derive(Debug)]
pub struct PositionBorrow<'a>(SlotBorrow<'a, Position>);

impl<'a> PositionBorrow<'a> {
    fn adjust(&mut self, new_position: Position) {
        self.0.adjust(new_position);
    }
}

impl<'a> std::ops::Deref for PositionBorrow<'a> {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Span {
    start: usize,
    end: usize,
    data: NonNull<[u8]>,
}

impl Bounded for Span {
    fn low_bound(&self) -> usize {
        self.start
    }
    fn high_bound(&self) -> usize {
        self.end
    }
}

/// A borrowed span of bytes from a stream buffer.
///
/// This represents a reference to a region of the buffer that tracks
/// its bounds in the borrow tracker. When dropped, it automatically unregisters.
#[derive(Debug)]
pub struct SpanBorrow<'a>(SlotBorrow<'a, Span>);

impl<'a> SpanBorrow<'a> {
    fn adjust(&mut self, new_span: Span) {
        self.0.adjust(new_span);
    }
}

impl<'a> std::ops::Deref for SpanBorrow<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0.data.as_ptr() }
    }
}

/// Tracks active borrows from a streaming buffer.
///
/// When parsing from a stream that may need to refill its buffer,
/// we need to track which regions are currently borrowed to ensure
/// we don't invalidate them. This tracker maintains sorted lists
/// of active position and span borrows.
///
/// # Safety
///
/// The tracker uses interior mutability and raw pointers internally
/// but provides a safe API. Borrows are automatically tracked on
/// creation and untracked on drop.
#[derive(Debug)]
pub struct BorrowTracker {
    positions: UnsafeCell<Arena<Position>>,
    spans: UnsafeCell<Arena<Span>>,
}

#[allow(clippy::needless_lifetimes)]
impl BorrowTracker {
    /// Create a new empty borrow tracker.
    pub fn new() -> Self {
        Self {
            positions: UnsafeCell::new(Arena::new()),
            spans: UnsafeCell::new(Arena::new()),
        }
    }

    /// Returns true if there are any active position or span borrows.
    pub fn has_position_borrows(&self) -> bool {
        unsafe { !(*self.positions.get()).is_empty() || !(*self.spans.get()).is_empty() }
    }

    /// Returns true if there are any active span borrows.
    pub fn has_span_borrows(&self) -> bool {
        unsafe { !(*self.spans.get()).is_empty() }
    }

    /// Returns the lowest borrowed position, if any.
    pub fn lowest_position(&self) -> Option<usize> {
        unsafe { (*self.positions.get()).lowest_low() }
    }

    /// Returns the highest borrowed position, if any.
    pub fn highest_position(&self) -> Option<usize> {
        unsafe { (*self.positions.get()).highest_high() }
    }

    /// Returns the lowest start of any borrowed span, if any.
    pub fn lowest_span_start(&self) -> Option<usize> {
        unsafe { (*self.spans.get()).lowest_low() }
    }

    /// Returns the highest end of any borrowed span, if any.
    pub fn highest_span_end(&self) -> Option<usize> {
        unsafe { (*self.spans.get()).highest_high() }
    }

    /// Borrow a position. The returned handle tracks this position
    /// until dropped.
    pub fn borrow_position<'a>(&'a self, position: usize) -> PositionBorrow<'a> {
        PositionBorrow(unsafe { SlotBorrow::new_position(self, Position(position)) })
    }

    /// Get the position value from a borrow.
    pub fn get_position<'a>(&'a self, borrow: &'_ PositionBorrow<'a>) -> usize {
        borrow.0.0
    }

    /// Adjust a position borrow to a new value.
    pub fn adjust_position<'a>(&'a self, position: &mut PositionBorrow<'a>, new_position: usize) {
        position.adjust(Position(new_position));
    }

    /// Adjust and return a position borrow.
    pub fn adjusted_position<'a>(&'a self, mut position: PositionBorrow<'a>, new_position: usize) -> PositionBorrow<'a> {
        self.adjust_position(&mut position, new_position);
        position
    }

    /// Borrow a span of bytes. The returned handle tracks this span
    /// until dropped.
    pub fn borrow_span<'a>(&'a self, index: usize, data: &'a [u8]) -> SpanBorrow<'a> {
        let data = NonNull::from(data);
        let (start, end) = (index, index + data.len());
        SpanBorrow(unsafe { SlotBorrow::new_span(self, Span { start, end, data }) })
    }

    /// Get the (start, end) bounds of a span borrow.
    pub fn get_span<'a>(&'a self, borrow: &'_ SpanBorrow<'a>) -> (usize, usize) {
        (borrow.0.start, borrow.0.end)
    }

    /// Adjust a span borrow to new bounds and data.
    pub fn adjust_span<'a>(&'a self, span: &mut SpanBorrow<'a>, index: usize, data: &'a [u8]) {
        let data = NonNull::from(data);
        let (start, end) = (index, index + data.len());
        span.adjust(Span { start, end, data })
    }

    /// Adjust and return a span borrow.
    pub fn adjusted_span<'a>(&'a self, mut span: SpanBorrow<'a>, index: usize, data: &'a [u8]) -> SpanBorrow<'a> {
        self.adjust_span(&mut span, index, data);
        span
    }
}

impl Default for BorrowTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_tracking() {
        let tracker = BorrowTracker::new();

        assert!(!tracker.has_position_borrows());

        let pos1 = tracker.borrow_position(10);
        assert!(tracker.has_position_borrows());
        assert_eq!(*pos1, 10);

        let pos2 = tracker.borrow_position(20);
        assert_eq!(tracker.lowest_position(), Some(10));
        assert_eq!(tracker.highest_position(), Some(20));

        drop(pos1);
        assert_eq!(tracker.lowest_position(), Some(20));

        drop(pos2);
        assert!(!tracker.has_position_borrows());
    }

    #[test]
    fn test_span_tracking() {
        let tracker = BorrowTracker::new();
        let data = b"hello world";

        assert!(!tracker.has_span_borrows());

        let span = tracker.borrow_span(0, data);
        assert!(tracker.has_span_borrows());
        assert_eq!(&*span, data);
        assert_eq!(tracker.get_span(&span), (0, 11));

        drop(span);
        assert!(!tracker.has_span_borrows());
    }

    #[test]
    fn test_position_adjustment() {
        let tracker = BorrowTracker::new();

        let mut pos = tracker.borrow_position(10);
        assert_eq!(*pos, 10);

        tracker.adjust_position(&mut pos, 15);
        assert_eq!(*pos, 15);
    }
}
