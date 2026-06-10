//! Integration tests for wire-stream.

#[cfg(test)]
mod slice_stream_tests {
    use crate::{SliceStream, WireRead, WireReadSync, WireReadSyncExt};

    #[test]
    fn test_cursor_save_restore() {
        let data = b"ABCDEF";
        let stream = SliceStream::new(data);

        let cursor = stream.position();
        stream.advance_by(3).expect("Should be able to advance cursor");
        assert_eq!(stream.consumed(), 3);

        stream.restore_to(&cursor).expect("Should be able to restore cursor");
        assert_eq!(stream.consumed(), 0);
    }

    #[test]
    fn test_peek_and_accept_partial() {
        let data = b"Hello, World!";
        let stream = SliceStream::new(data);

        let peeked = stream.peek(Some(5)).expect("Should be able to peek data");
        assert_eq!(&*peeked, b"Hello");
        assert_eq!(stream.consumed(), 0);

        stream.accept(&peeked, Some(3)).expect("Should be able to accept peeked");
        assert_eq!(stream.consumed(), 3);
    }

    #[test]
    fn test_extend_borrow() {
        let data = b"Hello, World!";
        let stream = SliceStream::new(data);

        let initial = stream.peek(Some(5)).expect("Should be able to peek data");
        assert_eq!(&*initial, b"Hello");

        let extended = stream.extend(&initial, Some(10)).expect("Should be able to extend peek data");
        assert_eq!(&*extended, b"Hello, Wor");
    }

    #[test]
    fn test_subslice() {
        let data = b"Hello, World!";
        let stream = SliceStream::new(data);

        let full = stream.peek(None).expect("Should be able to peek data");
        let sub = stream.subslice(&full, Some(7), Some(12)).expect("Should be able to subslice");
        assert_eq!(&*sub, b"World");
    }

    #[test]
    fn test_multiple_crlf_lines() {
        let data = b"line1\r\nline2\r\nline3\r\n";
        let stream = SliceStream::new(data);

        let line1 = stream.read_to_crlf_sync(None).expect("Should be able to read").expect("Should be able to read");
        assert_eq!(&*line1, b"line1");

        let line2 = stream.read_to_crlf_sync(None).expect("Should be able to read").expect("Should be able to read");
        assert_eq!(&*line2, b"line2");

        let line3 = stream.read_to_crlf_sync(None).expect("Should be able to read").expect("Should be able to read");
        assert_eq!(&*line3, b"line3");

        assert!(stream.is_empty());
    }

    #[test]
    fn test_async_operations() {
        let data = b"test\r\n";
        let stream = SliceStream::new(data);

        pollster::block_on(async {
            let peeked = stream.peek_read(Some(4)).await.expect("Should be able to peek data");
            assert_eq!(&*peeked, b"test\r\n");

            let exact: crate::SliceBorrowConst<'_, 4> = stream.peek_read_exactly().await.expect("Should be able to peek data");
            assert_eq!(*exact, *b"test");
        });
    }
}

#[cfg(test)]
mod borrow_tracker_tests {
    use crate::BorrowTracker;

    #[test]
    fn test_multiple_positions_ordering() {
        let tracker = BorrowTracker::new();

        let pos1 = tracker.borrow_position(50);
        let pos2 = tracker.borrow_position(10);
        let pos3 = tracker.borrow_position(30);

        assert_eq!(tracker.lowest_position(), Some(10));
        assert_eq!(tracker.highest_position(), Some(50));

        drop(pos2);
        assert_eq!(tracker.lowest_position(), Some(30));

        drop(pos1);
        assert_eq!(tracker.highest_position(), Some(30));

        drop(pos3);
        assert!(!tracker.has_position_borrows());
    }

    #[test]
    fn test_overlapping_spans() {
        let tracker = BorrowTracker::new();
        let data1 = b"hello";
        let data2 = b"world";

        let span1 = tracker.borrow_span(0, data1);
        let span2 = tracker.borrow_span(3, data2);

        assert_eq!(tracker.lowest_span_start(), Some(0));
        assert_eq!(tracker.highest_span_end(), Some(8));

        drop(span1);
        assert_eq!(tracker.lowest_span_start(), Some(3));

        drop(span2);
        assert!(!tracker.has_span_borrows());
    }
}
