//! Property-based tests for wire-stream.

use crate::read::{CrlfResult, find_crlf};
use crate::{SliceStream, WireReadSync};
use proptest::prelude::*;

// =============================================================================
// CRLF Scanning Properties
// =============================================================================

proptest! {
    /// If find_crlf returns Some(pos), then data[pos..pos+2] == b"\r\n"
    #[test]
    fn find_crlf_correctness(data in prop::collection::vec(any::<u8>(), 0..1000)) {
        if let Some(pos) = find_crlf(&data) {
            prop_assert!(pos + 1 < data.len());
            prop_assert_eq!(data[pos], b'\r');
            prop_assert_eq!(data[pos + 1], b'\n');
        }
    }

    /// find_crlf returns the FIRST occurrence
    #[test]
    fn find_crlf_returns_first(data in prop::collection::vec(any::<u8>(), 0..500)) {
        if let Some(pos) = find_crlf(&data) {
            // No CRLF should exist before this position
            for i in 0..pos {
                if data[i] == b'\r' && i + 1 < data.len() {
                    prop_assert_ne!(data[i + 1], b'\n', "Found earlier CRLF at {}", i);
                }
            }
        }
    }

    /// CrlfResult::scan matches find_crlf behavior
    #[test]
    fn crlf_result_matches_find_crlf(data in prop::collection::vec(any::<u8>(), 0..1000)) {
        let result = CrlfResult::scan(&data);
        let found = find_crlf(&data);

        match (result, found) {
            (CrlfResult::Found(pos), Some(found_pos)) => {
                prop_assert_eq!(pos, found_pos);
            }
            (CrlfResult::NotFound(_), None) => {
                // Both agree no CRLF
            }
            (result, found) => {
                prop_assert!(false, "Mismatch: CrlfResult={:?}, find_crlf={:?}", result, found);
            }
        }
    }

    /// When CRLF not found, scanned offset is correct for trailing \r
    #[test]
    fn crlf_not_found_trailing_cr(data in prop::collection::vec(any::<u8>(), 1..1000)) {
        // Only test when there's no CRLF
        if find_crlf(&data).is_some() {
            return Ok(());
        }

        let result = CrlfResult::scan(&data);
        if let CrlfResult::NotFound(scanned) = result {
            if data.last() == Some(&b'\r') {
                // Trailing \r: scanned should be len-1 (might be incomplete CRLF)
                prop_assert_eq!(scanned, data.len() - 1);
            } else {
                // No trailing \r: scanned should be full length
                prop_assert_eq!(scanned, data.len());
            }
        }
    }

    /// Empty slice has no CRLF
    #[test]
    fn crlf_empty(_dummy in Just(())) {
        prop_assert!(find_crlf(&[]).is_none());
        prop_assert_eq!(CrlfResult::scan(&[]), CrlfResult::NotFound(0));
    }

    /// Single byte never contains CRLF
    #[test]
    fn crlf_single_byte(byte in any::<u8>()) {
        prop_assert!(find_crlf(&[byte]).is_none());
    }
}

// =============================================================================
// Strategic CRLF placement tests
// =============================================================================

proptest! {
    /// CRLF at start of buffer
    #[test]
    fn crlf_at_start(suffix in prop::collection::vec(any::<u8>(), 0..100)) {
        let mut data = vec![b'\r', b'\n'];
        data.extend(&suffix);

        prop_assert_eq!(find_crlf(&data), Some(0));
    }

    /// CRLF at end of buffer
    #[test]
    fn crlf_at_end(prefix in prop::collection::vec(any::<u8>(), 0..100).prop_filter(
        "no CRLF in prefix",
        |p| find_crlf(p).is_none() && p.last() != Some(&b'\r')
    )) {
        let mut data = prefix;
        data.push(b'\r');
        data.push(b'\n');

        let expected_pos = data.len() - 2;
        prop_assert_eq!(find_crlf(&data), Some(expected_pos));
    }

    /// Lone \r not followed by \n is not a CRLF
    #[test]
    fn lone_cr_not_crlf(
        prefix in prop::collection::vec(any::<u8>(), 0..50).prop_filter(
            "no CRLF",
            |p| find_crlf(p).is_none()
        ),
        suffix_byte in any::<u8>().prop_filter("not newline", |&b| b != b'\n')
    ) {
        let mut data = prefix;
        data.push(b'\r');
        data.push(suffix_byte);

        // If there was no CRLF before, and we added \r followed by non-\n,
        // there should still be no CRLF
        prop_assert!(find_crlf(&data).is_none());
    }

    /// Multiple \r characters: only \r\n counts
    #[test]
    fn multiple_cr_only_crlf_counts(count in 1usize..10) {
        // Create a sequence of \r characters followed by \n
        let mut data: Vec<u8> = vec![b'\r'; count];
        data.push(b'\n');

        // The CRLF is at the last \r
        let expected_pos = count - 1;
        prop_assert_eq!(find_crlf(&data), Some(expected_pos));
    }
}

// =============================================================================
// Explicit edge case tests
// =============================================================================

#[test]
fn crlf_just_crlf() {
    assert_eq!(find_crlf(b"\r\n"), Some(0));
    assert_eq!(CrlfResult::scan(b"\r\n"), CrlfResult::Found(0));
}

#[test]
fn crlf_trailing_cr_only() {
    assert_eq!(find_crlf(b"hello\r"), None);
    assert_eq!(CrlfResult::scan(b"hello\r"), CrlfResult::NotFound(5));
}

#[test]
fn crlf_cr_without_lf() {
    assert_eq!(find_crlf(b"\rx"), None);
    assert_eq!(CrlfResult::scan(b"\rx"), CrlfResult::NotFound(2));
}

#[test]
fn crlf_multiple_lines() {
    let data = b"line1\r\nline2\r\nline3\r\n";
    assert_eq!(find_crlf(data), Some(5));

    // After consuming first line, find next
    assert_eq!(find_crlf(&data[7..]), Some(5));
}

// =============================================================================
// Accept/Unaccept Reversibility
// =============================================================================

proptest! {
    /// accept(n) followed by unaccept(n) restores cursor position
    #[test]
    fn accept_unaccept_reversibility(
        data in prop::collection::vec(any::<u8>(), 1..100),
        accept_len in 1usize..50
    ) {
        let stream = SliceStream::new(&data);
        let borrow = stream.peek(None).expect("peek should work");

        let accept_amount = accept_len.min(data.len());
        let initial_pos = stream.consumed();

        // Accept some bytes
        let accepted = stream.accept(&borrow, Some(accept_amount));
        if accepted.is_err() {
            return Ok(()); // Skip invalid accepts
        }

        prop_assert_eq!(stream.consumed(), accept_amount);

        // Unaccept back to start
        let unaccepted = stream.unaccept(&borrow, Some(0));
        prop_assert!(unaccepted.is_ok(), "unaccept should succeed");
        prop_assert_eq!(stream.consumed(), initial_pos);
    }

    /// accept then unaccept with partial amounts
    #[test]
    fn accept_unaccept_partial(
        data in prop::collection::vec(any::<u8>(), 4..100),
        keep in 1usize..20
    ) {
        let stream = SliceStream::new(&data);
        let borrow = stream.peek(None).expect("peek should work");

        let keep_amount = keep.min(data.len() - 1);

        // Accept all
        stream.accept(&borrow, None).expect("accept should work");
        prop_assert_eq!(stream.consumed(), data.len());

        // Unaccept to keep only `keep_amount` bytes consumed
        let result = stream.unaccept(&borrow, Some(keep_amount));
        prop_assert!(result.is_ok());
        prop_assert_eq!(stream.consumed(), keep_amount);
    }

    /// Multiple accept calls accumulate correctly
    #[test]
    fn multiple_accepts(
        data in prop::collection::vec(any::<u8>(), 10..100),
        chunk1 in 1usize..5,
        chunk2 in 1usize..5
    ) {
        let stream = SliceStream::new(&data);

        let c1 = chunk1.min(data.len() / 2);
        let c2 = chunk2.min(data.len() / 2);

        let borrow1 = stream.peek(Some(c1)).expect("peek should work");
        stream.accept(&borrow1, None).expect("accept should work");
        prop_assert_eq!(stream.consumed(), c1);

        let borrow2 = stream.peek(Some(c2)).expect("peek should work");
        stream.accept(&borrow2, None).expect("accept should work");
        prop_assert_eq!(stream.consumed(), c1 + c2);
    }

    /// Cursor position/restore is consistent with accept
    #[test]
    fn position_restore_with_accept(
        data in prop::collection::vec(any::<u8>(), 5..100),
        accept_len in 1usize..20
    ) {
        let stream = SliceStream::new(&data);
        let accept_amount = accept_len.min(data.len());

        // Save position
        let saved = stream.position();
        prop_assert_eq!(stream.consumed(), 0);

        // Accept some bytes
        let borrow = stream.peek(Some(accept_amount)).expect("peek should work");
        stream.accept(&borrow, None).expect("accept should work");
        prop_assert_eq!(stream.consumed(), accept_amount);

        // Restore to saved position
        stream.restore_to(&saved).expect("restore should work");
        prop_assert_eq!(stream.consumed(), 0);
    }
}

// =============================================================================
// Accept/Unaccept edge cases
// =============================================================================

#[test]
fn accept_exactly_then_unaccept_exactly() {
    let data = b"hello";
    let stream = SliceStream::new(data);

    let borrow = stream.peek_exactly::<3>().expect("peek should work");
    stream.accept_exactly(&borrow).expect("accept should work");
    assert_eq!(stream.consumed(), 3);

    stream.unaccept_exactly(&borrow).expect("unaccept should work");
    assert_eq!(stream.consumed(), 0);
}

#[test]
fn unaccept_fails_if_cursor_not_at_end() {
    let data = b"hello";
    let stream = SliceStream::new(data);

    let borrow = stream.peek_exactly::<3>().expect("peek should work");
    stream.accept_exactly(&borrow).expect("accept should work");

    // Move cursor further
    stream.advance_by(1).expect("advance should work");

    // Unaccept should fail because cursor is not at borrow end
    let result = stream.unaccept_exactly(&borrow);
    assert!(result.is_err());
}
