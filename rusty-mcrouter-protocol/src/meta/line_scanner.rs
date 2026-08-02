//! Incremental scanning for one `\n`-terminated frame at the head of a
//! streaming buffer. Both Meta decoders resume from a `scanned` cursor kept
//! in their state machine, so fragmented reads never rescan bytes.

use bytes::{Bytes, BytesMut};
use memchr::memchr;

/// The result of locating one complete line at the head of a buffer
/// without consuming anything.
pub(super) enum FindLine {
    /// No terminator buffered yet; the caller should wait for more bytes.
    Incomplete,
    /// The complete line (or the unterminated prefix) exceeds the frame
    /// limit. The buffer is left untouched for diagnostics.
    OverLimit,
    /// One complete line at the head of the buffer. `end` excludes the
    /// `\r\n` / `\n` terminator; `frame_len` includes it.
    Line { end: usize, frame_len: usize },
}

/// Locates the first `\n`-terminated line in `src`, bounded by `max_frame`
/// bytes including the terminator. Pure: consumes nothing and keeps no
/// cursor, so fragmented reads rescan the (bounded) unterminated prefix.
pub(super) fn find_line(src: &[u8], max_frame: usize) -> FindLine {
    let Some(newline) = memchr(b'\n', src) else {
        if src.len() >= max_frame {
            return FindLine::OverLimit;
        }
        return FindLine::Incomplete;
    };

    let frame_len = newline + 1;
    if frame_len > max_frame {
        return FindLine::OverLimit;
    }

    let end = if newline > 0 && src[newline - 1] == b'\r' {
        newline - 1
    } else {
        newline
    };
    FindLine::Line { end, frame_len }
}

pub(super) enum LineScan {
    /// No terminator buffered yet. `scanned` is the resume cursor to store;
    /// the buffer is untouched.
    Incomplete { scanned: usize },
    /// The complete line (or the unterminated prefix) exceeds `max_frame`.
    /// The buffer is left untouched for diagnostics.
    OverLimit,
    /// One complete frame split off the front of the buffer.
    Frame(LineFrame),
}

pub(super) struct LineFrame {
    pub bytes: Bytes,
    /// Line length excluding the `\r\n` / `\n` terminator.
    pub line_end: usize,
    /// The buffer's capacity before the split. The request decoder uses this
    /// to decide whether zero-copy key slices may retain the frame without
    /// pinning a large allocation.
    pub buffer_capacity: usize,
}

/// Scans from `scanned` (resetting a cursor gone stale through buffer
/// replacement) for the next newline, bounded by `max_frame` bytes
/// including the terminator.
pub(super) fn scan_line(mut scanned: usize, src: &mut BytesMut, max_frame: usize) -> LineScan {
    if scanned > src.len() {
        scanned = 0;
    }

    let Some(newline) = memchr(b'\n', &src[scanned..]).map(|offset| scanned + offset) else {
        if src.len() >= max_frame {
            return LineScan::OverLimit;
        }
        return LineScan::Incomplete { scanned: src.len() };
    };

    let frame_len = newline + 1;
    if frame_len > max_frame {
        return LineScan::OverLimit;
    }

    let line_end = if newline > 0 && src[newline - 1] == b'\r' {
        newline - 1
    } else {
        newline
    };
    let buffer_capacity = src.capacity();
    LineScan::Frame(LineFrame {
        bytes: src.split_to(frame_len).freeze(),
        line_end,
        buffer_capacity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_lines_without_consuming() {
        assert!(matches!(find_line(b"EN", 16), FindLine::Incomplete));
        assert!(matches!(
            find_line(b"EN\r\nHD", 16),
            FindLine::Line {
                end: 2,
                frame_len: 4,
            }
        ));
        assert!(matches!(
            find_line(b"HD\n", 16), // bare LF accepted
            FindLine::Line {
                end: 2,
                frame_len: 3,
            }
        ));
        assert!(matches!(find_line(b"abc\n", 4), FindLine::Line { .. }));
        assert!(matches!(find_line(b"abcd\n", 4), FindLine::OverLimit));
        assert!(matches!(find_line(b"abcd", 4), FindLine::OverLimit)); // full, unterminated
    }

    fn frame(scan: LineScan) -> LineFrame {
        match scan {
            LineScan::Frame(frame) => frame,
            LineScan::Incomplete { .. } => panic!("expected a frame, scan was incomplete"),
            LineScan::OverLimit => panic!("expected a frame, scan was over the limit"),
        }
    }

    #[test]
    fn resumes_across_incomplete_reads_and_trims_the_terminator() {
        let mut src = BytesMut::from(&b"EN"[..]);
        let LineScan::Incomplete { scanned } = scan_line(0, &mut src, 16) else {
            panic!("expected incomplete");
        };
        assert_eq!(scanned, 2);

        src.extend_from_slice(b"\r\nHD");
        let first = frame(scan_line(scanned, &mut src, 16));
        assert_eq!(&first.bytes[..first.line_end], b"EN");
        assert_eq!(src, b"HD".as_slice());

        src.extend_from_slice(b"\n");
        let second = frame(scan_line(0, &mut src, 16));
        assert_eq!(&second.bytes[..second.line_end], b"HD"); // bare LF accepted
        assert!(src.is_empty());
    }

    #[test]
    fn enforces_the_frame_limit_inclusive_of_the_terminator() {
        let mut src = BytesMut::from(&b"abc\n"[..]);
        assert!(matches!(scan_line(0, &mut src, 4), LineScan::Frame(_)));

        let mut src = BytesMut::from(&b"abcd\n"[..]);
        assert!(matches!(scan_line(0, &mut src, 4), LineScan::OverLimit));
        assert_eq!(src, b"abcd\n".as_slice()); // untouched

        let mut src = BytesMut::from(&b"abcd"[..]); // full, unterminated
        assert!(matches!(scan_line(0, &mut src, 4), LineScan::OverLimit));
    }

    #[test]
    fn resets_a_stale_cursor() {
        let mut src = BytesMut::from(&b"a\n"[..]);
        let scanned_past_buffer = 10;
        let scanned = frame(scan_line(scanned_past_buffer, &mut src, 16));
        assert_eq!(&scanned.bytes[..scanned.line_end], b"a");
    }
}
