//! Line location for the Meta decoders: find one complete `\n`-terminated
//! line at the head of a streaming buffer without consuming anything. The
//! decoders frame a complete command or reply first, then parse it in a
//! single pass.

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
    }

    #[test]
    fn enforces_the_frame_limit_inclusive_of_the_terminator() {
        assert!(matches!(find_line(b"abc\n", 4), FindLine::Line { .. }));
        assert!(matches!(find_line(b"abcd\n", 4), FindLine::OverLimit));
        assert!(matches!(find_line(b"abcd", 4), FindLine::OverLimit)); // full, unterminated
    }
}
