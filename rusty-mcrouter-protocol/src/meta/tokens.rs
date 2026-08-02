//! Read-side primitives shared by the Meta line parsers: locating one
//! complete line at the head of a streaming buffer, splitting it into
//! tokens, walking flag tokens, and parsing decimal numbers.
//!
//! Both decoders walk space-separated tokens and treat every token after the
//! positional ones as a single-letter flag with an inline argument. The
//! budget/shape/duplicate validation around that walk is identical across
//! commands; only the per-flag semantics differ, so those stay at the call
//! sites as plain `match` arms.

use std::str::FromStr;

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

/// Splits one command or reply line into its non-empty tokens. Runs of
/// spaces collapse, matching memcached's tokenizer.
pub(super) fn split_tokens(line: &[u8]) -> impl Iterator<Item = &[u8]> + Clone {
    line.split(|byte| *byte == b' ')
        .filter(|token| !token.is_empty())
}

/// Walks flag tokens, yielding `(letter, argument)` pairs after the checks
/// every Meta command shares: an optional token budget (memcached's
/// "options flags are too long", counted before any validation), a leading
/// ASCII letter, and letter-level duplicate rejection.
pub(super) fn flags<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
    budget: FlagBudget,
) -> impl Iterator<Item = Result<(u8, &'a [u8]), FlagError>> {
    let mut seen = SeenFlags::default();
    let mut remaining = match budget {
        FlagBudget::Tokens(count) => Some(count),
        FlagBudget::Unlimited => None,
    };
    tokens.map(move |token| {
        if let Some(remaining) = &mut remaining {
            if *remaining == 0 {
                return Err(FlagError::OverBudget);
            }
            *remaining -= 1;
        }
        let Some((&flag, argument)) = token.split_first() else {
            return Err(FlagError::InvalidToken);
        };
        if !flag.is_ascii_alphabetic() {
            return Err(FlagError::InvalidToken);
        }
        if !seen.insert(flag) {
            return Err(FlagError::Duplicate);
        }
        Ok((flag, argument))
    })
}

/// A bare flag carried an argument.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct UnexpectedArgument;

/// Validates that a bare flag token carries no argument.
pub(super) fn require_no_argument(argument: &[u8]) -> Result<(), UnexpectedArgument> {
    if argument.is_empty() {
        Ok(())
    } else {
        Err(UnexpectedArgument)
    }
}

#[derive(Clone, Copy)]
pub(super) enum FlagBudget {
    /// At most this many flag tokens; one more is an error.
    Tokens(usize),
    /// memcached's `ma` and `me` parsers have no token budget.
    Unlimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlagError {
    OverBudget,
    InvalidToken,
    Duplicate,
}

/// The token is not a decimal number that fits the requested width.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct BadNumber;

/// Parses one decimal token. std's integer grammar — an optional sign
/// followed by ASCII digits, overflow-checked, no whitespace — matches
/// memcached's accepted set exactly.
fn parse_number<T: FromStr>(raw: &[u8]) -> Result<T, BadNumber> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .ok_or(BadNumber)
}

pub(super) fn parse_u64(raw: &[u8]) -> Result<u64, BadNumber> {
    parse_number(raw)
}

pub(super) fn parse_u32(raw: &[u8]) -> Result<u32, BadNumber> {
    parse_number(raw)
}

pub(super) fn parse_usize(raw: &[u8]) -> Result<usize, BadNumber> {
    parse_number(raw)
}

pub(super) fn parse_i32(raw: &[u8]) -> Result<i32, BadNumber> {
    parse_number(raw)
}

pub(super) fn parse_i64(raw: &[u8]) -> Result<i64, BadNumber> {
    parse_number(raw)
}

/// A 256-bit set tracking which flag letters appeared on a line.
#[derive(Default)]
struct SeenFlags([u64; 4]);

impl SeenFlags {
    /// Returns true when `flag` was not already present.
    fn insert(&mut self, flag: u8) -> bool {
        let word = usize::from(flag / 64);
        let bit = 1_u64 << (flag % 64);
        let inserted = self.0[word] & bit == 0;
        self.0[word] |= bit;
        inserted
    }
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

    #[test]
    fn detects_duplicate_values_across_all_words() {
        let mut seen = SeenFlags::default();
        for flag in [0, 63, 64, 127, 128, 191, 192, 255] {
            assert!(seen.insert(flag));
            assert!(!seen.insert(flag));
        }
    }

    #[test]
    fn split_tokens_collapses_space_runs() {
        assert_eq!(
            split_tokens(b"mg  key   v").collect::<Vec<_>>(),
            vec![b"mg".as_slice(), b"key".as_slice(), b"v".as_slice()]
        );
        assert_eq!(split_tokens(b"   ").count(), 0);
    }

    #[test]
    fn flags_validate_shape_budget_and_duplicates() {
        let collect =
            |line: &'static [u8], budget| flags(split_tokens(line), budget).collect::<Vec<_>>();

        assert_eq!(
            collect(b"v Otag", FlagBudget::Unlimited),
            vec![Ok((b'v', b"".as_slice())), Ok((b'O', b"tag".as_slice()))]
        );
        assert_eq!(
            collect(b"v v", FlagBudget::Unlimited),
            vec![Ok((b'v', b"".as_slice())), Err(FlagError::Duplicate)]
        );
        assert_eq!(
            collect(b"1", FlagBudget::Unlimited),
            vec![Err(FlagError::InvalidToken)]
        );
        assert_eq!(
            collect(b"a b c", FlagBudget::Tokens(2)),
            vec![
                Ok((b'a', b"".as_slice())),
                Ok((b'b', b"".as_slice())),
                Err(FlagError::OverBudget),
            ]
        );
    }

    #[test]
    fn parses_numeric_boundaries() {
        assert_eq!(parse_u64(b"18446744073709551615"), Ok(u64::MAX));
        assert_eq!(parse_u64(b"18446744073709551616"), Err(BadNumber));
        assert_eq!(parse_u64(b"+123"), Ok(123)); // memcached accepts a bare sign
        assert_eq!(parse_i32(b"-2147483648"), Ok(i32::MIN));
        assert_eq!(parse_i32(b"2147483647"), Ok(i32::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Ok(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775807"), Ok(i64::MAX));
    }

    #[test]
    fn rejects_empty_signs_and_non_digits() {
        for raw in [b"".as_slice(), b"+", b"-", b"1x", b" 1", b"++1", b"+-1"] {
            assert_eq!(parse_u64(raw), Err(BadNumber));
            assert_eq!(parse_i64(raw), Err(BadNumber));
        }
    }
}
