use std::str::FromStr;

use memchr::memchr;

pub enum FindLine {
    Incomplete,
    OverLimit,
    /// `end` excludes the `\r\n` or `\n` terminator, `frame_len` includes it.
    Line {
        end: usize,
        frame_len: usize,
    },
}

/// locates the first `\n`-terminated line in src, bounded by `max_frame` len
pub fn find_line(src: &[u8], max_frame: usize) -> FindLine {
    let Some(new_line) = memchr(b'\n', src) else {
        if src.len() >= max_frame {
            return FindLine::OverLimit;
        }

        return FindLine::Incomplete;
    };

    let frame_len = new_line + 1;
    if frame_len > max_frame {
        return FindLine::OverLimit;
    }

    let end = if new_line > 0 && src[new_line - 1] == b'\r' {
        new_line - 1
    } else {
        new_line
    };

    FindLine::Line { end, frame_len }
}

/// splits one command or reply line into its non-empty tokens
/// slices of spaces collapse into nothing, matching memcached's tokenizer
pub fn split_tokens(line: &[u8]) -> impl Iterator<Item = &[u8]> + Clone {
    line.split(|byte| *byte == b' ')
        .filter(|token| !token.is_empty())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Flag<'a> {
    pub(super) letter: u8,
    pub(super) argument: &'a [u8],
}
/// walks flag tokens yielding `Flags` with an optional maximum flag count
pub fn flags<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
    budget: FlagBudget,
) -> impl Iterator<Item = Result<Flag<'a>, FlagError>> {
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

        let Some((&letter, argument)) = token.split_first() else {
            return Err(FlagError::InvalidToken);
        };

        if !letter.is_ascii_alphabetic() {
            return Err(FlagError::InvalidToken);
        }

        if !seen.insert(letter) {
            return Err(FlagError::Duplicate);
        }

        Ok(Flag { letter, argument })
    })
}

#[derive(Debug, Eq, PartialEq)]
pub struct UnexpectedFlagArgument;

pub fn require_no_argument(argument: &[u8]) -> Result<(), UnexpectedFlagArgument> {
    if argument.is_empty() {
        Ok(())
    } else {
        Err(UnexpectedFlagArgument)
    }
}

#[derive(Clone, Copy)]
pub enum FlagBudget {
    Tokens(usize),
    /// memcached's `ma` and `me` parsers have no token budget
    Unlimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlagError {
    OverBudget,
    InvalidToken,
    Duplicate,
}

#[derive(Debug, Eq, PartialEq)]
pub struct BadNumberError;

fn parse_number<T>(raw: &[u8]) -> Result<T, BadNumberError>
where
    T: FromStr,
{
    let text = std::str::from_utf8(raw).map_err(|_| BadNumberError)?;

    text.parse().map_err(|_| BadNumberError)
}

pub fn parse_u64(raw: &[u8]) -> Result<u64, BadNumberError> {
    parse_number(raw)
}

pub fn parse_u32(raw: &[u8]) -> Result<u32, BadNumberError> {
    parse_number(raw)
}

pub fn parse_usize(raw: &[u8]) -> Result<usize, BadNumberError> {
    parse_number(raw)
}

pub fn parse_i32(raw: &[u8]) -> Result<i32, BadNumberError> {
    parse_number(raw)
}

pub fn parse_i64(raw: &[u8]) -> Result<i64, BadNumberError> {
    parse_number(raw)
}

/// a 64-bit set tracking which flag letters appeared on a line
/// flags are ASCII which maximally 57 entries
#[derive(Default)]
struct SeenFlags(u64);

impl SeenFlags {
    /// returns true when `flag` was not already present
    fn insert(&mut self, flag: u8) -> bool {
        assert!(
            flag.is_ascii_alphabetic(),
            "flags must be ASCII, and are validated upstream"
        );

        let bit = 1_u64 << (flag - b'A');
        let inserted = self.0 & bit == 0;
        self.0 |= bit;

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
            vec![
                Ok(Flag {
                    letter: b'v',
                    argument: b"",
                }),
                Ok(Flag {
                    letter: b'O',
                    argument: b"tag",
                }),
            ]
        );
        assert_eq!(
            collect(b"v v", FlagBudget::Unlimited),
            vec![
                Ok(Flag {
                    letter: b'v',
                    argument: b"",
                }),
                Err(FlagError::Duplicate),
            ]
        );
        assert_eq!(
            collect(b"1", FlagBudget::Unlimited),
            vec![Err(FlagError::InvalidToken)]
        );
        assert_eq!(
            collect(b"a b c", FlagBudget::Tokens(2)),
            vec![
                Ok(Flag {
                    letter: b'a',
                    argument: b"",
                }),
                Ok(Flag {
                    letter: b'b',
                    argument: b"",
                }),
                Err(FlagError::OverBudget),
            ]
        );
    }

    #[test]
    fn parses_numeric_boundaries() {
        assert_eq!(parse_u64(b"18446744073709551615"), Ok(u64::MAX));
        assert_eq!(parse_u64(b"18446744073709551616"), Err(BadNumberError));
        assert_eq!(parse_u64(b"+123"), Ok(123)); // memcached accepts a bare sign
        assert_eq!(parse_i32(b"-2147483648"), Ok(i32::MIN));
        assert_eq!(parse_i32(b"2147483647"), Ok(i32::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Ok(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775807"), Ok(i64::MAX));
    }

    #[test]
    fn rejects_empty_signs_and_non_digits() {
        for raw in [b"".as_slice(), b"+", b"-", b"1x", b" 1", b"++1", b"+-1"] {
            assert_eq!(parse_u64(raw), Err(BadNumberError));
            assert_eq!(parse_i64(raw), Err(BadNumberError));
        }
    }

    #[test]
    fn detects_duplicates_for_all_ascii_letters() {
        let mut seen = SeenFlags::default();

        for flag in (b'A'..=b'Z').chain(b'a'..=b'z') {
            assert!(seen.insert(flag));
            assert!(!seen.insert(flag));
        }
    }
}
