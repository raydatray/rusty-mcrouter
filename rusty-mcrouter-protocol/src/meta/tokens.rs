//! Shared tokenization scaffolding for the Meta line parsers.
//!
//! Both decoders walk space-separated tokens and treat every token after the
//! positional ones as a single-letter flag with an inline argument. The
//! budget/shape/duplicate validation around that walk is identical across
//! commands; only the per-flag semantics differ, so those stay at the call
//! sites as plain `match` arms.

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
}
