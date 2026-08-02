use bytes::Bytes;

use crate::errors::ParseError;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetaReplyPlan {
    pub quiet: MetaQuietPolicy,        // q
    pub opaque: Option<Bytes>,         // O<token>
    pub external_key: Option<Bytes>,   // k, and the key returned by me
    pub key_encoding: KeyEncoding,     // b
    pub output_order: MetaOutputOrder, // c/f/s/t/h/l/O/k response order
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetaQuietPolicy {
    #[default]
    None,
    SuppressMiss,    // mg q suppresses EN
    SuppressSuccess, // ms/md/ma q suppresses successful HD or VA
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum KeyEncoding {
    #[default]
    Text,
    Base64, // b
}

// `mg` has at most eight unique client-visible output tokens.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetaOutputOrder {
    tokens: [Option<MetaOutputToken>; 8],
    len: u8,
}

impl MetaOutputOrder {
    pub(crate) fn push(&mut self, token: MetaOutputToken) -> Result<(), ParseError> {
        let slot = self
            .tokens
            .get_mut(usize::from(self.len))
            .ok_or(ParseError::TooManyFlags)?;
        *slot = Some(token);
        self.len += 1;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &MetaOutputToken> {
        self.tokens[..usize::from(self.len)]
            .iter()
            .map(|token| token.as_ref().expect("dense output order"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaOutputToken {
    Cas,         // c
    ClientFlags, // f
    Size,        // s
    Ttl,         // t
    HitState,    // h
    LastAccess,  // l
    Opaque,      // O<token>
    Key,         // k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_order_preserves_insertion_order() {
        let mut order = MetaOutputOrder::default();
        order.push(MetaOutputToken::Opaque).unwrap();
        order.push(MetaOutputToken::Size).unwrap();
        order.push(MetaOutputToken::Cas).unwrap();

        assert_eq!(
            order.iter().copied().collect::<Vec<_>>(),
            vec![
                MetaOutputToken::Opaque,
                MetaOutputToken::Size,
                MetaOutputToken::Cas,
            ]
        );
    }

    #[test]
    fn output_order_rejects_more_than_eight_tokens() {
        let mut order = MetaOutputOrder::default();
        for token in [
            MetaOutputToken::Cas,
            MetaOutputToken::ClientFlags,
            MetaOutputToken::Size,
            MetaOutputToken::Ttl,
            MetaOutputToken::HitState,
            MetaOutputToken::LastAccess,
            MetaOutputToken::Opaque,
            MetaOutputToken::Key,
        ] {
            order.push(token).unwrap();
        }

        assert_eq!(
            order.push(MetaOutputToken::Cas),
            Err(ParseError::TooManyFlags)
        );
    }
}
