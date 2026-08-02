use bytes::Bytes;

use crate::bounded_list::BoundedList;

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

/// `mg` has at most eight distinct client-visible output tokens.
pub type MetaOutputOrder = BoundedList<MetaOutputToken, 8>;

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
