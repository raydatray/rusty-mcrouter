mod arithmetic;
mod debug;
mod delete;
mod get;
mod store;

use crate::Key;

pub use arithmetic::{
    ArithmeticMode, ArithmeticRequest, ArithmeticTemporalInstruction,
    ArithmeticTemporalInstructions,
};
pub use debug::DebugRequest;
pub use delete::DeleteRequest;
pub use get::{GetRequest, GetTemporalInstruction, GetTemporalInstructions};
pub use store::{StoreMode, StoreRequest};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RequestKind {
    Get,
    Store,
    Delete,
    Arithmetic,
    Debug,
}

impl RequestKind {
    pub const ALL: [Self; 5] = [
        Self::Get,
        Self::Store,
        Self::Delete,
        Self::Arithmetic,
        Self::Debug,
    ];

    pub const COUNT: usize = Self::ALL.len();

    pub const fn meta_command(self) -> &'static str {
        match self {
            Self::Get => "mg",
            Self::Store => "ms",
            Self::Delete => "md",
            Self::Arithmetic => "ma",
            Self::Debug => "me",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Arithmetic(ArithmeticRequest),
    Delete(DeleteRequest),
    Debug(DebugRequest),
    Get(GetRequest),
    Store(StoreRequest),
}

impl Request {
    pub const fn kind(&self) -> RequestKind {
        match self {
            Self::Get(_) => RequestKind::Get,
            Self::Store(_) => RequestKind::Store,
            Self::Delete(_) => RequestKind::Delete,
            Self::Arithmetic(_) => RequestKind::Arithmetic,
            Self::Debug(_) => RequestKind::Debug,
        }
    }

    pub fn key(&self) -> &Key {
        match self {
            Self::Arithmetic(request) => &request.key,
            Self::Delete(request) => &request.key,
            Self::Debug(request) => &request.key,
            Self::Get(request) => &request.key,
            Self::Store(request) => &request.key,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{delete, get, request, store};

    use super::*;

    #[test]
    fn request_kind_matches_every_request_variant() {
        let cases = [
            (get(b"key"), RequestKind::Get, "mg"),
            (store(b"key", b"value"), RequestKind::Store, "ms"),
            (delete(b"key"), RequestKind::Delete, "md"),
            (request(b"ma key v\r\n"), RequestKind::Arithmetic, "ma"),
            (request(b"me key\r\n"), RequestKind::Debug, "me"),
        ];

        for (request, expected, command) in cases {
            assert_eq!(request.kind(), expected);
            assert_eq!(expected.meta_command(), command);
        }
    }

    #[test]
    fn all_covers_the_index_range() {
        assert_eq!(RequestKind::COUNT, 5);
        for (index, kind) in RequestKind::ALL.into_iter().enumerate() {
            assert_eq!(kind as usize, index);
        }
    }
}
