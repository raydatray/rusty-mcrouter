use bytes::BytesMut;

use crate::{
    request::{Parsed, Request},
    ProtocolError, Result,
};

mod add;
mod append;
mod decr;
mod delete;
mod get;
mod incr;
mod prepend;
mod replace;
mod reply;
mod set;
mod shared;
mod touch;

pub use reply::parse_reply;

use shared::read_line;

// TODO: parser is stateless and re-parses headers on every partial-read call.
// mcrouter's McServerAsciiParser holds in-progress state across calls. Make
// this stateful via a RequestParser struct holding ParseState — low priority,
// costs ~µs + 1 small alloc per partial read on multi-fragment set bodies.

pub fn parse_request(buf: &mut BytesMut) -> Result<Option<Parsed>> {
    let Some((line_end, total)) = read_line(buf, 0) else {
        return Ok(None);
    };
    let eol_idx = total - 1;

    let cmd = command_name(&buf[..line_end]);

    let single = match cmd {
        b"get" => return get::parse_request(buf, eol_idx),
        b"set" => set::parse_request(buf, eol_idx, line_end),
        b"add" => add::parse_request(buf, eol_idx, line_end),
        b"replace" => replace::parse_request(buf, eol_idx, line_end),
        b"append" => append::parse_request(buf, eol_idx, line_end),
        b"prepend" => prepend::parse_request(buf, eol_idx, line_end),
        b"incr" => incr::parse_request(buf, eol_idx),
        b"decr" => decr::parse_request(buf, eol_idx),
        b"touch" => touch::parse_request(buf, eol_idx),
        b"delete" => delete::parse_request(buf, eol_idx),
        _ => {
            let _ = buf.split_to(total);
            Err(ProtocolError::Malformed("unknown command"))
        }
    };

    Ok(single?.map(Parsed::One))
}

fn command_name(header: &[u8]) -> &[u8] {
    let end = header
        .iter()
        .position(|&b| b == b' ')
        .unwrap_or(header.len());
    &header[..end]
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn parse_request_returns_none_when_no_newline() {
        let mut empty = BytesMut::new();
        assert!(matches!(parse_request(&mut empty), Ok(None)));
        assert!(empty.is_empty());

        let mut partial = BytesMut::from(&b"get fo"[..]);
        assert!(matches!(parse_request(&mut partial), Ok(None)));
        assert_eq!(partial.as_ref(), b"get fo");
    }

    #[test]
    fn parse_request_strips_lf_and_crlf() {
        let cases: &[&[u8]] = &[b"get foo\n", b"get foo\r\n"];

        cases.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            let req = parse_request(&mut buf).unwrap().unwrap();
            assert_eq!(
                req,
                Parsed::One(Request::Get {
                    key: Bytes::from_static(b"foo")
                })
            );
            assert!(buf.is_empty());
        });
    }

    #[test]
    fn parse_request_consumes_one_frame_at_a_time() {
        let mut buf = BytesMut::from(&b"get foo\nget bar\n"[..]);

        let first = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            first,
            Parsed::One(Request::Get {
                key: Bytes::from_static(b"foo")
            })
        );
        assert_eq!(buf.as_ref(), b"get bar\n");

        let second = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            second,
            Parsed::One(Request::Get {
                key: Bytes::from_static(b"bar")
            })
        );
        assert!(buf.is_empty());

        assert!(matches!(parse_request(&mut buf), Ok(None)));
    }

    #[test]
    fn parse_request_propagates_errors_and_consumes_malformed_lines() {
        let mut unknown = BytesMut::from(&b"WAT foo\n"[..]);
        assert!(matches!(
            parse_request(&mut unknown),
            Err(ProtocolError::Malformed("unknown command"))
        ));
        assert!(unknown.is_empty());

        for terminator in [&b"\n"[..], &b"\r\n"[..]] {
            let mut buf = BytesMut::from(terminator);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed("unknown command"))
            ));
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn parse_request_rejects_uppercase_command() {
        // Commands are case-sensitive per the memcached text protocol.
        let mut buf = BytesMut::from(&b"GET foo\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("unknown command"))
        ));
    }

    #[test]
    fn parse_request_rejects_leading_whitespace() {
        let mut buf = BytesMut::from(&b" foo\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("unknown command"))
        ));
    }

    #[test]
    fn parse_request_pipelined_set_then_get() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nbar\r\nget foo\r\n"[..]);
        let first = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            first,
            Parsed::One(Request::Set {
                key: Bytes::from_static(b"foo"),
                flags: 0,
                exptime: 0,
                data: Bytes::from_static(b"bar"),
            })
        );
        assert_eq!(buf.as_ref(), b"get foo\r\n");

        let second = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            second,
            Parsed::One(Request::Get {
                key: Bytes::from_static(b"foo")
            })
        );
        assert!(buf.is_empty());
    }
}
