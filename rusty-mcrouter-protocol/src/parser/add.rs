use bytes::BytesMut;

use crate::{request::Request, Result};

use super::shared::{parse_storage_request, StorageRequest};

const ADD_HEADER_HELP: &str = "add requires <key> <flags> <exptime> <bytes>";

pub(super) fn parse_request(
    buf: &mut BytesMut,
    eol_idx: usize,
    line_text_end: usize,
) -> Result<Option<Request>> {
    let Some(StorageRequest {
        key,
        flags,
        exptime,
        data,
    }) = parse_storage_request(
        buf,
        eol_idx,
        line_text_end,
        b"add ",
        ADD_HEADER_HELP,
        "add: unexpected extra token in header",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Request::Add {
        key,
        flags,
        exptime,
        data,
    }))
}
