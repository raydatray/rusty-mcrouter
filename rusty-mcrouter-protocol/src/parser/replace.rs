use bytes::BytesMut;

use crate::{request::Request, Result};

use super::shared::{parse_storage_request, StorageRequest};

const REPLACE_HEADER_HELP: &str = "replace requires <key> <flags> <exptime> <bytes>";

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
        b"replace ",
        REPLACE_HEADER_HELP,
        "replace: unexpected extra token in header",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Request::Replace {
        key,
        flags,
        exptime,
        data,
    }))
}
