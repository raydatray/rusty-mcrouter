use bytes::BytesMut;

use crate::{request::Request, Result};

use super::shared::{parse_storage_request, StorageRequest};

const PREPEND_HEADER_HELP: &str = "prepend requires <key> <flags> <exptime> <bytes>";

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
        b"prepend ",
        PREPEND_HEADER_HELP,
        "prepend: unexpected extra token in header",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Request::Prepend {
        key,
        flags,
        exptime,
        data,
    }))
}
