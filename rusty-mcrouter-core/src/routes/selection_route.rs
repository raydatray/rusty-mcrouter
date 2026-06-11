use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::selectors::Selector;

use super::{DynRoute, Result, Route, RouteError};

pub struct SelectionRoute {
    children: Vec<Rc<dyn DynRoute>>,
    selector: Box<dyn Selector>,
}

impl SelectionRoute {
    pub fn new(children: Vec<Rc<dyn DynRoute>>, selector: Box<dyn Selector>) -> Self {
        SelectionRoute { children, selector }
    }
}

impl Route for SelectionRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        let idx = self.selector.select(routing_key(&req));

        // defensive bounds check: Ch3/Crc32 are bound to `n` and cannot exceed it,
        // but the trait-object seam can't prove that, so a buggy selector must
        // surface a route error instead of panicking the task.
        let child = self
            .children
            .get(idx)
            .ok_or(RouteError::SelectorOutOfRange {
                idx,
                len: self.children.len(),
            })?;

        child.route_dyn(req).await
    }
}

fn routing_key(req: &Request) -> &[u8] {
    let key = match req {
        Request::Get { key }
        | Request::Set { key, .. }
        | Request::Delete { key }
        | Request::Add { key, .. }
        | Request::Replace { key, .. }
        | Request::Append { key, .. }
        | Request::Prepend { key, .. }
        | Request::Incr { key, .. }
        | Request::Decr { key, .. }
        | Request::Touch { key, .. } => &key[..],
    };
    hash_stop(key)
}

/// mcrouter excludes everything from the `|#|` "hash stop" onward from the
/// routing key
/// - routing-prefix stripping is deferred until prefix routing
fn hash_stop(key: &[u8]) -> &[u8] {
    const MARKER: &[u8] = b"|#|";
    match key
        .windows(MARKER.len())
        .position(|window| window == MARKER)
    {
        Some(pos) => &key[..pos],
        None => key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn routing_key_extracts_single_key() {
        let req = Request::Delete {
            key: Bytes::from_static(b"user:1"),
        };
        assert_eq!(routing_key(&req), b"user:1");
    }

    #[test]
    fn routing_key_cuts_at_hash_stop() {
        let req = Request::Delete {
            key: Bytes::from_static(b"user:1|#|debuginfo"),
        };
        assert_eq!(routing_key(&req), b"user:1");
    }

    #[test]
    fn routing_key_hash_stop_makes_suffix_irrelevant() {
        // key and key|#|suffix must produce the same routing key
        let plain = Request::Get {
            key: Bytes::from_static(b"user:1"),
        };
        let suffixed = Request::Get {
            key: Bytes::from_static(b"user:1|#|x"),
        };
        assert_eq!(routing_key(&plain), routing_key(&suffixed));
    }

    #[test]
    fn hash_stop_handles_marker_edges() {
        assert_eq!(hash_stop(b"abc"), b"abc"); // no marker
        assert_eq!(hash_stop(b"a|#|b"), b"a"); // marker mid
        assert_eq!(hash_stop(b"|#|b"), b""); // marker at start -> empty prefix
        assert_eq!(hash_stop(b"a|#|"), b"a"); // marker at end
    }
}
