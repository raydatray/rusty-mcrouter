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

        Rc::clone(child).route_dyn(req).await
    }
}

/// mcrouter hashes on the key with the `/region/cluster/` routing prefix
/// removed and everything from the `|#|` "hash stop" onward excluded; both
/// rules live in [`rusty_mcrouter_protocol::Key`].
fn routing_key(req: &Request) -> &[u8] {
    req.key().routing_key()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{req_delete, req_get};

    #[test]
    fn routing_key_extracts_single_key() {
        assert_eq!(routing_key(&req_delete(b"user:1")), b"user:1");
    }

    #[test]
    fn routing_key_cuts_at_hash_stop() {
        assert_eq!(routing_key(&req_delete(b"user:1|#|debuginfo")), b"user:1");
    }

    #[test]
    fn routing_key_strips_the_routing_prefix() {
        // `/region/cluster/key` and `key` must land on the same child.
        assert_eq!(
            routing_key(&req_get(b"/region/cluster/user:1")),
            routing_key(&req_get(b"user:1"))
        );
    }

    #[test]
    fn routing_key_hash_stop_makes_suffix_irrelevant() {
        assert_eq!(
            routing_key(&req_get(b"user:1")),
            routing_key(&req_get(b"user:1|#|x"))
        );
    }
}
