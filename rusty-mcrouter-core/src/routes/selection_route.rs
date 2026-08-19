use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::routes::{DynRoute, Result, Route, RouteError};
use crate::selectors::Selector;
use crate::RouteContext;

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
    async fn route(&self, context: &RouteContext<'_>, request: Request) -> Result<Reply> {
        let idx = self.selector.select(routing_key(&request));

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

        child.route_dyn(context, request).await
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
    use rusty_mcrouter_protocol::test_support::{delete, get};

    #[test]
    fn routing_key_extracts_single_key() {
        assert_eq!(routing_key(&delete(b"user:1")), b"user:1");
    }

    #[test]
    fn routing_key_cuts_at_hash_stop() {
        assert_eq!(routing_key(&delete(b"user:1|#|debuginfo")), b"user:1");
    }

    #[test]
    fn routing_key_strips_the_routing_prefix() {
        // `/region/cluster/key` and `key` must land on the same child.
        assert_eq!(
            routing_key(&get(b"/region/cluster/user:1")),
            routing_key(&get(b"user:1"))
        );
    }

    #[test]
    fn routing_key_hash_stop_makes_suffix_irrelevant() {
        assert_eq!(
            routing_key(&get(b"user:1")),
            routing_key(&get(b"user:1|#|x"))
        );
    }
}
