use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::selectors::Selector;

use super::{DynRoute, Result, Route};

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

        self.children[idx].route_dyn(req).await
    }
}

fn routing_key(req: &Request) -> &[u8] {
    match req {
        Request::Set { key, .. }
        | Request::Delete { key }
        | Request::Add { key, .. }
        | Request::Replace { key, .. }
        | Request::Append { key, .. }
        | Request::Prepend { key, .. }
        | Request::Incr { key, .. }
        | Request::Decr { key, .. }
        | Request::Touch { key, .. } => &key[..],
        // hash-routing and multiget are independent (see docs/design/multiget.md):
        // until the routed Get is single-key - just take the single key.
        // this should never be emnpty so it wont explode
        Request::Get { keys } => &keys[0],
    }
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
