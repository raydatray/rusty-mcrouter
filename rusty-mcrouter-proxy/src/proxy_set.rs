use rusty_mcrouter_protocol::Request;

use crate::{config::ThreadMode, handle::ProxyHandle};

#[derive(Clone)]
pub struct ProxySet {
    proxies: Vec<ProxyHandle>,
}

impl ProxySet {
    pub fn new(proxies: Vec<ProxyHandle>) -> Self {
        assert!(!proxies.is_empty(), "proxyset empty");

        Self { proxies }
    }

    pub fn choose(&self, mode: ThreadMode, current_id: usize, _req: &Request) -> ProxyHandle {
        let idx = match mode {
            ThreadMode::SameThread => current_id,
            ThreadMode::FixedRemote { proxy_id } => proxy_id % self.proxies.len(),
            ThreadMode::AffinitizedRemote => current_id, // todo - actually hash on the request im just lazy now
        };

        self.proxies[idx].clone()
    }
}
