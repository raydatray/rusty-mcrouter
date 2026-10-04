use crate::{ProxyHandle, ThreadMode};

#[derive(Clone)]
pub struct ProxySet {
    proxies: Vec<ProxyHandle>,
}

impl ProxySet {
    pub fn new(proxies: Vec<ProxyHandle>) -> Self {
        assert!(!proxies.is_empty(), "proxyset empty");

        Self { proxies }
    }

    pub fn choose(&self, mode: ThreadMode, current_id: usize) -> ProxyHandle {
        let idx = match mode {
            ThreadMode::SameThread => current_id,
            ThreadMode::FixedRemote { proxy_id } => proxy_id % self.proxies.len(),
            ThreadMode::AffinitizedRemote => current_id, // todo - implement request-based affinity
        };

        self.proxies[idx].clone()
    }

    pub fn nth(&self, n: usize) -> &ProxyHandle {
        &self.proxies[n % self.proxies.len()]
    }
}
