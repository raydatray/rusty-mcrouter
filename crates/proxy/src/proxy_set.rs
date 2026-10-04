use crate::ProxyHandle;

/// Worker placement for a connection's request mailbox. All requests use the
/// mailbox, including requests served by the connection's own worker.
#[derive(Clone, Copy)]
pub enum ThreadMode {
    SameThread,
    // todo - thread modes: constructed once dispatch policy is configurable
    #[allow(dead_code)]
    FixedRemote {
        proxy_id: usize,
    },
    #[allow(dead_code)]
    AffinitizedRemote,
}

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
