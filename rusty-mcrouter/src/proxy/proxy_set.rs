use crate::proxy::handle::ProxyHandle;

#[derive(Clone)]
pub struct ProxySet {
    proxies: Vec<ProxyHandle>,
}
