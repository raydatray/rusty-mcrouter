use std::rc::Rc;

use rusty_mcrouter_core::DynRoute;
use tokio::sync::mpsc;

use crate::proxy::message::ProxyMessage;

pub struct Proxy {
    pub id: usize,
    pub route: Rc<dyn DynRoute>,
    pub rx: mpsc::Receiver<ProxyMessage>,
}
