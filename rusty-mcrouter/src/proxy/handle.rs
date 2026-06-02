use tokio::sync::mpsc;

use crate::proxy::message::ProxyMessage;

#[derive(Clone)]
pub struct ProxyHandle {
    id: usize,
    tx: mpsc::Sender<ProxyMessage>,
}
