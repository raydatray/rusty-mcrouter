use std::rc::Rc;

use bytes::Bytes;
use rusty_mcrouter_core::DynRoute;
use rusty_mcrouter_protocol::Reply;
use tokio::sync::mpsc;

use crate::proxy::message::{ProxyMessage, ProxyRequest};

pub struct Proxy {
    pub id: usize,
    pub route: Rc<dyn DynRoute>,
    pub rx: mpsc::Receiver<ProxyMessage>,
}

impl Proxy {
    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ProxyMessage::Request(req) => {
                    Self::spawn_request(Rc::clone(&self.route), req);
                }
                ProxyMessage::Shutdown => break,
            }
        }
    }

    pub fn spawn_request(route: Rc<dyn DynRoute>, req: ProxyRequest) {
        tokio::task::spawn_local(async move {
            let reply = route
                .route_dyn(req.request)
                .await
                .unwrap_or_else(|_| Reply::ServerError(Bytes::from_static(b"backend unavailable")));

            let _ = req.reply_tx.send(reply);
        });
    }
}
