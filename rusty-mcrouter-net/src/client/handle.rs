use super::command::ClientCommand;
use super::config::ClientConfig;
use super::connection::ClientConnection;
use crate::{NetError, Result, TimeoutPhase};
use rusty_mcrouter_protocol::{Reply, Request};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};

// cloneable producer handle for a single memcache connection.
// all clones share one socket-owning ClientConnection task
#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<ClientCommand>,
    reply_timeout: Option<Duration>,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        Self::connect_with_config(addr, ClientConfig::default()).await
    }

    pub async fn connect_with_config(addr: impl ToSocketAddrs, cfg: ClientConfig) -> Result<Self> {
        let stream = match cfg.connect_timeout {
            Some(dur) => match tokio::time::timeout(dur, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => return Err(NetError::Io(e)),
                Err(_elapsed) => {
                    return Err(NetError::Timeout {
                        phase: TimeoutPhase::Connect,
                    })
                }
            },
            None => TcpStream::connect(addr).await?,
        };
        let (tx, rx) = mpsc::channel(cfg.max_pending);

        let connection = ClientConnection::new(stream, rx, &cfg);
        tokio::spawn(connection.run());

        Ok(Self {
            tx,
            reply_timeout: cfg.reply_timeout,
        })
    }

    pub async fn send(&self, request: Request) -> Result<Reply> {
        match self.reply_timeout {
            Some(dur) => match tokio::time::timeout(dur, self.send_inner(request)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(NetError::Timeout {
                    phase: TimeoutPhase::Reply,
                }),
            },
            None => self.send_inner(request).await,
        }
    }

    async fn send_inner(&self, request: Request) -> Result<Reply> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(ClientCommand { request, reply_tx })
            .await
            .map_err(|_| NetError::ClientClosed)?;

        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(NetError::ClientClosed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{scripted_backend, Step};
    use bytes::Bytes;
    use rusty_mcrouter_protocol::Reply;

    fn get(key: &'static [u8]) -> Request {
        Request::Get {
            key: Bytes::from_static(key),
        }
    }

    fn hit_data(reply: Reply) -> Bytes {
        let Reply::Get { mut hits } = reply else {
            panic!("expected Reply::Get, got {reply:?}");
        };
        assert_eq!(hits.len(), 1);
        hits.remove(0).data
    }

    fn reply_only_cfg(reply_timeout: Option<Duration>) -> ClientConfig {
        ClientConfig {
            connect_timeout: None,
            write_timeout: None,
            reply_timeout,
            read_idle_timeout: None,
            ..ClientConfig::default()
        }
    }

    #[tokio::test]
    async fn pipelines_requests() {
        let addr =
            scripted_backend(vec![Step::ReadRequests(2), Step::Write(b"END\r\nEND\r\n")]).await;
        let client = Client::connect(addr).await.unwrap();

        let (a, b) = tokio::join!(client.send(get(b"a")), client.send(get(b"b")));
        assert_eq!(a.unwrap(), Reply::Get { hits: vec![] });
        assert_eq!(b.unwrap(), Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn matches_replies_fifo() {
        let addr = scripted_backend(vec![
            Step::ReadRequests(3),
            Step::Write(b"VALUE k 0 1\r\n1\r\nEND\r\n"),
            Step::Write(b"VALUE k 0 1\r\n2\r\nEND\r\n"),
            Step::Write(b"VALUE k 0 1\r\n3\r\nEND\r\n"),
        ])
        .await;
        let client = Client::connect(addr).await.unwrap();

        let (r1, r2, r3) = tokio::join!(
            client.send(get(b"k")),
            client.send(get(b"k")),
            client.send(get(b"k")),
        );
        assert_eq!(hit_data(r1.unwrap()).as_ref(), b"1");
        assert_eq!(hit_data(r2.unwrap()).as_ref(), b"2");
        assert_eq!(hit_data(r3.unwrap()).as_ref(), b"3");
    }

    #[tokio::test]
    async fn fails_pending_on_eof() {
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Close]).await;
        let client = Client::connect(addr).await.unwrap();
        assert!(client.send(get(b"a")).await.is_err());
    }

    #[tokio::test]
    async fn reassembles_reply_across_partial_reads() {
        let addr = scripted_backend(vec![
            Step::ReadRequests(1),
            Step::WriteChunked(b"VALUE foo 0 3\r\nbar\r\nEND\r\n"),
        ])
        .await;
        let client = Client::connect(addr).await.unwrap();

        assert_eq!(
            hit_data(client.send(get(b"foo")).await.unwrap()).as_ref(),
            b"bar"
        );
    }

    #[tokio::test]
    async fn tears_down_on_malformed_reply() {
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Write(b"WAT\r\n")]).await;
        let client = Client::connect(addr).await.unwrap();
        assert!(client.send(get(b"a")).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn reply_timeout_fires_when_backend_never_replies() {
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Hang]).await;
        let cfg = reply_only_cfg(Some(Duration::from_millis(100)));
        let client = Client::connect_with_config(addr, cfg).await.unwrap();

        let result = client.send(get(b"a")).await;
        assert!(matches!(
            result,
            Err(NetError::Timeout {
                phase: TimeoutPhase::Reply
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn reply_timeout_none_leaves_send_pending() {
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Hang]).await;
        let cfg = reply_only_cfg(None);
        let client = Client::connect_with_config(addr, cfg).await.unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(5), client.send(get(b"a"))).await;
        assert!(outcome.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn connect_timeout_fires_on_black_holed_addr() {
        // 192.0.2.0/24 is TEST-NET-1 (RFC 5737): routed to the default gateway and
        // dropped, so the connect stays pending until the deadline elapses.
        let cfg = ClientConfig {
            connect_timeout: Some(Duration::from_millis(100)),
            ..ClientConfig::default()
        };
        let result = Client::connect_with_config("192.0.2.1:12345", cfg).await;
        assert!(matches!(
            result.err(),
            Some(NetError::Timeout {
                phase: TimeoutPhase::Connect
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn write_timeout_fires_when_backend_stops_reading() {
        let addr = scripted_backend(vec![Step::Hang]).await;
        let cfg = ClientConfig {
            connect_timeout: None,
            write_timeout: Some(Duration::from_millis(100)),
            reply_timeout: None,
            read_idle_timeout: None,
            ..ClientConfig::default()
        };
        let client = Client::connect_with_config(addr, cfg).await.unwrap();

        let payload_larger_than_socket_buffers = vec![b'x'; 16 * 1024 * 1024];
        let big = Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 0,
            exptime: 0,
            data: Bytes::from(payload_larger_than_socket_buffers),
        };
        let result = client.send(big).await;
        assert!(matches!(
            result,
            Err(NetError::Timeout {
                phase: TimeoutPhase::Write
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn late_reply_to_timed_out_request_discarded_keeping_fifo_aligned() {
        let addr = scripted_backend(vec![
            Step::ReadRequests(1),
            Step::Write(b"VALUE k 0 1\r\nA\r\nEND\r\n"),
            Step::ReadRequests(3),
            Step::Write(b"VALUE k 0 1\r\nB\r\nEND\r\n"),
            Step::Write(b"VALUE k 0 1\r\nC\r\nEND\r\n"),
        ])
        .await;
        // Under paused time a pending deadline always beats real loopback I/O, so a
        // request that must SUCCEED cannot own a timer: A and C run timer-free
        // (reply_timeout=None) and B is orphaned by an explicit timeout, which drops
        // its receiver through the same path the client's internal reply timeout uses.
        let cfg = reply_only_cfg(None);
        let client = Client::connect_with_config(addr, cfg).await.unwrap();

        let a = client.send(get(b"k")).await;
        assert_eq!(hit_data(a.unwrap()).as_ref(), b"A");

        let b = tokio::time::timeout(Duration::from_millis(100), client.send(get(b"k"))).await;
        assert!(b.is_err());

        let c = client.send(get(b"k")).await;
        assert_eq!(hit_data(c.unwrap()).as_ref(), b"C");
    }

    #[tokio::test(start_paused = true)]
    async fn read_idle_deadline_reclaims_a_silent_connection() {
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Hang]).await;
        let cfg = ClientConfig {
            connect_timeout: None,
            write_timeout: None,
            reply_timeout: None,
            read_idle_timeout: Some(Duration::from_millis(200)),
            ..ClientConfig::default()
        };
        let client = Client::connect_with_config(addr, cfg).await.unwrap();

        let outstanding = client.send(get(b"a")).await;
        assert!(matches!(
            outstanding,
            Err(NetError::Timeout {
                phase: TimeoutPhase::Reply
            })
        ));

        assert!(matches!(
            client.send(get(b"b")).await,
            Err(NetError::ClientClosed)
        ));
    }
}
