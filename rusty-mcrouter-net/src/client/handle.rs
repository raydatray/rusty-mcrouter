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
    use bytes::{Bytes, BytesMut};
    use rusty_mcrouter_protocol::meta::{DecodedMetaCommand, MetaRequestDecoder};
    use rusty_mcrouter_protocol::reply::GetReply;
    use rusty_mcrouter_protocol::Reply;

    fn parse_request(input: &[u8]) -> Request {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input);
        let DecodedMetaCommand::Request { request, .. } =
            decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected request");
        };
        request
    }

    fn get(key: &'static [u8]) -> Request {
        parse_request(&[b"mg ", key, b" v\r\n"].concat())
    }

    fn hit_data(reply: Reply) -> Bytes {
        let Reply::Get(GetReply::Hit(hit)) = reply else {
            panic!("expected get hit, got {reply:?}");
        };
        hit.value.expect("hit with value")
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
            scripted_backend(vec![Step::ReadRequests(2), Step::Write(b"EN\r\nEN\r\n")]).await;
        let client = Client::connect(addr).await.unwrap();

        let (a, b) = tokio::join!(client.send(get(b"a")), client.send(get(b"b")));
        assert_eq!(a.unwrap(), Reply::Get(GetReply::Miss));
        assert_eq!(b.unwrap(), Reply::Get(GetReply::Miss));
    }

    #[tokio::test]
    async fn matches_replies_fifo() {
        let addr = scripted_backend(vec![
            Step::ReadRequests(3),
            Step::Write(b"VA 1\r\n1\r\n"),
            Step::Write(b"VA 1\r\n2\r\n"),
            Step::Write(b"VA 1\r\n3\r\n"),
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
            Step::WriteChunked(b"VA 3\r\nbar\r\n"),
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
        assert!(matches!(
            client.send(get(b"a")).await,
            Err(NetError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn shape_mismatch_reply_is_a_decode_error() {
        // `mg key v` expects EN or VA; a bare HD violates the expectation.
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Write(b"HD\r\n")]).await;
        let client = Client::connect(addr).await.unwrap();
        assert!(matches!(
            client.send(get(b"a")).await,
            Err(NetError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn encode_failure_fails_only_that_request() {
        // The routing prefix is stripped for the backend, leaving an empty
        // key: an encode error that must not poison the connection.
        let addr = scripted_backend(vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")]).await;
        let client = Client::connect(addr).await.unwrap();

        let bad = parse_request(b"mg /region/cluster/ v\r\n");
        assert!(matches!(client.send(bad).await, Err(NetError::Encode(_))));

        assert_eq!(
            client.send(get(b"ok")).await.unwrap(),
            Reply::Get(GetReply::Miss)
        );
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

        // The Meta value ceiling is 1 MiB, and a single 1 MiB frame can fit
        // in loopback socket buffers. Eight pipelined stores enqueue before
        // the connection task drains its channel (each send readies
        // immediately against channel capacity), so they coalesce into one
        // 8 MiB batch whose write must stall against a non-reading peer.
        let mut big = b"ms k 1048576\r\n".to_vec();
        big.extend_from_slice(&vec![b'x'; 1024 * 1024]);
        big.extend_from_slice(b"\r\n");
        let store = parse_request(&big);

        let results = tokio::join!(
            client.send(store.clone()),
            client.send(store.clone()),
            client.send(store.clone()),
            client.send(store.clone()),
            client.send(store.clone()),
            client.send(store.clone()),
            client.send(store.clone()),
            client.send(store),
        );
        let results = [
            results.0, results.1, results.2, results.3, results.4, results.5, results.6, results.7,
        ];
        for result in results {
            assert!(matches!(
                result,
                Err(NetError::Timeout {
                    phase: TimeoutPhase::Write
                })
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn late_reply_to_timed_out_request_discarded_keeping_fifo_aligned() {
        let addr = scripted_backend(vec![
            Step::ReadRequests(1),
            Step::Write(b"VA 1\r\nA\r\n"),
            Step::ReadRequests(3),
            Step::Write(b"VA 1\r\nB\r\n"),
            Step::Write(b"VA 1\r\nC\r\n"),
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
