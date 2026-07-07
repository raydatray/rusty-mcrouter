use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_protocol::{parse_request, Parsed, Reply, Request, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WANT_SERVER_ERROR: &[u8] = b"__rusty__.want_server_error";
const WANT_ERROR: &[u8] = b"__rusty__.want_error";

#[derive(Clone, Debug)]
struct Item {
    data: Bytes,
    flags: u32,
    expiry: Option<Instant>,
    cas: u64,
}

#[derive(Debug, Default)]
pub struct MockMcStore {
    items: HashMap<Bytes, Item>,
    next_cas: u64,
}

impl MockMcStore {
    pub fn apply(&mut self, req: Request) -> Reply {
        if let Some(reply) = fault_reply(key_of(&req)) {
            return reply;
        }

        match req {
            Request::Get { key } => Reply::Get {
                hits: self.get_value(&key).into_iter().collect(),
            },
            Request::Set {
                key,
                flags,
                exptime,
                data,
            } => {
                self.store(key, flags, exptime, data);
                Reply::Stored
            }
            Request::Add {
                key,
                flags,
                exptime,
                data,
            } => {
                if self.is_present(&key) {
                    Reply::NotStored
                } else {
                    self.store(key, flags, exptime, data);
                    Reply::Stored
                }
            }
            Request::Replace {
                key,
                flags,
                exptime,
                data,
            } => {
                if self.is_present(&key) {
                    self.store(key, flags, exptime, data);
                    Reply::Stored
                } else {
                    Reply::NotStored
                }
            }
            Request::Append { key, data, .. } => self.append_or_prepend(key, data, AppendMode::Append),
            Request::Prepend { key, data, .. } => {
                self.append_or_prepend(key, data, AppendMode::Prepend)
            }
            Request::Delete { key } => {
                if self.remove_present(&key) {
                    Reply::Deleted
                } else {
                    Reply::NotFound
                }
            }
            Request::Incr { key, delta } => self.update_numeric(key, delta, NumericMode::Incr),
            Request::Decr { key, delta } => self.update_numeric(key, delta, NumericMode::Decr),
            Request::Touch { key, exptime } => {
                self.remove_if_expired(&key);
                let expiry = expiry_from(exptime);
                let cas = self.next_cas();
                match self.items.get_mut(&key) {
                    Some(item) => {
                        item.expiry = expiry;
                        item.cas = cas;
                        Reply::Touched
                    }
                    None => Reply::NotFound,
                }
            }
        }
    }

    fn get_many(&mut self, keys: Vec<Bytes>) -> Reply {
        let hits = keys
            .iter()
            .filter_map(|key| self.get_value(key))
            .collect::<Vec<_>>();
        Reply::Get { hits }
    }

    fn get_value(&mut self, key: &Bytes) -> Option<Value> {
        self.remove_if_expired(key);
        self.items.get(key).map(|item| Value {
            key: key.clone(),
            flags: item.flags,
            data: item.data.clone(),
        })
    }

    fn store(&mut self, key: Bytes, flags: u32, exptime: i32, data: Bytes) {
        let item = Item {
            data,
            flags,
            expiry: expiry_from(exptime),
            cas: self.next_cas(),
        };
        self.items.insert(key, item);
    }

    fn append_or_prepend(&mut self, key: Bytes, data: Bytes, mode: AppendMode) -> Reply {
        self.remove_if_expired(&key);
        let cas = self.next_cas();
        let Some(item) = self.items.get_mut(&key) else {
            return Reply::NotStored;
        };

        let mut combined = BytesMut::with_capacity(item.data.len() + data.len());
        match mode {
            AppendMode::Append => {
                combined.extend_from_slice(&item.data);
                combined.extend_from_slice(&data);
            }
            AppendMode::Prepend => {
                combined.extend_from_slice(&data);
                combined.extend_from_slice(&item.data);
            }
        }
        item.data = combined.freeze();
        item.cas = cas;
        Reply::Stored
    }

    fn update_numeric(&mut self, key: Bytes, delta: u64, mode: NumericMode) -> Reply {
        self.remove_if_expired(&key);
        let cas = self.next_cas();
        let Some(item) = self.items.get_mut(&key) else {
            return Reply::NotFound;
        };
        let Some(current) = std::str::from_utf8(&item.data)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return Reply::Error;
        };
        let next = match mode {
            NumericMode::Incr => current.saturating_add(delta),
            NumericMode::Decr => current.saturating_sub(delta),
        };
        item.data = Bytes::from(next.to_string());
        item.cas = cas;
        Reply::Numeric(next)
    }

    fn is_present(&mut self, key: &Bytes) -> bool {
        self.remove_if_expired(key);
        self.items.contains_key(key)
    }

    fn remove_present(&mut self, key: &Bytes) -> bool {
        self.remove_if_expired(key);
        self.items.remove(key).is_some()
    }

    fn remove_if_expired(&mut self, key: &Bytes) {
        let expired = self
            .items
            .get(key)
            .and_then(|item| item.expiry)
            .is_some_and(|expiry| expiry <= Instant::now());
        if expired {
            self.items.remove(key);
        }
    }

    fn next_cas(&mut self) -> u64 {
        self.next_cas = self.next_cas.saturating_add(1);
        self.next_cas
    }
}

pub async fn spawn_mock_memcached() -> SocketAddr {
    spawn_mock(false).await
}

pub async fn spawn_failing_mock_memcached() -> SocketAddr {
    spawn_mock(true).await
}

async fn spawn_mock(always_server_error: bool) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = Arc::new(Mutex::new(MockMcStore::default()));

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                handle_connection(stream, store, always_server_error).await;
            });
        }
    });

    addr
}

async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<Mutex<MockMcStore>>,
    always_server_error: bool,
) {
    let mut input = BytesMut::with_capacity(4096);
    let mut scratch = [0; 4096];

    loop {
        match stream.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(n) => input.extend_from_slice(&scratch[..n]),
        }

        loop {
            let parsed = match parse_request(&mut input) {
                Ok(Some(parsed)) => parsed,
                Ok(None) => break,
                Err(_) => return,
            };

            let reply = if always_server_error {
                Reply::ServerError(Bytes::from_static(b"primary down"))
            } else {
                let mut store = store.lock().unwrap();
                apply_parsed(&mut store, parsed)
            };
            let mut output = BytesMut::new();
            reply.serialize_into(&mut output);
            if stream.write_all(&output).await.is_err() {
                return;
            }
        }
    }
}

fn apply_parsed(store: &mut MockMcStore, parsed: Parsed) -> Reply {
    match parsed {
        Parsed::One(req) => store.apply(req),
        Parsed::MultiGet(keys) => keys
            .iter()
            .find_map(fault_reply)
            .unwrap_or_else(|| store.get_many(keys)),
    }
}

fn key_of(req: &Request) -> &Bytes {
    match req {
        Request::Get { key }
        | Request::Set { key, .. }
        | Request::Delete { key }
        | Request::Add { key, .. }
        | Request::Replace { key, .. }
        | Request::Append { key, .. }
        | Request::Prepend { key, .. }
        | Request::Incr { key, .. }
        | Request::Decr { key, .. }
        | Request::Touch { key, .. } => key,
    }
}

fn fault_reply(key: &Bytes) -> Option<Reply> {
    match key.as_ref() {
        WANT_SERVER_ERROR => Some(Reply::ServerError(Bytes::from_static(b"injected"))),
        WANT_ERROR => Some(Reply::Error),
        _ => None,
    }
}

fn expiry_from(exptime: i32) -> Option<Instant> {
    match exptime.cmp(&0) {
        std::cmp::Ordering::Less => Some(Instant::now()),
        std::cmp::Ordering::Equal => None,
        std::cmp::Ordering::Greater => Some(Instant::now() + Duration::from_secs(exptime as u64)),
    }
}

enum AppendMode {
    Append,
    Prepend,
}

enum NumericMode {
    Incr,
    Decr,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &'static [u8]) -> Bytes {
        Bytes::from_static(bytes)
    }

    fn set_req(key: Bytes, flags: u32, exptime: i32, data: Bytes) -> Request {
        Request::Set {
            key,
            flags,
            exptime,
            data,
        }
    }

    fn value(key: &'static [u8], flags: u32, data: &'static [u8]) -> Value {
        Value {
            key: Bytes::from_static(key),
            flags,
            data: Bytes::from_static(data),
        }
    }

    #[test]
    fn set_get_round_trip() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(set_req(key(b"k"), 9, 0, key(b"world"))),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"k") }),
            Reply::Get {
                hits: vec![value(b"k", 9, b"world")]
            }
        );
    }

    #[test]
    fn add_and_replace_present_absent() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(Request::Add {
                key: key(b"k"),
                flags: 1,
                exptime: 0,
                data: key(b"first"),
            }),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Add {
                key: key(b"k"),
                flags: 2,
                exptime: 0,
                data: key(b"second"),
            }),
            Reply::NotStored
        );
        assert_eq!(
            store.apply(Request::Replace {
                key: key(b"missing"),
                flags: 1,
                exptime: 0,
                data: key(b"nope"),
            }),
            Reply::NotStored
        );
        assert_eq!(
            store.apply(Request::Replace {
                key: key(b"k"),
                flags: 3,
                exptime: 0,
                data: key(b"third"),
            }),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"k") }),
            Reply::Get {
                hits: vec![value(b"k", 3, b"third")]
            }
        );
    }

    #[test]
    fn append_and_prepend_keep_original_flags() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(set_req(key(b"a"), 7, 0, key(b"hello"))),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Append {
                key: key(b"a"),
                flags: 999,
                exptime: 999,
                data: key(b" world"),
            }),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"a") }),
            Reply::Get {
                hits: vec![value(b"a", 7, b"hello world")]
            }
        );

        assert_eq!(
            store.apply(set_req(key(b"p"), 8, 0, key(b"world"))),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Prepend {
                key: key(b"p"),
                flags: 999,
                exptime: 999,
                data: key(b"hello "),
            }),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"p") }),
            Reply::Get {
                hits: vec![value(b"p", 8, b"hello world")]
            }
        );

        assert_eq!(
            store.apply(Request::Append {
                key: key(b"missing"),
                flags: 0,
                exptime: 0,
                data: key(b"x"),
            }),
            Reply::NotStored
        );
        assert_eq!(
            store.apply(Request::Prepend {
                key: key(b"missing"),
                flags: 0,
                exptime: 0,
                data: key(b"x"),
            }),
            Reply::NotStored
        );
    }

    #[test]
    fn delete_present_absent() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(set_req(key(b"k"), 0, 0, key(b"v"))),
            Reply::Stored
        );
        assert_eq!(store.apply(Request::Delete { key: key(b"k") }), Reply::Deleted);
        assert_eq!(
            store.apply(Request::Delete { key: key(b"k") }),
            Reply::NotFound
        );
    }

    #[test]
    fn incr_decr_and_decr_clamps_at_zero() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(set_req(key(b"n"), 0, 0, key(b"42"))),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Incr {
                key: key(b"n"),
                delta: 1,
            }),
            Reply::Numeric(43)
        );
        assert_eq!(
            store.apply(Request::Decr {
                key: key(b"n"),
                delta: 5,
            }),
            Reply::Numeric(38)
        );
        assert_eq!(
            store.apply(Request::Decr {
                key: key(b"n"),
                delta: 100,
            }),
            Reply::Numeric(0)
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"n") }),
            Reply::Get {
                hits: vec![value(b"n", 0, b"0")]
            }
        );
        assert_eq!(
            store.apply(Request::Incr {
                key: key(b"missing"),
                delta: 1,
            }),
            Reply::NotFound
        );
        assert_eq!(
            store.apply(Request::Decr {
                key: key(b"missing"),
                delta: 1,
            }),
            Reply::NotFound
        );
    }

    #[test]
    fn touch_present_absent() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(Request::Touch {
                key: key(b"missing"),
                exptime: 60,
            }),
            Reply::NotFound
        );
        assert_eq!(
            store.apply(set_req(key(b"k"), 42, 0, key(b"hello"))),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Touch {
                key: key(b"k"),
                exptime: 60,
            }),
            Reply::Touched
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"k") }),
            Reply::Get {
                hits: vec![value(b"k", 42, b"hello")]
            }
        );
    }

    #[test]
    fn get_miss() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(Request::Get { key: key(b"missing") }),
            Reply::Get { hits: vec![] }
        );
    }

    #[test]
    fn lazy_expiry_treats_expired_as_absent() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(set_req(key(b"k"), 0, -1, key(b"v"))),
            Reply::Stored
        );
        assert_eq!(
            store.apply(Request::Get { key: key(b"k") }),
            Reply::Get { hits: vec![] }
        );
        assert_eq!(
            store.apply(Request::Delete { key: key(b"k") }),
            Reply::NotFound
        );
    }
}
