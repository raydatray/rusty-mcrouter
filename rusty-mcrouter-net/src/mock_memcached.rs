//! An in-process Meta-protocol memcached double for end-to-end tests.
//!
//! A connection is served by the same codec pair a Meta frontend uses:
//! `MetaRequestDecoder` -> in-memory store -> `MetaReplyEncoder` with the
//! decoded reply plan, so quiet suppression, opaque echo, and `k`/`b` key
//! echo all behave like a real server without bespoke wire code.
//!
//! Deliberate simplifications (documented, not bugs): no recache/stale
//! machinery (`R` wins, `W`/`X`/`Z` beyond vivification), `I` invalidation
//! is ignored, `mg E` does not override CAS, and temporal flags apply in a
//! fixed order (vivify, update TTL, then read TTL) instead of request order.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_protocol::meta::{
    DecodedMetaCommand, MetaReplyEncoder, MetaReplyPlan, MetaRequestDecodeError,
    MetaRequestDecoder,
};
use rusty_mcrouter_protocol::reply::{
    ArithmeticReply, ArithmeticResult, DebugField, DebugHit, DebugReply, DeleteReply, ErrorReply,
    GetHit, GetReply, RecacheState, StoreReply, StoreResult,
};
use rusty_mcrouter_protocol::request::{
    ArithmeticMode, ArithmeticRequest, ArithmeticTemporalInstruction, DebugRequest, DeleteRequest,
    GetRequest, GetTemporalInstruction, StoreMode, StoreRequest,
};
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WANT_SERVER_ERROR: &[u8] = b"__rusty__.want_server_error";
const WANT_ERROR: &[u8] = b"__rusty__.want_error";

#[derive(Clone, Debug)]
struct Item {
    data: Bytes,
    client_flags: u32,
    expiry: Option<Instant>,
    cas: u64,
    fetched: bool,
}

#[derive(Debug, Default)]
pub struct MockMcStore {
    items: HashMap<Bytes, Item>,
    next_cas: u64,
}

impl MockMcStore {
    pub fn apply(&mut self, request: Request) -> Reply {
        match request {
            Request::Get(request) => self.get(request),
            Request::Store(request) => self.store(request),
            Request::Delete(request) => self.delete(request),
            Request::Arithmetic(request) => self.arithmetic(request),
            Request::Debug(request) => self.debug(request),
        }
    }

    fn get(&mut self, request: GetRequest) -> Reply {
        let key = request.key.into_bytes();
        self.remove_if_expired(&key);

        let vivify_ttl = request.temporal.iter().find_map(|step| match step {
            GetTemporalInstruction::Vivify(ttl) => Some(*ttl),
            _ => None,
        });
        let update_ttl = request.temporal.iter().find_map(|step| match step {
            GetTemporalInstruction::UpdateTtl(ttl) => Some(*ttl),
            _ => None,
        });
        let wants_ttl = request
            .temporal
            .iter()
            .any(|step| matches!(step, GetTemporalInstruction::ReturnTtl));

        let (recache, cas) = if self.items.contains_key(&key) {
            (RecacheState::None, self.next_cas())
        } else {
            match vivify_ttl {
                Some(ttl) => {
                    let cas = self.next_cas();
                    self.items.insert(
                        key.clone(),
                        Item {
                            data: Bytes::new(),
                            client_flags: 0,
                            expiry: expiry_from(ttl),
                            cas,
                            fetched: false,
                        },
                    );
                    (RecacheState::Won, cas)
                }
                None => return Reply::Get(GetReply::Miss),
            }
        };
        let _ = cas;

        let item = self.items.get_mut(&key).expect("present or vivified");
        if let Some(ttl) = update_ttl {
            item.expiry = expiry_from(ttl);
        }
        let hit_before = item.fetched;
        if !request.no_lru_bump {
            item.fetched = true;
        }

        // `C<cas>`: suppress the value when the client's token matches.
        let suppress_value = request.check_cas.is_some_and(|check| check == item.cas);
        let value = (request.return_value && !suppress_value).then(|| item.data.clone());

        Reply::Get(GetReply::Hit(GetHit {
            size: request.return_size.then_some(item.data.len() as u64),
            value,
            client_flags: request.return_client_flags.then_some(item.client_flags),
            cas: request.return_cas.then_some(item.cas),
            ttl: wants_ttl.then(|| ttl_remaining(item.expiry)),
            hit_before: request.return_hit_state.then_some(hit_before),
            last_access_seconds: request.return_last_access.then_some(0),
            recache,
            stale: false,
        }))
    }

    fn store(&mut self, request: StoreRequest) -> Reply {
        let StoreRequest {
            key,
            value,
            return_cas,
            return_size,
            mode,
            client_flags,
            ttl,
            compare_cas,
            override_cas,
            invalidate: _,
            vivify_ttl,
        } = request;
        let key = key.into_bytes();
        self.remove_if_expired(&key);

        let size = value.len() as u64;
        let result = |cas: u64| StoreResult {
            // memcached echoes c0 on failure codes; successes carry the new CAS.
            cas: return_cas.then_some(cas),
            size: return_size.then_some(size),
        };

        let existing = self.items.get(&key);
        if let Some(check) = compare_cas {
            match existing {
                None => return Reply::Store(StoreReply::NotFound(result(0))),
                Some(item) if item.cas != check => {
                    return Reply::Store(StoreReply::Exists(result(0)));
                }
                Some(_) => {}
            }
        }
        match mode {
            StoreMode::Add if existing.is_some() => {
                return Reply::Store(StoreReply::NotStored(result(0)));
            }
            StoreMode::Replace if existing.is_none() => {
                return Reply::Store(StoreReply::NotStored(result(0)));
            }
            StoreMode::Append | StoreMode::Prepend if existing.is_none() => {
                // `N` vivifies on an append/prepend miss, seeding the payload.
                let Some(ttl) = vivify_ttl else {
                    return Reply::Store(StoreReply::NotStored(result(0)));
                };
                let cas = override_cas.unwrap_or_else(|| self.next_cas());
                self.items.insert(
                    key,
                    Item {
                        data: value,
                        client_flags: client_flags.unwrap_or(0),
                        expiry: expiry_from(ttl),
                        cas,
                        fetched: false,
                    },
                );
                return Reply::Store(StoreReply::Success(result(cas)));
            }
            _ => {}
        }

        let cas = override_cas.unwrap_or_else(|| self.next_cas());
        let stored_size = match mode {
            StoreMode::Set | StoreMode::Add | StoreMode::Replace => {
                let len = value.len() as u64;
                self.items.insert(
                    key,
                    Item {
                        data: value,
                        client_flags: client_flags.unwrap_or(0),
                        expiry: expiry_from(ttl.unwrap_or(0)),
                        cas,
                        fetched: false,
                    },
                );
                len
            }
            StoreMode::Append | StoreMode::Prepend => {
                let item = self.items.get_mut(&key).expect("miss handled above");
                let mut combined = BytesMut::with_capacity(item.data.len() + value.len());
                match mode {
                    StoreMode::Append => {
                        combined.extend_from_slice(&item.data);
                        combined.extend_from_slice(&value);
                    }
                    _ => {
                        combined.extend_from_slice(&value);
                        combined.extend_from_slice(&item.data);
                    }
                }
                item.data = combined.freeze();
                item.cas = cas;
                item.data.len() as u64
            }
        };
        Reply::Store(StoreReply::Success(StoreResult {
            cas: return_cas.then_some(cas),
            size: return_size.then_some(stored_size),
        }))
    }

    fn delete(&mut self, request: DeleteRequest) -> Reply {
        let key = request.key.into_bytes();
        self.remove_if_expired(&key);

        let Some(item) = self.items.get_mut(&key) else {
            return Reply::Delete(DeleteReply::NotFound);
        };
        if let Some(check) = request.compare_cas {
            if item.cas != check {
                return Reply::Delete(DeleteReply::Exists);
            }
        }
        if request.remove_value {
            item.data = Bytes::new();
            item.cas = request.override_cas.unwrap_or(item.cas + 1);
        } else {
            self.items.remove(&key);
        }
        Reply::Delete(DeleteReply::Success)
    }

    fn arithmetic(&mut self, request: ArithmeticRequest) -> Reply {
        let key = request.key.into_bytes();
        self.remove_if_expired(&key);

        let vivify_ttl = request.temporal.iter().find_map(|step| match step {
            ArithmeticTemporalInstruction::Vivify(ttl) => Some(*ttl),
            _ => None,
        });
        let update_ttl = request.temporal.iter().find_map(|step| match step {
            ArithmeticTemporalInstruction::UpdateTtl(ttl) => Some(*ttl),
            _ => None,
        });
        let wants_ttl = request
            .temporal
            .iter()
            .any(|step| matches!(step, ArithmeticTemporalInstruction::ReturnTtl));

        let next = match self.items.get(&key) {
            None => {
                let Some(ttl) = vivify_ttl else {
                    return Reply::Arithmetic(ArithmeticReply::NotFound(
                        ArithmeticResult::default(),
                    ));
                };
                // A vivified counter is seeded with `J` and skips the delta.
                let seeded = request.initial_value.unwrap_or(0);
                let cas = request.override_cas.unwrap_or_else(|| self.next_cas());
                self.items.insert(
                    key.clone(),
                    Item {
                        data: Bytes::from(seeded.to_string()),
                        client_flags: 0,
                        expiry: expiry_from(ttl),
                        cas,
                        fetched: false,
                    },
                );
                seeded
            }
            Some(item) => {
                if let Some(check) = request.compare_cas {
                    if item.cas != check {
                        return Reply::Arithmetic(ArithmeticReply::Exists(
                            ArithmeticResult::default(),
                        ));
                    }
                }
                let Some(current) = std::str::from_utf8(&item.data)
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                else {
                    return Reply::Error(ErrorReply::Client(Some(Bytes::from_static(
                        b"cannot increment or decrement non-numeric value",
                    ))));
                };
                let next = match request.mode {
                    ArithmeticMode::Increment => current.wrapping_add(request.delta),
                    ArithmeticMode::Decrement => current.saturating_sub(request.delta),
                };
                let cas = request.override_cas.unwrap_or_else(|| self.next_cas());
                let item = self.items.get_mut(&key).expect("present");
                item.data = Bytes::from(next.to_string());
                item.cas = cas;
                if let Some(ttl) = update_ttl {
                    item.expiry = expiry_from(ttl);
                }
                next
            }
        };

        let item = self.items.get(&key).expect("stored above");
        Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
            value: request.return_value.then_some(next),
            cas: request.return_cas.then_some(item.cas),
            ttl: wants_ttl.then(|| ttl_remaining(item.expiry)),
        }))
    }

    fn debug(&mut self, request: DebugRequest) -> Reply {
        let key = request.key.into_bytes();
        self.remove_if_expired(&key);

        let Some(item) = self.items.get(&key) else {
            return Reply::Debug(DebugReply::Miss);
        };
        let field = |name: &'static [u8], value: String| DebugField {
            name: Bytes::from_static(name),
            value: Bytes::from(value),
        };
        Reply::Debug(DebugReply::Hit(DebugHit {
            fields: vec![
                field(b"exp", ttl_remaining(item.expiry).to_string()),
                field(b"la", "0".to_string()),
                field(b"cas", item.cas.to_string()),
                field(b"fetch", if item.fetched { "yes" } else { "no" }.to_string()),
                field(b"cls", "1".to_string()),
                field(b"size", item.data.len().to_string()),
            ],
        }))
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
    let mut decoder = MetaRequestDecoder::new();
    let encoder = MetaReplyEncoder::new();
    let mut input = BytesMut::with_capacity(4096);
    let mut scratch = [0; 4096];

    loop {
        match stream.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(n) => input.extend_from_slice(&scratch[..n]),
        }

        let mut output = BytesMut::new();
        loop {
            match decoder.decode(&mut input) {
                Ok(Some(DecodedMetaCommand::Request {
                    request,
                    reply_plan,
                })) => {
                    let reply = if always_server_error {
                        Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
                            b"primary down",
                        ))))
                    } else if let Some(fault) = fault_reply(request.key().as_bytes()) {
                        fault
                    } else {
                        store.lock().unwrap().apply(request)
                    };
                    if encoder.encode(&reply, &reply_plan, &mut output).is_err() {
                        let fallback = Reply::Error(ErrorReply::Server(Some(
                            Bytes::from_static(b"mock reply encoding failed"),
                        )));
                        let _ = encoder.encode(&fallback, &MetaReplyPlan::default(), &mut output);
                    }
                }
                Ok(Some(DecodedMetaCommand::NoOp)) => encoder.encode_noop(&mut output),
                Ok(None) => break,
                Err(MetaRequestDecodeError::Recoverable(error)) => {
                    let _ = encoder.encode(
                        &Reply::Error(error),
                        &MetaReplyPlan::default(),
                        &mut output,
                    );
                }
                Err(MetaRequestDecodeError::Fatal(_)) => return,
            }
        }
        if !output.is_empty() && stream.write_all(&output).await.is_err() {
            return;
        }
    }
}

fn fault_reply(key: &[u8]) -> Option<Reply> {
    match key {
        WANT_SERVER_ERROR => Some(Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
            b"injected",
        ))))),
        WANT_ERROR => Some(Reply::Error(ErrorReply::Error)),
        _ => None,
    }
}

fn expiry_from(ttl: i32) -> Option<Instant> {
    match ttl.cmp(&0) {
        std::cmp::Ordering::Less => Some(Instant::now()),
        std::cmp::Ordering::Equal => None,
        std::cmp::Ordering::Greater => Some(Instant::now() + Duration::from_secs(ttl as u64)),
    }
}

fn ttl_remaining(expiry: Option<Instant>) -> i64 {
    match expiry {
        None => -1,
        Some(at) => at
            .saturating_duration_since(Instant::now())
            .as_secs()
            .try_into()
            .unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(input: &[u8]) -> Request {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input);
        let DecodedMetaCommand::Request { request, .. } =
            decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected request");
        };
        request
    }

    fn hit(reply: Reply) -> GetHit {
        let Reply::Get(GetReply::Hit(hit)) = reply else {
            panic!("expected get hit, got {reply:?}");
        };
        hit
    }

    #[test]
    fn store_get_round_trip_with_projections() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(req(b"ms k 5 F9 c s\r\nworld\r\n")),
            Reply::Store(StoreReply::Success(StoreResult {
                cas: Some(1),
                size: Some(5),
            }))
        );

        let hit = hit(store.apply(req(b"mg k v f c s\r\n")));
        assert_eq!(hit.value.as_deref(), Some(b"world".as_slice()));
        assert_eq!(hit.client_flags, Some(9));
        assert_eq!(hit.cas, Some(1));
        assert_eq!(hit.size, Some(5));
    }

    #[test]
    fn get_miss_and_bare_hit() {
        let mut store = MockMcStore::default();
        assert_eq!(store.apply(req(b"mg nope v\r\n")), Reply::Get(GetReply::Miss));

        store.apply(req(b"ms k 1\r\nx\r\n"));
        assert_eq!(hit(store.apply(req(b"mg k\r\n"))), GetHit::default());
    }

    #[test]
    fn add_and_replace_gate_on_presence() {
        let mut store = MockMcStore::default();
        assert!(matches!(
            store.apply(req(b"ms k 1 ME\r\na\r\n")),
            Reply::Store(StoreReply::Success(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms k 1 ME\r\nb\r\n")),
            Reply::Store(StoreReply::NotStored(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms missing 1 MR\r\nc\r\n")),
            Reply::Store(StoreReply::NotStored(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms k 1 MR\r\nd\r\n")),
            Reply::Store(StoreReply::Success(_))
        ));
        assert_eq!(
            hit(store.apply(req(b"mg k v\r\n"))).value.as_deref(),
            Some(b"d".as_slice())
        );
    }

    #[test]
    fn append_prepend_and_vivify() {
        let mut store = MockMcStore::default();
        store.apply(req(b"ms k 5\r\nhello\r\n"));
        assert!(matches!(
            store.apply(req(b"ms k 6 MA\r\n world\r\n")),
            Reply::Store(StoreReply::Success(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms k 6 MP\r\nsays: \r\n")),
            Reply::Store(StoreReply::Success(_))
        ));
        assert_eq!(
            hit(store.apply(req(b"mg k v\r\n"))).value.as_deref(),
            Some(b"says: hello world".as_slice())
        );

        assert!(matches!(
            store.apply(req(b"ms missing 1 MA\r\nx\r\n")),
            Reply::Store(StoreReply::NotStored(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms vivified 3 MA N60\r\nnew\r\n")),
            Reply::Store(StoreReply::Success(_))
        ));
        assert_eq!(
            hit(store.apply(req(b"mg vivified v\r\n"))).value.as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[test]
    fn compare_cas_gates_stores_and_deletes() {
        let mut store = MockMcStore::default();
        store.apply(req(b"ms k 1\r\na\r\n")); // cas 1
        assert!(matches!(
            store.apply(req(b"ms k 1 C999\r\nb\r\n")),
            Reply::Store(StoreReply::Exists(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms missing 1 C1\r\nb\r\n")),
            Reply::Store(StoreReply::NotFound(_))
        ));
        assert!(matches!(
            store.apply(req(b"ms k 1 C1\r\nb\r\n")),
            Reply::Store(StoreReply::Success(_))
        ));

        assert_eq!(
            store.apply(req(b"md k C999\r\n")),
            Reply::Delete(DeleteReply::Exists)
        );
        assert_eq!(store.apply(req(b"md k\r\n")), Reply::Delete(DeleteReply::Success));
        assert_eq!(
            store.apply(req(b"md k\r\n")),
            Reply::Delete(DeleteReply::NotFound)
        );
    }

    #[test]
    fn get_check_cas_suppresses_matching_value() {
        let mut store = MockMcStore::default();
        store.apply(req(b"ms k 1\r\nx\r\n")); // cas 1
        assert_eq!(hit(store.apply(req(b"mg k v C1\r\n"))).value, None);
        assert_eq!(
            hit(store.apply(req(b"mg k v C999\r\n"))).value.as_deref(),
            Some(b"x".as_slice())
        );
    }

    #[test]
    fn delete_x_leaves_an_empty_tombstone() {
        let mut store = MockMcStore::default();
        store.apply(req(b"ms k 5\r\nhello\r\n"));
        assert_eq!(store.apply(req(b"md k x\r\n")), Reply::Delete(DeleteReply::Success));
        assert_eq!(
            hit(store.apply(req(b"mg k v s\r\n"))),
            GetHit {
                value: Some(Bytes::new()),
                size: Some(0),
                ..GetHit::default()
            }
        );
    }

    #[test]
    fn arithmetic_full_surface() {
        let mut store = MockMcStore::default();
        assert_eq!(
            store.apply(req(b"ma missing\r\n")),
            Reply::Arithmetic(ArithmeticReply::NotFound(ArithmeticResult::default()))
        );

        // vivify seeds J and skips the delta
        assert_eq!(
            store.apply(req(b"ma counter N60 J5 v c\r\n")),
            Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
                value: Some(5),
                cas: Some(1),
                ttl: None,
            }))
        );
        assert_eq!(
            store.apply(req(b"ma counter D2 v\r\n")),
            Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
                value: Some(7),
                ..ArithmeticResult::default()
            }))
        );
        assert_eq!(
            store.apply(req(b"ma counter MD D100 v\r\n")),
            Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
                value: Some(0),
                ..ArithmeticResult::default()
            }))
        );
        assert_eq!(
            store.apply(req(b"ma counter C999\r\n")),
            Reply::Arithmetic(ArithmeticReply::Exists(ArithmeticResult::default()))
        );

        store.apply(req(b"ms text 3\r\nabc\r\n"));
        assert_eq!(
            store.apply(req(b"ma text\r\n")),
            Reply::Error(ErrorReply::Client(Some(Bytes::from_static(
                b"cannot increment or decrement non-numeric value",
            ))))
        );
    }

    #[test]
    fn debug_reports_metadata() {
        let mut store = MockMcStore::default();
        assert_eq!(store.apply(req(b"me missing\r\n")), Reply::Debug(DebugReply::Miss));

        store.apply(req(b"ms k 3\r\nabc\r\n"));
        let Reply::Debug(DebugReply::Hit(hit)) = store.apply(req(b"me k\r\n")) else {
            panic!("expected debug hit");
        };
        let field = |name: &[u8]| {
            hit.fields
                .iter()
                .find(|field| field.name == name)
                .unwrap_or_else(|| panic!("missing field {name:?}"))
                .value
                .clone()
        };
        assert_eq!(field(b"exp"), "-1");
        assert_eq!(field(b"cas"), "1");
        assert_eq!(field(b"fetch"), "no");
        assert_eq!(field(b"size"), "3");
    }

    #[test]
    fn hit_state_and_lru_bump() {
        let mut store = MockMcStore::default();
        store.apply(req(b"ms k 1\r\nx\r\n"));
        assert_eq!(hit(store.apply(req(b"mg k h u\r\n"))).hit_before, Some(false));
        // `u` above suppressed the bump, so the state is still fresh.
        assert_eq!(hit(store.apply(req(b"mg k h\r\n"))).hit_before, Some(false));
        assert_eq!(hit(store.apply(req(b"mg k h\r\n"))).hit_before, Some(true));
    }

    #[test]
    fn vivify_on_get_miss_wins_recache() {
        let mut store = MockMcStore::default();
        let first = hit(store.apply(req(b"mg k v N60 t\r\n")));
        assert_eq!(first.recache, RecacheState::Won);
        assert_eq!(first.value.as_deref(), Some(b"".as_slice()));
        assert!(first.ttl.is_some_and(|ttl| (1..=60).contains(&ttl)));
    }

    #[test]
    fn lazy_expiry_treats_expired_as_absent() {
        let mut store = MockMcStore::default();
        store.apply(req(b"ms k 1 T-1\r\nx\r\n"));
        assert_eq!(store.apply(req(b"mg k v\r\n")), Reply::Get(GetReply::Miss));
        assert_eq!(
            store.apply(req(b"md k\r\n")),
            Reply::Delete(DeleteReply::NotFound)
        );
    }
}
