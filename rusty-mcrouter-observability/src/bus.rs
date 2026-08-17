use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rusty_mcrouter_net::tko::TkoEventSink;
use tokio::time::Instant;

use crate::{events::Event, logging};

pub struct EventSender {
    tx: tokio::sync::mpsc::Sender<Event>,
    dropped_counter: Arc<AtomicU64>,
}

impl Clone for EventSender {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            dropped_counter: Arc::clone(&self.dropped_counter),
        }
    }
}

impl EventSender {
    pub fn tko_sink(&self) -> TkoEventSink {
        let sender = self.clone();

        Box::new(move |event| sender.emit(Event::Tko(event)))
    }

    pub fn emit(&self, event: Event) {
        if self.tx.try_send(event).is_err() {
            // either full or closed, either way we cannot block so move on
            self.dropped_counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped_total(&self) -> u64 {
        self.dropped_counter.load(Ordering::Relaxed)
    }

    pub fn dropped_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.dropped_counter)
    }
}

const DROP_WARN_INTERVAL: Duration = Duration::from_secs(1);

pub struct EventConsumer {
    rx: tokio::sync::mpsc::Receiver<Event>,
    dropped_counter: Arc<AtomicU64>,
}

pub fn channel(capacity: usize) -> (EventSender, EventConsumer) {
    assert!(capacity > 0, "a zero-capacity event bus drops everything");

    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    let dropped_counter = Arc::new(AtomicU64::new(0));

    (
        EventSender {
            tx,
            dropped_counter: Arc::clone(&dropped_counter),
        },
        EventConsumer {
            rx,
            dropped_counter,
        },
    )
}

impl EventConsumer {
    pub async fn run(mut self) {
        let mut last_seen = 0u64;
        let mut last_warned = Instant::now() - DROP_WARN_INTERVAL;

        while let Some(event) = self.rx.recv().await {
            logging::write(&event);
            self.warn_if_shedding_events(&mut last_seen, &mut last_warned);
        }
    }

    fn warn_if_shedding_events(&self, last_seen: &mut u64, last_warned: &mut Instant) {
        let dropped = self.dropped_counter.load(Ordering::Relaxed);
        if dropped > *last_seen && last_warned.elapsed() >= DROP_WARN_INTERVAL {
            tracing::warn!(
                target: "rusty-mcrouter-observability::bus",
                dropped_total = dropped,
                new = dropped - *last_seen,
                "event bus shed events; log stream incomplete"
            );
            *last_seen = dropped;
            *last_warned = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::events::{WorkerEvent, WorkerEventRecord};

    use super::*;

    fn worker_event(proxy_id: usize) -> Event {
        Event::Worker(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Started,
        })
    }

    /// THE contract: a full queue sheds instead of blocking, and every
    /// shed event is counted.
    #[test]
    fn full_queue_sheds_and_counts() {
        let (tx, _consumer) = channel(2);
        for i in 0..5 {
            tx.emit(worker_event(i));
        }
        assert_eq!(tx.dropped_total(), 3);
    }

    /// delivered events arrive in emit order.
    #[tokio::test]
    async fn delivered_events_stay_ordered() {
        let (tx, mut consumer) = channel(8);
        for i in 0..4 {
            tx.emit(worker_event(i));
        }
        drop(tx);
        let mut seen = Vec::new();
        while let Some(Event::Worker(w)) = consumer.rx.recv().await {
            seen.push(w.proxy_id);
        }
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }

    /// emit must work from sync contexts - Drop impls fire tko events.
    #[test]
    fn emit_from_drop_impl_works() {
        struct EmitsOnDrop(EventSender);
        impl Drop for EmitsOnDrop {
            fn drop(&mut self) {
                self.0.emit(worker_event(99));
            }
        }

        let (tx, mut consumer) = channel(4);
        drop(EmitsOnDrop(tx.clone()));
        let event = consumer.rx.try_recv().expect("event emitted from Drop");
        assert!(matches!(
            event,
            Event::Worker(WorkerEventRecord { proxy_id: 99, .. })
        ));
    }

    /// senders are cheap clones sharing one drop counter.
    #[test]
    fn cloned_senders_share_the_drop_counter() {
        let (tx, _consumer) = channel(1);
        let tx2 = tx.clone();
        tx.emit(worker_event(0)); // fills the queue
        tx2.emit(worker_event(1)); // shed
        tx.emit(worker_event(2)); // shed
        assert_eq!(tx.dropped_total(), 2);
        assert_eq!(tx2.dropped_total(), 2);
    }

    /// closed-channel emits (shutdown race) count as drops, not panics.
    #[tokio::test]
    async fn emit_after_consumer_drop_is_a_counted_drop() {
        let (tx, consumer) = channel(4);
        drop(consumer);
        tx.emit(worker_event(0));
        assert_eq!(tx.dropped_total(), 1);
    }

    /// run() exits when the last sender drops - the shutdown story.
    #[tokio::test]
    async fn run_exits_when_senders_are_gone() {
        let (tx, consumer) = channel(4);
        tx.emit(worker_event(0));
        drop(tx);
        consumer.run().await; // must return, not hang
    }
}
