use std::{sync::Arc, time::Duration};

use rusty_mcrouter_observability_primitives::{Counter, EventSink};
use tokio::time::Instant;

use crate::{events::Event, logging};

pub struct EventSender {
    tx: tokio::sync::mpsc::Sender<Event>,
    dropped: Arc<Counter>,
}

impl Clone for EventSender {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            dropped: Arc::clone(&self.dropped),
        }
    }
}

impl EventSender {
    pub fn sink<T>(&self) -> EventSink<T>
    where
        T: Send + 'static,
        Event: From<T>,
    {
        let sender = self.clone();

        EventSink::new(move |event: T| sender.emit(event.into()))
    }

    pub fn emit(&self, event: Event) {
        if self.tx.try_send(event).is_err() {
            // either full or closed, either way we cannot block so move on
            self.dropped.inc();
        }
    }

    pub fn dropped_total(&self) -> u64 {
        self.dropped.load()
    }

    pub fn dropped_counter(&self) -> Arc<Counter> {
        Arc::clone(&self.dropped)
    }
}

const DROP_WARN_INTERVAL: Duration = Duration::from_secs(1);

pub struct EventConsumer {
    rx: tokio::sync::mpsc::Receiver<Event>,
    dropped: Arc<Counter>,
    last_seen: u64,
    last_warned: Instant,
}

pub fn channel(capacity: usize) -> (EventSender, EventConsumer) {
    assert!(capacity > 0, "a zero-capacity event bus drops everything");

    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    let dropped = Arc::new(Counter::default());

    (
        EventSender {
            tx,
            dropped: Arc::clone(&dropped),
        },
        EventConsumer {
            rx,
            dropped,
            last_seen: 0,
            last_warned: Instant::now() - DROP_WARN_INTERVAL,
        },
    )
}

impl EventConsumer {
    pub async fn recv(&mut self) -> Option<Event> {
        let event = self.rx.recv().await;
        if event.is_some() {
            self.warn_if_shedding_events();
        }
        event
    }

    pub fn try_recv(&mut self) -> Option<Event> {
        let event = self.rx.try_recv().ok();
        if event.is_some() {
            self.warn_if_shedding_events();
        }
        event
    }

    pub async fn run(mut self) {
        while let Some(event) = self.recv().await {
            logging::write(&event);
        }
    }

    fn warn_if_shedding_events(&mut self) {
        let dropped = self.dropped.load();
        if dropped > self.last_seen && self.last_warned.elapsed() >= DROP_WARN_INTERVAL {
            tracing::warn!(
                target: "rusty-mcrouter-observability::bus",
                dropped_total = dropped,
                new = dropped - self.last_seen,
                "event bus shed events; log stream incomplete"
            );
            self.last_seen = dropped;
            self.last_warned = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_proxy::{WorkerEvent, WorkerEventRecord};

    use super::*;

    fn worker_record(proxy_id: usize) -> WorkerEventRecord {
        WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Started,
        }
    }

    fn worker_event(proxy_id: usize) -> Event {
        Event::Worker(worker_record(proxy_id))
    }

    /// THE contract: a full queue sheds instead of blocking, and every
    /// shed event is counted.
    #[test]
    fn full_queue_sheds_and_counts() {
        let (tx, _consumer) = channel(2);
        let sink = tx.sink::<WorkerEventRecord>();
        for i in 0..5 {
            sink.emit(worker_record(i));
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
