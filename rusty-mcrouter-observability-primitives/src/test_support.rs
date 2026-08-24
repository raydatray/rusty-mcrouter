use std::sync::{Arc, Mutex};

use crate::EventSink;

pub type EventLog<T> = Arc<Mutex<Vec<T>>>;

pub fn noop_sink<T: Send + 'static>() -> EventSink<T> {
    EventSink::new(drop)
}

pub fn recording_sink<T: Send + 'static>() -> (EventSink<T>, EventLog<T>) {
    recording_sink_with(std::convert::identity)
}

pub fn recording_sink_with<T, U>(
    map: impl Fn(T) -> U + Send + Sync + 'static,
) -> (EventSink<T>, EventLog<U>)
where
    T: Send + 'static,
    U: Send + 'static,
{
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink_events = Arc::clone(&events);
    let sink = EventSink::new(move |event| {
        sink_events.lock().unwrap().push(map(event));
    });
    (sink, events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provides_noop_and_recording_sinks() {
        noop_sink().emit("ignored");

        let (sink, events) = recording_sink();
        sink.emit("recorded");
        assert_eq!(*events.lock().unwrap(), vec!["recorded"]);

        let (sink, events) = recording_sink_with(str::len);
        sink.emit("mapped");
        assert_eq!(*events.lock().unwrap(), vec![6]);
    }
}
