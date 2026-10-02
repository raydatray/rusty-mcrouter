use std::sync::{Arc, Mutex};

use crate::EventSink;

pub type EventLog<T> = Arc<Mutex<Vec<T>>>;

pub fn noop_sink<T>() -> Box<dyn EventSink<T>>
where
    T: Send + 'static,
{
    Box::new(|_: T| {})
}

pub fn recording_sink<T>() -> (Box<dyn EventSink<T>>, EventLog<T>)
where
    T: Send + 'static,
{
    recording_sink_with(std::convert::identity)
}

pub fn recording_sink_with<T, U>(
    map: impl Fn(T) -> U + Send + Sync + 'static,
) -> (Box<dyn EventSink<T>>, EventLog<U>)
where
    T: Send + 'static,
    U: Send + 'static,
{
    let events: EventLog<U> = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::clone(&events);

    let sink = move |event: T| {
        let recorded = map(event);
        output.lock().unwrap().push(recorded);
    };

    (Box::new(sink), events)
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
