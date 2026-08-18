pub struct EventSink<T: Send + 'static>(Box<dyn Fn(T) + Send + Sync + 'static>);

impl<T: Send + 'static> EventSink<T> {
    pub fn new(sink: impl Fn(T) + Send + Sync + 'static) -> Self {
        Self(Box::new(sink))
    }

    pub fn emit(&self, event: T) {
        (self.0)(event);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn sink_delivers_owned_events() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = {
            let seen = Arc::clone(&seen);
            EventSink::new(move |event| seen.lock().unwrap().push(event))
        };

        sink.emit(String::from("started"));
        assert_eq!(&*seen.lock().unwrap(), &["started"]);
    }
}
