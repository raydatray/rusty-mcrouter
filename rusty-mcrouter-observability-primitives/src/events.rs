pub struct EventSink<T: Send + 'static>(Box<dyn Fn(T) + Send + Sync + 'static>);

impl<T: Send + 'static> EventSink<T> {
    pub fn new(sink: impl Fn(T) + Send + Sync + 'static) -> Self {
        Self(Box::new(sink))
    }

    pub fn emit(&self, event: T) {
        (self.0)(event);
    }
}
