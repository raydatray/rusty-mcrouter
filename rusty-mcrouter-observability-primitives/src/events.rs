pub trait EventSink<T: Send + 'static>: Send + Sync {
    fn emit(&self, event: T);
}
