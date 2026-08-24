pub trait EventSink<T>: Send + Sync
where
    T: Send + 'static,
{
    fn emit(&self, event: T);
}
