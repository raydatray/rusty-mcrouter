pub trait Selector: 'static {
    fn select(&self, routing_key: &[u8]) -> usize;
}
