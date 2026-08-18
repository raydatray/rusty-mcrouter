use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
    pub addr: Arc<str>,
    pub reply_timeout: Option<Duration>,
}
