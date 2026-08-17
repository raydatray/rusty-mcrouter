use rusty_mcrouter_net::tko::TkoEventRecord;

pub enum Event {
    Tko(TkoEventRecord),
    Worker(WorkerEventRecord),
}

// temporary - these should move to the binary
#[derive(Clone, Copy, Debug)]
pub struct WorkerEventRecord {
    pub proxy_id: usize,
    pub event: WorkerEvent,
}

#[derive(Clone, Copy, Debug)]
pub enum WorkerEvent {
    Started,
    Stopped,
}
