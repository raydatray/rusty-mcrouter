use rusty_mcrouter_observability_primitives::EventSink;

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

pub type WorkerEventSink = EventSink<WorkerEventRecord>;
