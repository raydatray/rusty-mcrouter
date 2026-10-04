use rusty_mcrouter_observability_primitives::EventSink;

#[derive(Clone, Copy, Debug)]
pub struct WorkerEventRecord {
    pub worker_id: usize,
    pub event: WorkerEvent,
}

#[derive(Clone, Copy, Debug)]
pub enum WorkerEvent {
    Started,
    Stopped,
}

pub type WorkerEventSink = Box<dyn EventSink<WorkerEventRecord>>;
