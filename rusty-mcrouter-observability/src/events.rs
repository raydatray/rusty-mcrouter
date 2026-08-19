use rusty_mcrouter_backend::tko::TkoEventRecord;
use rusty_mcrouter_core::RoutingEventRecord;
use rusty_mcrouter_proxy::WorkerEventRecord;

pub enum Event {
    Tko(TkoEventRecord),
    Routing(RoutingEventRecord),
    Worker(WorkerEventRecord),
}

impl From<TkoEventRecord> for Event {
    fn from(record: TkoEventRecord) -> Self {
        Self::Tko(record)
    }
}

impl From<WorkerEventRecord> for Event {
    fn from(record: WorkerEventRecord) -> Self {
        Self::Worker(record)
    }
}

impl From<RoutingEventRecord> for Event {
    fn from(record: RoutingEventRecord) -> Self {
        Self::Routing(record)
    }
}
