use rusty_mcrouter_backend::tko::TkoEventRecord;
use rusty_mcrouter_proxy::WorkerEventRecord;

pub enum Event {
    Tko(TkoEventRecord),
    Worker(WorkerEventRecord),
}
