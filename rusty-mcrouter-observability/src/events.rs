use rusty_mcrouter_net::tko::TkoEventRecord;
use rusty_mcrouter_proxy::WorkerEventRecord;

pub enum Event {
    Tko(TkoEventRecord),
    Worker(WorkerEventRecord),
}
