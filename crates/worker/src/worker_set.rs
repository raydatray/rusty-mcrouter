use crate::WorkerHandle;

/// Worker placement for a connection's request mailbox. All requests use the
/// mailbox, including requests served by the connection's own worker.
#[derive(Clone, Copy)]
pub enum ThreadMode {
    SameThread,
    // todo - thread modes: constructed once dispatch policy is configurable
    #[allow(dead_code)]
    FixedRemote {
        worker_id: usize,
    },
    #[allow(dead_code)]
    AffinitizedRemote,
}

#[derive(Clone)]
pub struct WorkerSet {
    workers: Vec<WorkerHandle>,
}

impl WorkerSet {
    pub fn new(workers: Vec<WorkerHandle>) -> Self {
        assert!(!workers.is_empty(), "workerset empty");

        Self { workers }
    }

    pub fn choose(&self, mode: ThreadMode, current_id: usize) -> WorkerHandle {
        let idx = match mode {
            ThreadMode::SameThread => current_id,
            ThreadMode::FixedRemote { worker_id } => worker_id % self.workers.len(),
            ThreadMode::AffinitizedRemote => current_id, // todo - implement request-based affinity
        };

        self.workers[idx].clone()
    }

    pub fn nth(&self, n: usize) -> &WorkerHandle {
        &self.workers[n % self.workers.len()]
    }
}
