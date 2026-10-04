use tokio::sync::oneshot;

pub(crate) enum ControlCommand {
    WorkersReady,
    Shutdown { acknowledged: oneshot::Sender<()> },
}
