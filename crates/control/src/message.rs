use tokio::sync::oneshot;

pub(crate) enum ControlCommand {
    ProxiesReady,
    Shutdown { acknowledged: oneshot::Sender<()> },
}
