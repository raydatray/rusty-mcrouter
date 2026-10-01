use std::sync::mpsc::SyncSender;

/// Send exactly one readiness result, including executor or worker-build failures.
pub(crate) fn report_startup<T, A>(
    prepared: anyhow::Result<T>,
    ready_tx: SyncSender<anyhow::Result<A>>,
    address: impl FnOnce(&T) -> A,
) -> anyhow::Result<T> {
    match prepared {
        Ok(prepared) => {
            if ready_tx.send(Ok(address(&prepared))).is_err() {
                anyhow::bail!("startup receiver closed");
            }
            Ok(prepared)
        }
        Err(error) => {
            let _ = ready_tx.send(Err(anyhow::anyhow!("{error:#}")));
            Err(error)
        }
    }
}
