use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use rusty_mcrouter_backend::destination::DestinationConfig;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::RootRouteOptions;
use rusty_mcrouter_proxy::ProxyHandle;
use tokio::time::{interval_at, Instant, Interval, MissedTickBehavior};

use crate::{ConfigMetrics, ReloadStage};

fn parse_config(bytes: &[u8]) -> anyhow::Result<ConfigDocument> {
    let text = std::str::from_utf8(bytes).context("config is not valid UTF-8")?;
    Ok(rusty_mcrouter_config::parse(text)?)
}

pub struct RunningConfig {
    pub bytes: Vec<u8>,
    pub document: Arc<ConfigDocument>,
}

pub struct ReloaderSetup {
    pub path: PathBuf,
    pub delay: Duration,
    pub running: RunningConfig,
    pub proxies: Vec<ProxyHandle>,
    pub defaults: DestinationConfig,
    pub root_options: RootRouteOptions,
    pub metrics: Arc<ConfigMetrics>,
}

/// Watches settled config changes and coordinates generation application across proxies.
pub struct ConfigReloader {
    path: PathBuf,
    delay: Duration,
    ticker: Option<Interval>,
    watch: Watch,
    last_seen: FileState,
    running: Running,
    next_generation: u64,
    proxies: Vec<ProxyHandle>,
    defaults: DestinationConfig,
    root_options: RootRouteOptions,
    metrics: Arc<ConfigMetrics>,
}

#[derive(Clone, Debug, PartialEq)]
enum FileState {
    Bytes(Vec<u8>),
    Unreadable(io::ErrorKind),
}

impl FileState {
    fn read(path: &Path) -> Self {
        match std::fs::read(path) {
            Ok(bytes) => Self::Bytes(bytes),
            Err(error) => Self::Unreadable(error.kind()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Watch {
    Idle,
    /// Seen a tick ago; applied after a re-read, like mcrouter's
    /// reconfiguration delay (ConfigApi.cpp:224-240).
    Settling,
}

struct Running {
    generation: u64,
    bytes: Vec<u8>,
    document: Arc<ConfigDocument>,
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Applied(u64),
    Unchanged,
    ShuttingDown,
}

struct Rejected {
    stage: ReloadStage,
    error: anyhow::Error,
}

impl Rejected {
    fn new(stage: ReloadStage, error: anyhow::Error) -> Self {
        Self { stage, error }
    }
}

impl ConfigReloader {
    pub fn new(cfg: ReloaderSetup) -> Self {
        let RunningConfig { bytes, document } = cfg.running;
        Self {
            path: cfg.path,
            delay: cfg.delay,
            ticker: None,
            watch: Watch::Idle,
            last_seen: FileState::Bytes(bytes.clone()),
            running: Running {
                generation: 1,
                bytes,
                document,
            },
            next_generation: 2,
            proxies: cfg.proxies,
            defaults: cfg.defaults,
            root_options: cfg.root_options,
            metrics: cfg.metrics,
        }
    }

    /// Cancel-safe. The interval is created lazily because it needs a runtime.
    pub async fn tick(&mut self) {
        let delay = self.delay;
        self.ticker
            .get_or_insert_with(|| {
                let mut ticker = interval_at(Instant::now() + delay, delay);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                ticker
            })
            .tick()
            .await;
    }

    pub async fn poll(&mut self) {
        let state = FileState::read(&self.path);
        match self.watch {
            Watch::Settling => {
                self.watch = Watch::Idle;
                if state != self.last_seen {
                    self.last_seen = state.clone();
                    self.attempt(state).await;
                }
            }
            Watch::Idle if state != self.last_seen => self.watch = Watch::Settling,
            Watch::Idle => {}
        }
    }

    async fn attempt(&mut self, state: FileState) {
        self.metrics.reload_attempts.inc();
        let started = Instant::now();
        match self.try_apply(state).await {
            Ok(Outcome::Applied(generation)) => {
                let applied_at = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_secs());
                self.metrics.applied(generation, applied_at);
                tracing::info!(
                    generation,
                    pools = self.running.document.pools().len(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "config reloaded"
                );
            }
            Ok(Outcome::Unchanged) => self.metrics.in_sync(),
            Ok(Outcome::ShuttingDown) => {
                tracing::debug!("config reload abandoned: proxies are stopping");
            }
            Err(Rejected { stage, error }) => {
                self.metrics.rejected(stage);
                tracing::error!(
                    stage = stage.label(),
                    running_generation = self.running.generation,
                    error = format!("{error:#}"),
                    "config reload rejected; keeping the running config"
                );
            }
        }
    }

    async fn try_apply(&mut self, state: FileState) -> Result<Outcome, Rejected> {
        let bytes = match state {
            FileState::Bytes(bytes) => bytes,
            FileState::Unreadable(kind) => {
                let error = anyhow!("cannot read `{}`: {kind}", self.path.display());
                return Err(Rejected::new(ReloadStage::Read, error));
            }
        };
        if bytes == self.running.bytes {
            return Ok(Outcome::Unchanged);
        }

        let document =
            parse_config(&bytes).map_err(|error| Rejected::new(ReloadStage::Parse, error))?;
        if document == *self.running.document {
            self.running.bytes = bytes;
            return Ok(Outcome::Unchanged);
        }
        rusty_mcrouter_core::validate(&document, &self.defaults, &self.root_options)
            .map_err(|error| Rejected::new(ReloadStage::Validate, error.into()))?;

        // not running.generation + 1: a part-failed apply must not reuse a number
        let generation = self.next_generation;
        self.next_generation += 1;
        let document = Arc::new(document);

        let mut pending = Vec::with_capacity(self.proxies.len());
        for proxy in &self.proxies {
            match proxy
                .begin_reconfigure(generation, Arc::clone(&document))
                .await
            {
                Ok(applied) => pending.push((proxy.id(), applied)),
                Err(_) => return Ok(Outcome::ShuttingDown),
            }
        }

        let mut failed = Vec::new();
        for (id, applied) in pending {
            match applied.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failed.push(format!("proxy-{id}: {error}")),
                Err(_) => return Ok(Outcome::ShuttingDown),
            }
        }
        if !failed.is_empty() {
            let error = anyhow!(failed.join("; "));
            return Err(Rejected::new(ReloadStage::Apply, error));
        }

        self.running = Running {
            generation,
            bytes,
            document,
        };
        Ok(Outcome::Applied(generation))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use rusty_mcrouter_proxy::ProxyCommand;

    use super::*;

    const V1: &str = r#"{ "route": "NullRoute" }"#;
    const V2: &str = r#"{ "route": "ErrorRoute|v2" }"#;

    struct Fixture {
        reloader: ConfigReloader,
        path: PathBuf,
        metrics: Arc<ConfigMetrics>,
        applied: Arc<Mutex<Vec<(usize, u64)>>>,
    }

    impl Fixture {
        fn new(name: &str, proxies: usize) -> Self {
            let path = std::env::temp_dir().join(format!(
                "rusty-mcrouter-reload-{}-{name}.json",
                std::process::id()
            ));
            std::fs::write(&path, V1).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            let document = parse_config(&bytes).unwrap();
            let metrics = ConfigMetrics::started(1);
            let applied = Arc::new(Mutex::new(Vec::new()));
            let handles = (0..proxies)
                .map(|id| fake_proxy(id, Arc::clone(&applied)))
                .collect();

            let reloader = ConfigReloader::new(ReloaderSetup {
                path: path.clone(),
                delay: Duration::from_millis(10),
                running: RunningConfig {
                    bytes,
                    document: Arc::new(document),
                },
                proxies: handles,
                defaults: DestinationConfig::default(),
                root_options: RootRouteOptions::default(),
                metrics: Arc::clone(&metrics),
            });
            Self {
                reloader,
                path,
                metrics,
                applied,
            }
        }

        fn write(&self, contents: &str) {
            std::fs::write(&self.path, contents).unwrap();
        }

        /// one tick to see the change, one to apply it
        async fn settle(&mut self) {
            self.reloader.poll().await;
            self.reloader.poll().await;
        }

        fn failures(&self, stage: ReloadStage) -> u64 {
            self.metrics.reload_failures[stage as usize].load()
        }

        fn applied(&self) -> Vec<(usize, u64)> {
            let mut applied = self.applied.lock().unwrap().clone();
            applied.sort();
            applied
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn fake_proxy(id: usize, applied: Arc<Mutex<Vec<(usize, u64)>>>) -> ProxyHandle {
        let (handle, inbox) = ProxyHandle::allocate(id);
        let mut commands = inbox.command_rx;
        tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                if let ProxyCommand::Reconfigure {
                    generation,
                    applied: ack,
                    ..
                } = command
                {
                    applied.lock().unwrap().push((id, generation));
                    let _ = ack.send(Ok(()));
                }
            }
        });
        handle
    }

    #[tokio::test]
    async fn applies_a_changed_file_to_every_proxy() {
        let mut fixture = Fixture::new("applies", 2);

        fixture.write(V2);
        fixture.reloader.poll().await;
        assert!(fixture.applied().is_empty(), "the first tick only settles");
        fixture.reloader.poll().await;

        assert_eq!(fixture.applied(), [(0, 2), (1, 2)]);
        assert_eq!(fixture.metrics.generation.load(), 2);
        assert_eq!(fixture.metrics.reload_attempts.load(), 1);
        assert_eq!(fixture.metrics.last_reload_successful.load(), 1);
    }

    #[tokio::test]
    async fn invalid_json_is_rejected_and_the_running_config_kept() {
        let mut fixture = Fixture::new("invalid-json", 1);

        fixture.write("{ not json");
        fixture.settle().await;

        assert!(fixture.applied().is_empty());
        assert_eq!(fixture.failures(ReloadStage::Parse), 1);
        assert_eq!(fixture.metrics.last_reload_successful.load(), 0);
        assert_eq!(fixture.metrics.generation.load(), 1);
    }

    #[tokio::test]
    async fn unbuildable_config_is_rejected_before_any_proxy_sees_it() {
        let mut fixture = Fixture::new("unbuildable", 1);

        // plural routes without the default /././ prefix
        fixture.write(r#"{ "routes": { "/a/b/": "NullRoute" } }"#);
        fixture.settle().await;

        assert!(fixture.applied().is_empty());
        assert_eq!(fixture.failures(ReloadStage::Validate), 1);
    }

    #[tokio::test]
    async fn a_broken_edit_is_not_sticky() {
        let mut fixture = Fixture::new("not-sticky", 1);

        fixture.write("{ half written");
        fixture.settle().await;
        fixture.write(V2);
        fixture.settle().await;

        assert_eq!(fixture.applied(), [(0, 2)]);
        assert_eq!(fixture.failures(ReloadStage::Parse), 1);
        assert_eq!(fixture.metrics.last_reload_successful.load(), 1);
    }

    #[tokio::test]
    async fn reverting_a_broken_edit_is_in_sync_without_a_new_generation() {
        let mut fixture = Fixture::new("revert", 1);

        fixture.write("{ broken");
        fixture.settle().await;
        fixture.write(V1);
        fixture.settle().await;

        assert!(fixture.applied().is_empty());
        assert_eq!(fixture.metrics.last_reload_successful.load(), 1);
        assert_eq!(fixture.metrics.generation.load(), 1);
    }

    #[tokio::test]
    async fn cosmetic_edit_is_a_no_op() {
        let mut fixture = Fixture::new("cosmetic", 1);

        fixture.write("// same routes\n{ \"route\":   \"NullRoute\" }");
        fixture.settle().await;

        assert!(fixture.applied().is_empty());
        assert_eq!(fixture.metrics.reload_attempts.load(), 1);
        assert_eq!(fixture.metrics.last_reload_successful.load(), 1);
    }

    #[tokio::test]
    async fn missing_file_is_reported_once() {
        let mut fixture = Fixture::new("missing", 1);

        std::fs::remove_file(&fixture.path).unwrap();
        for _ in 0..4 {
            fixture.reloader.poll().await;
        }

        assert_eq!(fixture.failures(ReloadStage::Read), 1);
        assert_eq!(fixture.metrics.generation.load(), 1);
    }

    #[tokio::test]
    async fn generations_are_never_reused() {
        let mut fixture = Fixture::new("generations", 1);

        fixture.write(V2);
        fixture.settle().await;
        fixture.write(V1);
        fixture.settle().await;

        assert_eq!(fixture.applied(), [(0, 2), (0, 3)]);
        assert_eq!(fixture.metrics.generation.load(), 3);
    }
}
