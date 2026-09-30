use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use rusty_mcrouter_backend::destination::DestinationConfig;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::RootRouteOptions;
use rusty_mcrouter_observability::{ConfigMetrics, ReloadStage};
use rusty_mcrouter_proxy::ProxyHandle;
use tokio::time::{interval_at, Instant, Interval, MissedTickBehavior};

use crate::config;

pub struct ReloaderConfig {
    pub path: PathBuf,
    pub delay: Duration,
    pub running: (Vec<u8>, Arc<ConfigDocument>),
    pub proxies: Vec<ProxyHandle>,
    pub defaults: DestinationConfig,
    pub root_options: RootRouteOptions,
    pub metrics: Arc<ConfigMetrics>,
}

/// See docs/design/0002-config-reload.md.
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
    pub fn new(cfg: ReloaderConfig) -> Self {
        let (bytes, document) = cfg.running;
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
                self.metrics.applied(generation);
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
            config::parse(&bytes).map_err(|error| Rejected::new(ReloadStage::Parse, error))?;
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
