//! Control-plane coordination and its fact-owned telemetry.

mod handle;
mod message;
mod reload;
mod runtime;
mod setup;

pub use rusty_mcrouter_config::{ConfigMetrics, ReloadStage};

pub use handle::{ControlHandle, ControlInbox};
pub use reload::{ConfigReloader, ReloaderSetup, RunningConfig};
pub use runtime::ControlRuntime;
pub use setup::ControlSetup;
