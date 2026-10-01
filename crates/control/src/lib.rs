//! Control-plane coordination and its fact-owned telemetry.

mod handle;
mod message;
mod metrics;
mod reload;
mod runtime;
mod setup;

pub use handle::{ControlHandle, ControlInbox};
pub use metrics::{ConfigMetrics, ConfigSource, ReloadStage};
pub use reload::{ConfigReloader, ReloaderSetup, RunningConfig};
pub use runtime::ControlRuntime;
pub use setup::ControlSetup;
