mod config;
// destination::destination is deliberate: the module owns key/config/
// counters/probe siblings and the struct keeps the domain name
#[allow(clippy::module_inception)]
mod destination;
mod key;
mod map;
mod metrics;
mod probe;

pub use config::Config;
pub use destination::Destination;
pub use key::DestinationKey;
pub use map::Map;
pub use metrics::{DestinationMetrics, DestinationMetricsRegistry};
