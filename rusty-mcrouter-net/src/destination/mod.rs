mod config;
mod counters;
// destination::destination is deliberate: the module owns key/config/
// counters/probe siblings and the struct keeps the domain name
#[allow(clippy::module_inception)]
mod destination;
mod key;
mod map;
mod probe;

pub use config::Config;
pub use counters::{DestinationCounters, DestinationCountersRegistry};
pub use destination::Destination;
pub use key::Key;
pub use map::Map;
