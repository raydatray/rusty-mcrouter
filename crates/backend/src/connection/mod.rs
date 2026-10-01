mod actor;
mod config;
mod handle;
mod types;

pub use actor::Connection;
pub use config::{BackendConnectionConfig, ConnectionInbox, ConnectionResources, ConnectionSetup};
pub use handle::ConnectionHandle;
pub use types::{ConnectionEvent, DownReason};
