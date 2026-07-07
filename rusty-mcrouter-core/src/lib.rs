mod failover;
mod route_builder;
mod routes;
mod selectors;

#[cfg(test)]
mod test_support;

pub use crate::route_builder::{build_route, BuildError};
pub use crate::routes::{
    DestinationRoute, DynRoute, ErrorRoute, FailoverRoute, NullRoute, PoolRoute, Route, RouteError,
    RouteFuture,
};
pub use crate::selectors::SelectorBuildError;
