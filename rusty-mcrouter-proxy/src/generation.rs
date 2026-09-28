use std::{rc::Rc, sync::Arc};

use rusty_mcrouter_backend::destination::{self, DestinationConfig};
use rusty_mcrouter_backend::DestinationFactory;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::{
    build_route_with_options, BuildError, DynRoute, RootRouteOptions, RoutingEventSink,
    RoutingMetricsShard, RoutingState,
};

pub(crate) struct RouteGeneration {
    pub(crate) route: Rc<dyn DynRoute>,
    pub(crate) state: Rc<RoutingState>,
}

pub(crate) struct GenerationBuilder {
    destinations: Rc<destination::Map>,
    defaults: DestinationConfig,
    root_options: RootRouteOptions,
    routing_metrics: Arc<RoutingMetricsShard>,
    routing_events: Rc<RoutingEventSink>,
}

impl GenerationBuilder {
    pub(crate) fn new(
        destinations: Rc<destination::Map>,
        defaults: DestinationConfig,
        root_options: RootRouteOptions,
        routing_metrics: Arc<RoutingMetricsShard>,
        routing_events: RoutingEventSink,
    ) -> Self {
        Self {
            destinations,
            defaults,
            root_options,
            routing_metrics,
            routing_events: Rc::new(routing_events),
        }
    }

    /// Must run while the current generation is still alive: destinations,
    /// gates and pool metrics dedup through weak maps, so building first is
    /// what hands the new graph the live connections, health and counters.
    pub(crate) fn build(&self, config: &ConfigDocument) -> Result<Rc<RouteGeneration>, BuildError> {
        // per generation, so its gate cache can't keep removed pools' gates alive
        let factory = DestinationFactory::new(Rc::clone(&self.destinations));
        let route = build_route_with_options(config, &factory, &self.defaults, &self.root_options)?;
        let state = RoutingState::new(
            Arc::clone(&self.routing_metrics),
            Rc::clone(&self.routing_events),
            config,
        );
        Ok(Rc::new(RouteGeneration { route, state }))
    }
}
