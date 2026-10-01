use std::{cell::RefCell, rc::Rc, sync::Arc};

use rusty_mcrouter_backend::destination::{self, DestinationConfig};
use rusty_mcrouter_backend::DestinationFactory;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::{
    build_route_with_options, BuildError, DynRoute, RootRouteOptions, RoutingEventSink,
    RoutingMetricsShard, RoutingState,
};

pub(crate) struct RouteGeneration {
    pub(crate) generation: u64,
    pub(crate) route: Rc<dyn DynRoute>,
    pub(crate) state: Rc<RoutingState>,
}

/// Borrowed only to clone or replace, never across an `.await`.
pub(crate) struct RouteSlot(RefCell<Rc<RouteGeneration>>);

impl RouteSlot {
    pub(crate) fn new(initial: Rc<RouteGeneration>) -> Rc<Self> {
        Rc::new(Self(RefCell::new(initial)))
    }

    pub(crate) fn current(&self) -> Rc<RouteGeneration> {
        Rc::clone(&self.0.borrow())
    }

    pub(crate) fn replace(&self, next: Rc<RouteGeneration>) -> Rc<RouteGeneration> {
        self.0.replace(next)
    }
}

pub(crate) struct GenerationSetup {
    pub(crate) destinations: Rc<destination::Map>,
    pub(crate) defaults: DestinationConfig,
    pub(crate) root_options: RootRouteOptions,
    pub(crate) metrics: Arc<RoutingMetricsShard>,
    pub(crate) events: RoutingEventSink,
}

pub(crate) struct GenerationBuilder {
    destinations: Rc<destination::Map>,
    defaults: DestinationConfig,
    root_options: RootRouteOptions,
    routing_metrics: Arc<RoutingMetricsShard>,
    routing_events: Rc<RoutingEventSink>,
}

impl GenerationBuilder {
    pub(crate) fn new(setup: GenerationSetup) -> Self {
        Self {
            destinations: setup.destinations,
            defaults: setup.defaults,
            root_options: setup.root_options,
            routing_metrics: setup.metrics,
            routing_events: Rc::new(setup.events),
        }
    }

    /// Must run while the current generation is still alive: destinations,
    /// gates and pool metrics dedup through weak maps, so building first is
    /// what hands the new graph the live connections, health and counters.
    pub(crate) fn build(
        &self,
        generation: u64,
        config: &ConfigDocument,
    ) -> Result<Rc<RouteGeneration>, BuildError> {
        // per generation, so its gate cache can't keep removed pools' gates alive
        let factory = DestinationFactory::new(Rc::clone(&self.destinations));
        let route = build_route_with_options(config, &factory, &self.defaults, &self.root_options)?;
        let state = RoutingState::new(
            Arc::clone(&self.routing_metrics),
            Rc::clone(&self.routing_events),
            config,
        );
        Ok(Rc::new(RouteGeneration {
            generation,
            route,
            state,
        }))
    }
}
