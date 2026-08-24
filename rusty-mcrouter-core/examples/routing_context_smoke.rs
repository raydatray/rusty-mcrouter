use std::hint::black_box;
use std::rc::Rc;
use std::time::{Duration, Instant};

use rusty_mcrouter_core::{
    DynRoute, ErrorRoute, NullRoute, Route, RouteContext, RouteError, RoutingMetricsLayout,
    RoutingMetricsShard, RoutingState,
};
use rusty_mcrouter_observability_primitives::test_support::noop_sink;
use rusty_mcrouter_protocol::test_support::get;
use rusty_mcrouter_protocol::{Reply, Request};

const WARMUP: usize = 10_000;
const ITERATIONS: usize = 1_000_000;
const FORWARDING_DEPTH: usize = 16;

struct Forward {
    child: Rc<dyn DynRoute>,
}

impl Route for Forward {
    async fn route(
        &self,
        context: &RouteContext<'_>,
        request: Request,
    ) -> Result<Reply, RouteError> {
        self.child.route_dyn(context, request).await
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("routing context smoke runtime");

    runtime.block_on(async {
        let ignored_context_route = ErrorRoute::new(None).into_dyn();
        let read_context_route = NullRoute.into_dyn();
        let deep_route = forwarding_chain(Rc::clone(&read_context_route));
        let request = get(b"routing-context-smoke");
        let layout = RoutingMetricsLayout::new(Vec::<String>::new());
        let routing_state = RoutingState::new(RoutingMetricsShard::new(layout), noop_sink());
        let context = routing_state.context();

        run_direct(&ignored_context_route, &context, &request, WARMUP).await;
        run_direct(&read_context_route, &context, &request, WARMUP).await;
        run_entry(&routing_state, &read_context_route, &request, WARMUP).await;
        run_direct(&deep_route, &context, &request, WARMUP).await;
        run_entry(&routing_state, &deep_route, &request, WARMUP).await;

        let direct_ignored =
            run_direct(&ignored_context_route, &context, &request, ITERATIONS).await;
        let direct_read = run_direct(&read_context_route, &context, &request, ITERATIONS).await;
        let entry_read = run_entry(&routing_state, &read_context_route, &request, ITERATIONS).await;
        let deep_direct = run_direct(&deep_route, &context, &request, ITERATIONS).await;
        let deep_entry = run_entry(&routing_state, &deep_route, &request, ITERATIONS).await;

        println!(
            "direct, context ignored: {:.2} ns/op",
            ns_per_op(direct_ignored)
        );
        println!(
            "direct, NullRoute:       {:.2} ns/op",
            ns_per_op(direct_read)
        );
        println!(
            "entry, NullRoute:        {:.2} ns/op",
            ns_per_op(entry_read)
        );
        println!(
            "deep({FORWARDING_DEPTH}), direct:    {:.2} ns/op",
            ns_per_op(deep_direct)
        );
        println!(
            "deep({FORWARDING_DEPTH}), entry:     {:.2} ns/op",
            ns_per_op(deep_entry)
        );
    });
}

fn forwarding_chain(mut route: Rc<dyn DynRoute>) -> Rc<dyn DynRoute> {
    for _ in 0..FORWARDING_DEPTH {
        route = Forward { child: route }.into_dyn();
    }
    route
}

async fn run_direct(
    route: &Rc<dyn DynRoute>,
    context: &RouteContext<'_>,
    request: &Request,
    iterations: usize,
) -> Duration {
    let started = Instant::now();
    for _ in 0..iterations {
        let reply = Rc::clone(route)
            .route_dyn(black_box(context), black_box(request.clone()))
            .await
            .expect("benchmark route cannot fail");
        black_box(reply);
    }
    started.elapsed()
}

async fn run_entry(
    routing_state: &RoutingState,
    route: &Rc<dyn DynRoute>,
    request: &Request,
    iterations: usize,
) -> Duration {
    let started = Instant::now();
    for _ in 0..iterations {
        let context = routing_state.context();
        let reply = Rc::clone(route)
            .route_dyn(black_box(&context), black_box(request.clone()))
            .await
            .expect("benchmark route cannot fail");
        black_box(reply);
    }
    started.elapsed()
}

fn ns_per_op(duration: Duration) -> f64 {
    duration.as_nanos() as f64 / ITERATIONS as f64
}
