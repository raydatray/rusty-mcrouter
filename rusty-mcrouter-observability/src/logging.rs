use rusty_mcrouter_net::tko::{TkoEvent, TkoEventRecord};
use rusty_mcrouter_proxy::{WorkerEvent, WorkerEventRecord};

use crate::events::Event;

pub fn write(event: &Event) {
    match event {
        Event::Tko(r) => tko(r),
        Event::Worker(r) => worker(r),
    }
}

fn tko(r: &TkoEventRecord) {
    let server = &*r.server;
    let pool = r.pool.as_deref();

    match r.event {
        TkoEvent::MarkSoftTko => tracing::warn!(
            target: "rusty-mcrouter-observability::tko",
            server,
            pool,
            reason = ?r.reason,
            consecutive_failures = r.consecutive_failures,
            global_soft_tkos = r.global_soft_tkos,
            global_hard_tkos = r.global_hard_tkos,
            "destination marked soft tko"
        ),
        TkoEvent::MarkHardTko => tracing::warn!(
            target: "rusty-mcrouter-observability::tko",
            server,
            pool,
            reason = ?r.reason,
            global_soft_tkos = r.global_soft_tkos,
            global_hard_tkos = r.global_hard_tkos,
            "destination marked hard tko"
        ),
        TkoEvent::UnMarkTko => tracing::info!(
            target: "rusty-mcrouter-observability::tko",
            server,
            pool,
            "destination recovered"
        ),
        TkoEvent::RemoveFromConfig => tracing::info!(
            target: "rusty-mcrouter-observability::tko",
            server,
            pool,
            "tko'd destination removed from config"
        ),
        TkoEvent::EnterFailOpen => tracing::error!(
            target: "rusty-mcrouter-observability::tko",
            server,
            pool,
            "pool entered fail-open: all destinations tko'd"
        ),
        TkoEvent::ExitFailOpen => tracing::info!(
            target: "rusty-mcrouter-observability::tko",
            server,
            pool,
            "pool exited fail-open"
        ),
    }
}

fn worker(r: &WorkerEventRecord) {
    match r.event {
        WorkerEvent::Started => tracing::info!(
            target: "rusty-mcrouter-observability::worker",
            proxy_id = r.proxy_id,
            "proxy worker started"
        ),
        WorkerEvent::Stopped => tracing::info!(
            target: "rusty-mcrouter-observability::worker",
            proxy_id = r.proxy_id,
            "proxy worker stopped"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rusty_mcrouter_net::classify::ResultCode;
    use tracing::level_filters::LevelFilter;
    use tracing::Level;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    use super::*;

    /// records (level, target) for every event fired while installed.
    #[derive(Clone, Default)]
    struct Collector(Arc<Mutex<Vec<(Level, String)>>>);

    impl<S: tracing::Subscriber> Layer<S> for Collector {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            self.0.lock().unwrap().push((
                *event.metadata().level(),
                event.metadata().target().to_string(),
            ));
        }
    }

    /// runs write() against each event with an isolated subscriber -
    /// no global state, so tests can't order-poison each other.
    fn levels_for(events: &[Event]) -> Vec<Level> {
        let collector = Collector::default();
        let subscriber = tracing_subscriber::registry()
            .with(LevelFilter::TRACE)
            .with(collector.clone());
        tracing::subscriber::with_default(subscriber, || {
            for event in events {
                write(event);
            }
        });
        let seen = collector.0.lock().unwrap();
        seen.iter().map(|(level, _)| *level).collect()
    }

    fn tko_event(event: TkoEvent) -> Event {
        Event::Tko(TkoEventRecord {
            event,
            server: Arc::from("10.0.0.1:11211"),
            pool: Some(Arc::from("test_pool")),
            reason: ResultCode::Timeout,
            consecutive_failures: 3,
            global_soft_tkos: 1,
            global_hard_tkos: 0,
        })
    }

    /// the level policy from design 0001, pinned event by event.
    #[test]
    fn levels_follow_the_documented_policy() {
        let levels = levels_for(&[
            tko_event(TkoEvent::MarkSoftTko),
            tko_event(TkoEvent::MarkHardTko),
            tko_event(TkoEvent::UnMarkTko),
            tko_event(TkoEvent::RemoveFromConfig),
            tko_event(TkoEvent::EnterFailOpen),
            tko_event(TkoEvent::ExitFailOpen),
        ]);
        assert_eq!(
            levels,
            vec![
                Level::WARN,  // MarkSoftTko
                Level::WARN,  // MarkHardTko
                Level::INFO,  // UnMarkTko
                Level::INFO,  // RemoveFromConfig
                Level::ERROR, // EnterFailOpen
                Level::INFO,  // ExitFailOpen
            ]
        );
    }

    #[test]
    fn worker_lifecycle_is_info() {
        let started = Event::Worker(WorkerEventRecord {
            proxy_id: 0,
            event: WorkerEvent::Started,
        });
        assert_eq!(levels_for(&[started]), vec![Level::INFO]);
    }

    /// every event writes exactly one line - nothing is silently eaten.
    #[test]
    fn every_event_produces_exactly_one_line() {
        let events: Vec<Event> = [
            TkoEvent::MarkSoftTko,
            TkoEvent::MarkHardTko,
            TkoEvent::UnMarkTko,
            TkoEvent::RemoveFromConfig,
            TkoEvent::EnterFailOpen,
            TkoEvent::ExitFailOpen,
        ]
        .into_iter()
        .map(tko_event)
        .collect();
        assert_eq!(levels_for(&events).len(), events.len());
    }
}
