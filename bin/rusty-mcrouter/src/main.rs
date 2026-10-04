mod app;
mod args;
mod config;
mod control;
mod lifecycle;
mod worker_fleet;

fn main() -> anyhow::Result<()> {
    app::run()
}
