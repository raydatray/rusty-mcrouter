mod app;
mod args;
mod config;
mod control;
mod lifecycle;
mod proxy_fleet;

fn main() -> anyhow::Result<()> {
    app::run()
}
