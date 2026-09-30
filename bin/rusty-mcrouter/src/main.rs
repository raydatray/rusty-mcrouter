mod app;
mod args;
mod config;
mod control;
mod proxy_fleet;
mod reload;

fn main() -> anyhow::Result<()> {
    app::run()
}
