use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use rusty_mcrouter_backend::destination::DestinationConfig;
use rusty_mcrouter_config::RoutingPrefix;
use rusty_mcrouter_core::RootRouteOptions;

#[derive(Debug, Parser)]
pub(crate) struct Args {
    // mcrouter: McrouterOptions::config (--config); path-only here.
    #[arg(
        long,
        value_name = "PATH",
        help = "path to mcrouter-format JSON config file"
    )]
    pub(crate) config: PathBuf,

    // mcrouter supports address/port lists; rusty-mcrouter currently binds one endpoint.
    #[arg(
        long,
        value_name = "ADDR",
        default_value = "127.0.0.1:5000",
        help = "address to listen on"
    )]
    listen: String,

    // mcrouter: McrouterOptions::num_proxies (--num-proxies).
    #[arg(
        long,
        value_name = "N",
        default_value_t = 1,
        help = "num of proxy threads"
    )]
    pub(crate) num_proxies: usize,

    // mcrouter: McrouterStandaloneOptions::num_listening_sockets.
    #[arg(
        long,
        value_name = "M",
        default_value_t = 1,
        help = "number of SO_REUSEPORT listening sockets"
    )]
    pub(crate) num_listening_sockets: usize,

    // rusty-mcrouter only; no direct mcrouter option.
    #[arg(
        long,
        value_name = "ADDR",
        default_value = "127.0.0.1:5001",
        help = "address for the prometheus /metrics endpoint"
    )]
    metrics_addr: String,

    // mcrouter: McrouterOptions::default_route (--route-prefix).
    #[arg(
        short = 'R',
        long = "route-prefix",
        default_value = "/././",
        help = "default routing prefix"
    )]
    default_route: RoutingPrefix,

    // mcrouter: McrouterOptions::send_invalid_route_to_default.
    #[arg(
        long,
        help = "send requests with unknown routing prefixes to the default route"
    )]
    send_invalid_route_to_default: bool,

    // mcrouter: McrouterOptions::server_timeout_ms (--server-timeout).
    #[arg(
        short = 't',
        long = "server-timeout",
        default_value_t = 1000,
        help = "per-request reply timeout, ms; also the connect timeout default"
    )]
    server_timeout_ms: u64,

    // mcrouter: McrouterOptions::connect_timeout_retries.
    #[arg(
        long,
        default_value_t = 0,
        help = "extra connect attempts after a connect TIMEOUT (other connect errors never retry)"
    )]
    connect_timeout_retries: usize,

    // mcrouter: McrouterOptions::failures_until_tko (--timeouts-until-tko).
    #[arg(
        long = "timeouts-until-tko",
        default_value_t = 3,
        help = "consecutive soft failures (timeouts) before a server is marked TKO"
    )]
    failures_until_tko: u64,

    // mcrouter: McrouterOptions::probe_delay_initial_ms (--probe-timeout-initial).
    #[arg(
        short = 'r',
        long = "probe-timeout-initial",
        default_value_t = 10_000,
        help = "first probe delay after a TKO mark, ms"
    )]
    probe_delay_initial_ms: u64,

    // mcrouter: McrouterOptions::probe_delay_max_ms (--probe-timeout-max).
    #[arg(
        long = "probe-timeout-max",
        default_value_t = 60_000,
        help = "probe backoff ceiling, ms"
    )]
    probe_delay_max_ms: u64,

    // mcrouter: McrouterOptions::reset_inactive_connection_interval.
    #[arg(
        long = "reset-inactive-connection-interval",
        default_value_t = 60_000,
        help = "idle connections are closed within at most 2x this interval, ms; 0 disables"
    )]
    reset_inactive_connection_interval: u64,

    // mcrouter: McrouterOptions::disable_tko_tracking.
    #[arg(long, help = "disable TKO tracking entirely (no fast-fail, no probes)")]
    disable_tko_tracking: bool,
}

impl Args {
    pub(crate) fn from_cli() -> anyhow::Result<Self> {
        let args = <Self as Parser>::parse();
        args.validate()?;
        Ok(args)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.num_proxies > 0, "num_proxies must be >= 1");
        anyhow::ensure!(
            self.num_listening_sockets > 0,
            "num_listening_sockets must be >= 1"
        );
        anyhow::ensure!(
            self.num_listening_sockets <= self.num_proxies,
            "num_listening_sockets ({}) must be <= num_proxies ({})",
            self.num_listening_sockets,
            self.num_proxies
        );
        Ok(())
    }

    pub(crate) fn listen_addr(&self) -> anyhow::Result<SocketAddr> {
        resolve_address("listen", &self.listen)
    }

    pub(crate) fn metrics_addr(&self) -> anyhow::Result<SocketAddr> {
        resolve_address("metrics", &self.metrics_addr)
    }

    pub(crate) fn destination_defaults(&self) -> DestinationConfig {
        DestinationConfig {
            // connect_timeout defaults to the server timeout, like mcrouter
            // (McRouteHandleProvider-inl.h:197-205); pools may override both
            connect_timeout: Some(Duration::from_millis(self.server_timeout_ms)),
            reply_timeout: Some(Duration::from_millis(self.server_timeout_ms)),
            connect_timeout_retries: self.connect_timeout_retries,
            failures_until_tko: self.failures_until_tko,
            probe_delay_initial: Duration::from_millis(self.probe_delay_initial_ms),
            probe_delay_max: Duration::from_millis(self.probe_delay_max_ms),
            disable_tko_tracking: self.disable_tko_tracking,
        }
    }

    pub(crate) fn root_route_options(&self) -> RootRouteOptions {
        RootRouteOptions {
            default_route: self.default_route.clone(),
            send_invalid_to_default: self.send_invalid_route_to_default,
        }
    }

    pub(crate) fn sweep_interval(&self) -> Duration {
        Duration::from_millis(self.reset_inactive_connection_interval)
    }
}

fn resolve_address(name: &str, value: &str) -> anyhow::Result<SocketAddr> {
    value
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve {name} address: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(extra: &[&str]) -> Args {
        let mut args = vec!["rusty-mcrouter", "--config", "config.json"];
        args.extend_from_slice(extra);
        Args::try_parse_from(args).unwrap()
    }

    #[test]
    fn root_route_options_use_mcrouter_defaults() {
        let options = parse_args(&[]).root_route_options();

        assert_eq!(options.default_route.as_str(), "/././");
        assert!(!options.send_invalid_to_default);
    }

    #[test]
    fn root_route_options_accept_short_and_long_flags() {
        let options =
            parse_args(&["-R", "/a/a/", "--send-invalid-route-to-default"]).root_route_options();

        assert_eq!(options.default_route.as_str(), "/a/a/");
        assert!(options.send_invalid_to_default);
    }

    #[test]
    fn route_prefix_rejects_malformed_values() {
        assert!(Args::try_parse_from([
            "rusty-mcrouter",
            "--config",
            "config.json",
            "--route-prefix",
            "/invalid/",
        ])
        .is_err());
    }

    #[test]
    fn mapped_options_use_mcrouter_flag_names() {
        let args = parse_args(&[
            "--server-timeout",
            "11",
            "--timeouts-until-tko",
            "12",
            "--probe-timeout-initial",
            "13",
            "--probe-timeout-max",
            "14",
            "--reset-inactive-connection-interval",
            "15",
        ]);
        let defaults = args.destination_defaults();

        assert_eq!(defaults.reply_timeout, Some(Duration::from_millis(11)));
        assert_eq!(defaults.failures_until_tko, 12);
        assert_eq!(defaults.probe_delay_initial, Duration::from_millis(13));
        assert_eq!(defaults.probe_delay_max, Duration::from_millis(14));
        assert_eq!(args.sweep_interval(), Duration::from_millis(15));
    }

    #[test]
    fn validates_thread_and_listener_counts() {
        assert!(parse_args(&["--num-proxies", "0"]).validate().is_err());
        assert!(parse_args(&["--num-listening-sockets", "0"])
            .validate()
            .is_err());
        assert!(
            parse_args(&["--num-proxies", "1", "--num-listening-sockets", "2"])
                .validate()
                .is_err()
        );
    }
}
