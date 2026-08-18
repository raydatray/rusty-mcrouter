use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub connect_timeout: Option<Duration>,
    pub reply_timeout: Option<Duration>,
    pub connect_timeout_retries: usize,
    pub failures_until_tko: u64,
    pub probe_delay_initial: Duration,
    pub probe_delay_max: Duration,
    pub disable_tko_tracking: bool,
}

impl Default for Config {
    /// mcrouter's defaults (mcrouter_options_list.h): server_timeout 1000ms
    /// (connect_timeout defaults to it), 0 connect retries, 3 failures to
    /// TKO, probes from 10s backing off to 60s, tracking enabled.
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_millis(1000)),
            reply_timeout: Some(Duration::from_millis(1000)),
            connect_timeout_retries: 0,
            failures_until_tko: 3,
            probe_delay_initial: Duration::from_secs(10),
            probe_delay_max: Duration::from_secs(60),
            disable_tko_tracking: false,
        }
    }
}
