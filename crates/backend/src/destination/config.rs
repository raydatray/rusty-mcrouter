use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DestinationConfig {
    pub connect_timeout: Duration,
    pub reply_timeout: Duration,
    pub connect_timeout_retries: usize,
    pub failures_until_tko: u64,
    pub probe_delay_initial: Duration,
    pub probe_delay_max: Duration,
    pub disable_tko_tracking: bool,
}

impl Default for DestinationConfig {
    /// mcrouter's defaults (mcrouter_options_list.h)
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(1000),
            reply_timeout: Duration::from_millis(1000),
            connect_timeout_retries: 0,
            failures_until_tko: 3,
            probe_delay_initial: Duration::from_secs(10),
            probe_delay_max: Duration::from_secs(60),
            disable_tko_tracking: false,
        }
    }
}
