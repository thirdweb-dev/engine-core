use std::time::Duration;

#[derive(Clone, Debug)]
pub struct QueueOptions {
    pub max_success: usize,
    pub max_failed: usize,
    pub lease_duration: Duration,
    pub local_concurrency: usize,

    pub polling_interval: Duration,

    /// If true, always poll for jobs even if there are no available permits
    /// This is important, because polling is how delayed and timed out jobs are handled
    /// If you have a horiztonally scaled deployment, this can be set to the default of false
    /// But if there's only one node, you can set this to true to avoid the local concurrency from blocking queue housekeeping
    pub always_poll: bool,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            max_success: 1000,
            max_failed: 10000,
            local_concurrency: 100,
            polling_interval: Duration::from_millis(100),
            lease_duration: Duration::from_secs(30),
            always_poll: false,
        }
    }
}
