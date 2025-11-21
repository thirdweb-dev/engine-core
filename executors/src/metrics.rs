use lazy_static::lazy_static;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, Registry, TextEncoder,
    register_histogram_vec_with_registry,
};
use std::sync::Arc;

/// Metrics configuration for executor metrics
pub struct ExecutorMetrics {
    pub transaction_queued_to_sent_duration: HistogramVec,
    pub transaction_queued_to_confirmed_duration: HistogramVec,
    pub eoa_job_processing_duration: HistogramVec,
    // EOA degradation and stuck metrics (low cardinality - only problematic EOAs)
    pub eoa_degraded_send_duration: HistogramVec,
    pub eoa_degraded_confirmation_duration: HistogramVec,
    pub eoa_stuck_duration: HistogramVec,
}

impl ExecutorMetrics {
    /// Create new executor metrics with the provided registry
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let transaction_queued_to_sent_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "tw_engine_executor_transaction_queued_to_sent_duration_seconds",
                "Time from when transaction is queued to when it's sent to the network"
            )
            .buckets(vec![
                0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0
            ]),
            &["executor_type", "chain_id"],
            registry
        )?;

        let transaction_queued_to_confirmed_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "tw_engine_executor_transaction_queued_to_confirmed_duration_seconds",
                "Time from when transaction is queued to when it's confirmed on-chain"
            )
            .buckets(vec![
                0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0
            ]),
            &["executor_type", "chain_id"],
            registry
        )?;

        let eoa_job_processing_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "tw_engine_eoa_executor_job_processing_duration_seconds",
                "Time taken for EOA executor to process a single job (may include multiple transactions)"
            ).buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]),
            &["chain_id"],
            registry
        )?;

        // EOA degradation and stuck metrics (low cardinality - only problematic EOAs)
        let eoa_degraded_send_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "tw_engine_executor_eoa_degraded_send_duration_seconds",
                "Duration of EOA transactions that exceeded the send degradation threshold"
            )
            .buckets(vec![5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0]),
            &["eoa_address", "chain_id"],
            registry
        )?;

        let eoa_degraded_confirmation_duration =
            register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "tw_engine_executor_eoa_degraded_confirmation_duration_seconds",
                "Duration of EOA transactions that exceeded the confirmation degradation threshold"
            ).buckets(vec![30.0, 45.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0]),
            &["eoa_address", "chain_id"],
            registry
        )?;

        let eoa_stuck_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "tw_engine_executor_eoa_stuck_duration_seconds",
                "Duration since last nonce movement for EOAs that are considered stuck"
            )
            .buckets(vec![
                200.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0, 7200.0, 14400.0
            ]),
            &["eoa_address", "chain_id", "out_of_funds"],
            registry
        )?;

        Ok(ExecutorMetrics {
            transaction_queued_to_sent_duration,
            transaction_queued_to_confirmed_duration,
            eoa_job_processing_duration,
            eoa_degraded_send_duration,
            eoa_degraded_confirmation_duration,
            eoa_stuck_duration,
        })
    }
}

lazy_static! {
    /// Default metrics registry for executors (fallback if no external registry provided)
    static ref DEFAULT_EXECUTOR_METRICS_REGISTRY: Registry = Registry::new();

    /// Default executor metrics instance (used when no external metrics are provided)
    static ref DEFAULT_EXECUTOR_METRICS: ExecutorMetrics =
        ExecutorMetrics::new(&DEFAULT_EXECUTOR_METRICS_REGISTRY)
            .expect("Failed to create default executor metrics");

    /// Global metrics instance - can be set by the binary crate or uses default
    static ref EXECUTOR_METRICS_INSTANCE: std::sync::RwLock<Option<Arc<ExecutorMetrics>>> =
        std::sync::RwLock::new(None);
}

/// Initialize executor metrics with a custom registry
/// This should be called once at application startup by the binary crate
pub fn initialize_metrics(metrics: ExecutorMetrics) {
    let mut instance = EXECUTOR_METRICS_INSTANCE.write().unwrap();
    *instance = Some(Arc::new(metrics));
}

/// Get the current metrics instance (either custom or default)
fn get_metrics() -> Arc<ExecutorMetrics> {
    let instance = EXECUTOR_METRICS_INSTANCE.read().unwrap();
    match instance.as_ref() {
        Some(metrics) => metrics.clone(),
        None => {
            // Use default metrics if no custom metrics were set
            // This ensures backward compatibility
            drop(instance); // Release read lock
            Arc::new(
                ExecutorMetrics::new(&DEFAULT_EXECUTOR_METRICS_REGISTRY)
                    .expect("Failed to create default metrics"),
            )
        }
    }
}

/// Export metrics in Prometheus format from the default registry
/// For custom registries, the binary crate should handle metrics export directly
pub fn export_default_metrics() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let encoder = TextEncoder::new();
    let metric_families = DEFAULT_EXECUTOR_METRICS_REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

/// Record transaction timing metrics
pub fn record_transaction_queued_to_sent(
    executor_type: &str,
    chain_id: u64,
    duration_seconds: f64,
) {
    let metrics = get_metrics();
    metrics
        .transaction_queued_to_sent_duration
        .with_label_values(&[executor_type, &chain_id.to_string()])
        .observe(duration_seconds);
}

pub fn record_transaction_queued_to_confirmed(
    executor_type: &str,
    chain_id: u64,
    duration_seconds: f64,
) {
    let metrics = get_metrics();
    metrics
        .transaction_queued_to_confirmed_duration
        .with_label_values(&[executor_type, &chain_id.to_string()])
        .observe(duration_seconds);
}

/// Record EOA job processing time
pub fn record_eoa_job_processing_time(chain_id: u64, duration_seconds: f64) {
    let metrics = get_metrics();
    metrics
        .eoa_job_processing_duration
        .with_label_values(&[&chain_id.to_string()])
        .observe(duration_seconds);
}

/// EOA Metrics abstraction that encapsulates configuration and provides clean interface
#[derive(Debug, Clone)]
pub struct EoaMetrics {
    pub send_degradation_threshold_seconds: u64,
    pub confirmation_degradation_threshold_seconds: u64,
    pub stuck_threshold_seconds: u64,
}

impl EoaMetrics {
    /// Create new EoaMetrics with configuration
    pub fn new(
        send_degradation_threshold_seconds: u64,
        confirmation_degradation_threshold_seconds: u64,
        stuck_threshold_seconds: u64,
    ) -> Self {
        Self {
            send_degradation_threshold_seconds,
            confirmation_degradation_threshold_seconds,
            stuck_threshold_seconds,
        }
    }

    /// Record EOA transaction send metrics with automatic degradation detection
    pub fn record_transaction_sent(
        &self,
        eoa_address: alloy::primitives::Address,
        chain_id: u64,
        duration_seconds: f64,
    ) {
        // Always record the regular metric
        record_transaction_queued_to_sent("eoa", chain_id, duration_seconds);

        // Only record degraded metric if threshold exceeded (low cardinality)
        if duration_seconds > self.send_degradation_threshold_seconds as f64 {
            let metrics = get_metrics();
            metrics
                .eoa_degraded_send_duration
                .with_label_values(&[&eoa_address.to_string(), &chain_id.to_string()])
                .observe(duration_seconds);
        }
    }

    /// Record EOA transaction confirmation metrics with automatic degradation detection
    pub fn record_transaction_confirmed(
        &self,
        eoa_address: alloy::primitives::Address,
        chain_id: u64,
        duration_seconds: f64,
    ) {
        // Always record the regular metric
        record_transaction_queued_to_confirmed("eoa", chain_id, duration_seconds);

        // Only record degraded metric if threshold exceeded (low cardinality)
        if duration_seconds > self.confirmation_degradation_threshold_seconds as f64 {
            let metrics = get_metrics();
            metrics
                .eoa_degraded_confirmation_duration
                .with_label_values(&[&eoa_address.to_string(), &chain_id.to_string()])
                .observe(duration_seconds);
        }
    }

    /// Record stuck EOA metric when nonce hasn't moved for too long
    pub fn record_stuck_eoa(
        &self,
        eoa_address: alloy::primitives::Address,
        chain_id: u64,
        time_since_last_movement_seconds: f64,
        out_of_funds: bool,
    ) {
        // Only record if EOA is actually stuck (exceeds threshold)
        if time_since_last_movement_seconds > self.stuck_threshold_seconds as f64 {
            let metrics = get_metrics();
            metrics
                .eoa_stuck_duration
                .with_label_values(&[
                    &eoa_address.to_string(),
                    &chain_id.to_string(),
                    &out_of_funds.to_string(),
                ])
                .observe(time_since_last_movement_seconds);
        }
    }

    /// Check if an EOA should be considered stuck based on time since last nonce movement
    pub fn is_stuck(&self, time_since_last_movement_ms: u64) -> bool {
        let time_since_last_movement_seconds = time_since_last_movement_ms as f64 / 1000.0;
        time_since_last_movement_seconds > self.stuck_threshold_seconds as f64
    }
}

/// Helper to calculate duration in seconds from unix timestamps (milliseconds)
pub fn calculate_duration_seconds(start_timestamp_ms: u64, end_timestamp_ms: u64) -> f64 {
    (end_timestamp_ms.saturating_sub(start_timestamp_ms)) as f64 / 1000.0
}

/// Helper to calculate duration in seconds when start timestamp is in seconds (TWMQ format)
pub fn calculate_duration_seconds_from_twmq(
    start_timestamp_seconds: u64,
    end_timestamp_ms: u64,
) -> f64 {
    let start_timestamp_ms = start_timestamp_seconds * 1000;
    calculate_duration_seconds(start_timestamp_ms, end_timestamp_ms)
}

/// Get current timestamp in milliseconds
pub fn current_timestamp_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_duration_seconds() {
        // Test normal case
        let start = 1000;
        let end = 3500;
        assert_eq!(calculate_duration_seconds(start, end), 2.5);

        // Test edge case where end < start (should return 0)
        assert_eq!(calculate_duration_seconds(3500, 1000), 0.0);
    }

    #[test]
    fn test_calculate_duration_seconds_from_twmq() {
        // Test TWMQ timestamp (seconds) to milliseconds conversion
        let start_seconds = 1640995200; // Unix timestamp in seconds
        let end_ms = 1640995203500; // Unix timestamp in milliseconds (3.5 seconds later)
        assert_eq!(
            calculate_duration_seconds_from_twmq(start_seconds, end_ms),
            3.5
        );

        // Test edge case where end < start (should return 0)
        assert_eq!(
            calculate_duration_seconds_from_twmq(1640995203, 1640995200000),
            0.0
        );
    }

    #[test]
    fn test_metrics_recording() {
        // Test that metrics can be recorded without panicking
        record_transaction_queued_to_sent("test", 1, 1.5);
        record_transaction_queued_to_confirmed("test", 1, 10.0);
        record_eoa_job_processing_time(1, 2.0);

        // Test new EOA metrics abstraction
        let eoa_metrics = EoaMetrics::new(10, 120, 600);
        let test_address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        eoa_metrics.record_transaction_sent(test_address, 1, 5.0); // Won't record degradation (below threshold)
        eoa_metrics.record_transaction_sent(test_address, 1, 15.0); // Will record degradation (above threshold)
        eoa_metrics.record_transaction_confirmed(test_address, 1, 60.0); // Won't record degradation (below threshold)
        eoa_metrics.record_transaction_confirmed(test_address, 1, 180.0); // Will record degradation (above threshold)
        eoa_metrics.record_stuck_eoa(test_address, 1, 900.0, false); // Will record stuck EOA

        // Test that default metrics can be exported
        let metrics_output =
            export_default_metrics().expect("Should be able to export default metrics");
        assert!(
            metrics_output
                .contains("tw_engine_executor_transaction_queued_to_sent_duration_seconds")
        );
        assert!(
            metrics_output
                .contains("tw_engine_executor_transaction_queued_to_confirmed_duration_seconds")
        );
        assert!(metrics_output.contains("tw_engine_eoa_executor_job_processing_duration_seconds"));
        assert!(metrics_output.contains("tw_engine_executor_eoa_degraded_send_duration_seconds"));
        assert!(
            metrics_output
                .contains("tw_engine_executor_eoa_degraded_confirmation_duration_seconds")
        );
        assert!(metrics_output.contains("tw_engine_executor_eoa_stuck_duration_seconds"));
    }

    #[test]
    fn test_custom_metrics_registry() {
        // Test using a custom registry
        let custom_registry = Registry::new();
        let custom_metrics =
            ExecutorMetrics::new(&custom_registry).expect("Should create custom metrics");

        // Initialize with custom metrics
        initialize_metrics(custom_metrics);

        // Record some metrics
        record_transaction_queued_to_sent("custom_test", 42, 2.5);

        // Export from custom registry
        let encoder = TextEncoder::new();
        let metric_families = custom_registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .expect("Should encode metrics");
        let metrics_output = String::from_utf8(buffer).expect("Should convert to string");

        assert!(
            metrics_output
                .contains("tw_engine_executor_transaction_queued_to_sent_duration_seconds")
        );
        assert!(metrics_output.contains("custom_test"));
        assert!(metrics_output.contains("42"));
    }
}
