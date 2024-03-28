// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Context};
use prometheus::{register_int_gauge, IntGauge, IntGaugeVec, Registry};
use std::collections::HashMap;
use tokio::sync::Mutex;

/// Defines a structure to hold and manage metrics for a watchdog service.
/// This structure is thread-safe, allowing concurrent access and modification of metrics.
#[derive(Clone)]
pub struct WatchdogMetrics {
    // The Prometheus registry to which metrics are registered.
    registry: Registry,
    // A HashMap to store IntGauge metrics, keyed by their names.
    // Wrapped in a Mutex to ensure thread-safe access.
    metrics: Mutex<HashMap<String, IntGauge>>,
}

impl WatchdogMetrics {
    /// Constructs a new WatchdogMetrics instance with the given Prometheus registry
    pub fn new(registry: &Registry) -> Self {
        Self {
            registry: registry.clone(),
            metrics: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieves or creates an "exact" metric for the specified metric name.
    /// The metric name is suffixed with "_exact" to denote its type.
    pub async fn get_exact(&self, metric_name: &str) -> anyhow::Result<&IntGauge> {
        let mut metrics = self.metrics.lock().await;
        let metric = format!("{}_exact", metric_name);
        // If the metric doesn't exist, register it and insert into the map.
        metrics
            .entry(metric)
            .or_insert(register_int_gauge!(metric, &self.registry).unwrap());
        metrics
            .get(&metric)
            .context("Failed to get expected metric")
    }

    /// Similar to get_exact, but for "lower" bound metrics.
    pub async fn get_lower(&self, metric_name: &str) -> anyhow::Result<&IntGauge> {
        let mut metrics = self.metrics.lock().await;
        let metric = format!("{}_lower", metric_name);
        metrics
            .entry(metric)
            .or_insert(register_int_gauge!(metric, &self.registry).unwrap());
        metrics
            .get(&metric)
            .context("Failed to get expected metric")
    }

    /// Similar to get_exact, but for "upper" bound metrics.
    pub async fn get_upper(&self, metric_name: &str) -> anyhow::Result<&IntGauge> {
        let mut metrics = self.metrics.lock().await;
        let metric = format!("{}_upper", metric_name);
        metrics
            .entry(metric)
            .or_insert(register_int_gauge!(metric, &self.registry).unwrap());
        metrics
            .get(&metric)
            .context("Failed to get expected metric")
    }
}
