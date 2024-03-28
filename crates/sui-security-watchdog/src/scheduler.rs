// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::downloader::GithubRepoDownloader;
use crate::metrics::WatchdogMetrics;
use crate::query_runner::{QueryRunner, SnowflakeQueryRunner};
use crate::SecurityWatchdogConfig;
use chrono::{DateTime, Utc};
use log::info;
use prometheus::{IntGauge, Registry};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

// Define the main scheduler struct. This will manage all scheduled jobs and their execution.
#[derive(Clone, Serialize, Deserialize)]
pub struct ScheduleEntry {
    name: String,
    cron_schedule: String,
    sql_query: String,
    metric_name: String,
    timed_upper_limits: BTreeMap<DateTime<Utc>, f64>,
    timed_lower_limits: BTreeMap<DateTime<Utc>, f64>,
    timed_exact_limits: BTreeMap<DateTime<Utc>, f64>,
}

pub struct SchedulerService {
    scheduler: JobScheduler,
    query_runner: Arc<dyn QueryRunner>,
    metrics: Arc<WatchdogMetrics>,
    downloader: GithubRepoDownloader,
    scheduled_jobs: Mutex<HashSet<Uuid>>, // Tracks scheduled job IDs for cancellation
    last_config_hash: Mutex<Option<u64>>, // Tracks hash of the last config file for change detection
}

impl SchedulerService {
    pub fn new(config: &SecurityWatchdogConfig, registry: &Registry) -> anyhow::Result<Self> {
        Ok(Self {
            scheduler: JobScheduler::new()?,
            query_runner: Arc::new(SnowflakeQueryRunner::from_config(config)?),
            metrics: Arc::new(WatchdogMetrics::new(registry)),
            downloader: GithubRepoDownloader::from_config(config)?,
            scheduled_jobs: Mutex::new(HashSet::new()),
            last_config_hash: Mutex::new(None),
        })
    }

    // Start the scheduler, executing its main loop and handling scheduled tasks.
    pub async fn start(&self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15 * 60)); // 15 minutes
        loop {
            interval.tick().await;
            if let Err(e) = self.process_tasks().await {
                info!("Error processing tasks: {}", e);
            }
        }
    }

    async fn process_tasks(&self) -> anyhow::Result<()> {
        // Download or update the repository to ensure we have the latest configuration.
        self.downloader.download()?;

        let entries = self.downloader.read_entries()?;
        let new_config_hash = self.calculate_hash(&entries)?;

        let mut last_hash = self.last_config_hash.lock().await;

        if last_hash.is_none() || *last_hash.as_ref()? != new_config_hash {
            info!("Config file has changed. Updating scheduled tasks.");
            self.cancel_all_jobs().await;
            self.schedule_new_jobs(&entries).await?;
            *last_hash = Some(new_config_hash);
        }

        Ok(())
    }

    async fn cancel_all_jobs(&self) {
        let mut scheduled_jobs = self.scheduled_jobs.lock().await;
        for job_id in scheduled_jobs.iter() {
            self.scheduler.remove(job_id).await?;
        }
        scheduled_jobs.clear();
    }

    async fn schedule_new_jobs(&self, entries: &[ScheduleEntry]) -> anyhow::Result<()> {
        let mut scheduled_jobs = self.scheduled_jobs.lock().await;
        for entry in entries {
            let job_id = Self::schedule_job(
                entry.clone(),
                self.scheduler.clone(),
                self.query_runner.clone(),
                self.metrics.clone(),
            )
            .await?;
            scheduled_jobs.insert(job_id);
        }
        Ok(())
    }

    async fn schedule_job(
        entry: ScheduleEntry,
        scheduler: JobScheduler,
        query_runner: Arc<dyn QueryRunner>,
        metrics: Arc<WatchdogMetrics>,
    ) -> anyhow::Result<Uuid> {
        let job = Job::new_async(entry.cron_schedule, |_uuid, _lock| {
            Box::pin(async move {
                info!("Running job: {}", entry.name);
                let ScheduleEntry {
                sql_query,
                timed_exact_limits,
                timed_upper_limits,
                timed_lower_limits,
                metric_name,
                .. // Ignore remaining fields
            } = entry;
                let res = query_runner.run(sql_query).await?;
                let update_metrics = |limits: &BTreeMap<DateTime<Utc>, f64>, metric: &IntGauge| {
                    if let Some(value) = Self::get_current_limit(limits) {
                        metric.set((res - value) as i64);
                    } else {
                        metric.set(0);
                    }
                };

                update_metrics(&timed_exact_limits, metrics.get_exact(&metric_name).await?);
                update_metrics(&timed_upper_limits, metrics.get_upper(&metric_name).await?);
                update_metrics(&timed_lower_limits, metrics.get_lower(&metric_name).await?);
            })
        })?;
        let job_id = scheduler.add(job).await?;
        info!("Scheduled job: {}", entry.name);
        Ok(job_id)
    }

    fn calculate_hash<T: std::hash::Hash>(&self, value: &T) -> anyhow::Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        Ok(hasher.finish())
    }

    fn get_current_limit(limits: &BTreeMap<DateTime<Utc>, f64>) -> Option<f64> {
        limits.range(..Utc::now()).next_back().map(|(_, val)| *val)
    }
}
