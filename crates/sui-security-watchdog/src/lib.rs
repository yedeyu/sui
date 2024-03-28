// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use arrow_array::Array;

use anyhow::Context;
use chrono::TimeZone;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io::Read;
use std::path::PathBuf;

mod downloader;
mod metrics;
mod query_runner;
pub mod scheduler;

#[derive(Parser, Clone, Debug)]
#[clap(
    name = "Sui Security Watchdog",
    about = "Watchdog service to monitor chain data.",
    rename_all = "kebab-case"
)]
pub struct SecurityWatchdogConfig {
    #[clap(long)]
    pub github_username: String,
    #[clap(long)]
    pub github_private_key_path: PathBuf,
    #[clap(long)]
    pub github_repo: String,
    #[clap(long)]
    pub github_config_relative_file_path: PathBuf,
    #[clap(long)]
    pub local_file_path: PathBuf,
    #[clap(long, default_value = None, global = true)]
    pub sf_account_identifier: Option<String>,
    #[clap(long, default_value = None, global = true)]
    pub sf_warehouse: Option<String>,
    #[clap(long, default_value = None, global = true)]
    pub sf_database: Option<String>,
    #[clap(long, default_value = None, global = true)]
    pub sf_schema: Option<String>,
    #[clap(long, default_value = None, global = true)]
    pub sf_username: Option<String>,
    #[clap(long, default_value = None, global = true)]
    pub sf_role: Option<String>,
    #[clap(long, default_value = None, global = true)]
    pub sf_password: Option<String>,
    /// The url of the metrics client to connect to.
    #[clap(long, default_value = "127.0.0.1", global = true)]
    pub client_metric_host: String,
    /// The port of the metrics client to connect to.
    #[clap(long, default_value = "8081", global = true)]
    pub client_metric_port: u16,
}
