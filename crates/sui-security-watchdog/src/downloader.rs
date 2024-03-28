// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::scheduler::ScheduleEntry;
use crate::SecurityWatchdogConfig;
use anyhow::Context;
use log::info;
use ssh2::Session;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::net::TcpStream;
use std::path::{Path, PathBuf};

pub struct GithubRepoDownloader {
    ssh_url: String,
    ssh_port: u32,
    ssh_user: String,
    ssh_key: PathBuf,
    repo_name: String,
    local_path: PathBuf,
    file_rel_path: PathBuf,
}

impl GithubRepoDownloader {
    pub fn new(
        ssh_url: &str,
        ssh_port: u32,
        ssh_user: &str,
        ssh_key: &Path,
        repo_name: &str,
        local_path: &Path,
        file_rel_path: &Path,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            ssh_url: ssh_url.to_string(),
            ssh_port,
            ssh_user: ssh_user.to_string(),
            ssh_key: ssh_key.to_path_buf(),
            repo_name: repo_name.to_string(),
            local_path: local_path.to_path_buf(),
            file_rel_path: file_rel_path.to_path_buf(),
        })
    }

    pub fn from_config(config: &SecurityWatchdogConfig) -> anyhow::Result<Self> {
        let ssh_url = format!("git@github.com:MystenLabs/{}.git", config.github_repo);
        Self::new(
            &ssh_url,
            22,
            &config.github_username,
            &config.github_private_key_path,
            &config.github_repo,
            &config.local_file_path,
            &config.github_config_relative_file_path,
        )
    }

    pub fn download(&self) -> anyhow::Result<()> {
        let tcp = TcpStream::connect(format!("{}:{}", &self.ssh_url, self.ssh_port))
            .context("Failed to connect over TCP")?;
        let mut session = Session::new()?;
        session.set_tcp_stream(tcp);
        session.handshake().context("SSH handshake failed")?;
        session.set_known_hosts(ssh2::KnownHosts::empty())?;
        session
            .userauth_pubkey_file(&self.ssh_user, None, &self.ssh_key, None)
            .context("SSH authentication failed")?;
        let mut channel = session.channel_session()?;
        let target_dir = self.local_path.join(&self.repo_name);
        if target_dir.exists() {
            fs::remove_dir_all(&target_dir)?;
        }
        info!("Cloning into {}", target_dir.display());
        channel.exec(format!(
            "git clone {} {}",
            self.local_path.display(),
            self.repo_name
        ))?;
        channel.wait_close()?;
        Ok(())
    }

    // Simplified path construction using `join`
    fn file_path(&self) -> PathBuf {
        self.local_path
            .join(&self.repo_name)
            .join(&self.file_rel_path)
    }

    pub fn read_entries(&self) -> anyhow::Result<Vec<ScheduleEntry>> {
        let mut file = File::open(self.file_path())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let entries: Vec<ScheduleEntry> = serde_json::from_str(&contents)?;
        Ok(entries)
    }
}
