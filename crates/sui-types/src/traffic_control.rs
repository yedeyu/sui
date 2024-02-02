// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, net::SocketAddr};

use crate::error::{SuiError, SuiResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct TrafficTally {
    pub connection_ip: Option<SocketAddr>,
    pub proxy_ip: Option<SocketAddr>,
    pub result: SuiResult,
    pub timestamp: DateTime<Utc>,
}

// Serializable representation of policy types, used in config
// in order to easily change in tests or to killswitch
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub enum PolicyType {
    /// Does nothing
    #[default]
    NoOp,
    /// Simple policy that blocks IP when any error code in
    /// `tallyable_error_codes` is encountered 3 times in a row
    SimpleErrorTest,
}

pub struct PolicyResponse {
    pub block_connection_ip: bool,
    pub block_proxy_ip: bool,
}

pub trait Policy {
    // returns, e.g. (true, false) if connection_ip should be added to blocklist
    // and proxy_ip should not
    fn handle_tally(&mut self, tally: TrafficTally) -> PolicyResponse;
    fn policy_config(&self) -> &PolicyConfig;
}

// Nonserializable representation, also note that inner types are
// not object safe, so we can't use a trait object instead
#[derive(Clone)]
pub enum TrafficControlPolicy {
    NoOp(NoOpPolicy),
    SimpleErrorTest(SimpleErrorTestPolicy),
}

impl Policy for TrafficControlPolicy {
    fn handle_tally(&mut self, tally: TrafficTally) -> PolicyResponse {
        match self {
            TrafficControlPolicy::NoOp(policy) => policy.handle_tally(tally),
            TrafficControlPolicy::SimpleErrorTest(policy) => policy.handle_tally(tally),
        }
    }

    fn policy_config(&self) -> &PolicyConfig {
        match self {
            TrafficControlPolicy::NoOp(policy) => policy.policy_config(),
            TrafficControlPolicy::SimpleErrorTest(policy) => policy.policy_config(),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct PolicyConfig {
    pub tallyable_error_codes: Vec<SuiError>,
    pub remote_blocklist_ttl_sec: u64,
    pub end_user_blocklist_ttl_sec: u64,
    pub spam_policy_type: PolicyType,
    pub error_policy_type: PolicyType,
    pub channel_capacity: usize,
}

impl PolicyConfig {
    pub fn to_spam_policy(&self) -> TrafficControlPolicy {
        self.to_policy(&self.spam_policy_type)
    }

    pub fn to_error_policy(&self) -> TrafficControlPolicy {
        self.to_policy(&self.error_policy_type)
    }

    fn to_policy(&self, policy_type: &PolicyType) -> TrafficControlPolicy {
        match policy_type {
            PolicyType::NoOp => TrafficControlPolicy::NoOp(NoOpPolicy::new(self.clone())),
            PolicyType::SimpleErrorTest => {
                TrafficControlPolicy::SimpleErrorTest(SimpleErrorTestPolicy::new(self.clone()))
            }
        }
    }
}

#[derive(Clone)]
pub struct NoOpPolicy {
    config: PolicyConfig,
}

impl NoOpPolicy {
    pub fn new(config: PolicyConfig) -> Self {
        Self { config }
    }

    fn handle_tally(&mut self, _tally: TrafficTally) -> PolicyResponse {
        PolicyResponse {
            block_connection_ip: false,
            block_proxy_ip: false,
        }
    }

    fn policy_config(&self) -> &PolicyConfig {
        &self.config
    }
}

#[derive(Clone)]
pub struct SimpleErrorTestPolicy {
    config: PolicyConfig,
    frequencies: HashMap<SocketAddr, u64>,
}

impl SimpleErrorTestPolicy {
    pub fn new(config: PolicyConfig) -> Self {
        Self {
            config,
            frequencies: HashMap::new(),
        }
    }

    fn handle_tally(&mut self, tally: TrafficTally) -> PolicyResponse {
        // increment the count for the IP
        let count = self
            .frequencies
            .entry(tally.connection_ip.unwrap())
            .or_insert(0);
        *count += 1;
        PolicyResponse {
            block_connection_ip: false,
            block_proxy_ip: *count >= 3,
        }
    }

    fn policy_config(&self) -> &PolicyConfig {
        &self.config
    }
}
