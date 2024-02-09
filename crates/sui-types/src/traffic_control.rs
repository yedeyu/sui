// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, net::IpAddr};

use crate::error::SuiResult;
use chrono::{DateTime, Utc};
use core::hash::Hash;
use jsonrpsee::core::server::helpers::MethodResponse;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::Debug;
use tracing::info;

#[derive(Clone, Debug)]
pub enum ServiceResponse {
    Validator(SuiResult),
    Fullnode(MethodResponse),
}

impl PartialEq for ServiceResponse {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ServiceResponse::Validator(a), ServiceResponse::Validator(b)) => a == b,
            (ServiceResponse::Fullnode(a), ServiceResponse::Fullnode(b)) => {
                a.error_code == b.error_code && a.success == b.success
            }
            _ => false,
        }
    }
}

impl Eq for ServiceResponse {}

impl Hash for ServiceResponse {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ServiceResponse::Validator(result) => result.hash(state),
            ServiceResponse::Fullnode(response) => {
                response.error_code.hash(state);
                response.success.hash(state);
            }
        }
    }
}

impl ServiceResponse {
    pub fn is_ok(&self) -> bool {
        match self {
            ServiceResponse::Validator(result) => result.is_ok(),
            ServiceResponse::Fullnode(response) => response.success,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TrafficTally {
    pub connection_ip: Option<IpAddr>,
    pub proxy_ip: Option<IpAddr>,
    pub result: ServiceResponse,
    pub timestamp: DateTime<Utc>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RemoteFirewallConfig {
    pub remote_fw_url: String,
    pub destination_port: u16,
    #[serde(default)]
    pub delegate_spam_blocking: bool,
    #[serde(default)]
    pub delegate_error_blocking: bool,
}

// Serializable representation of policy types, used in config
// in order to easily change in tests or to killswitch
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub enum PolicyType {
    /// Does nothing
    #[default]
    NoOp,

    /* Below this point are test policies, and thus should not be used in production */
    ///
    /// Simple policy that adds connection_ip to blocklist when the same connection_ip
    /// is encountered in tally N times. If used in an error policy, this would trigger
    /// after N errors
    TestNConnIP(u64),
    /// Test policy that inspects the proxy_ip and connection_ip to ensure they are present
    /// in the tally. Tests IP forwarding. To be used only in tests that submit transactions
    /// through a client
    TestInspectIp,
    /// Test policy that panics when invoked. To be used as an error policy in tests that do
    /// not expect request errors in order to verify that the error policy is not invoked
    TestPanicOnInvocation,
}

#[derive(Clone, Debug, Default)]
pub struct PolicyResponse {
    pub block_connection_ip: Option<SocketAddr>,
    pub block_proxy_ip: Option<SocketAddr>,
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
    TestNConnIP(TestNConnIPPolicy),
    TestInspectIp(TestInspectIpPolicy),
    TestPanicOnInvocation(TestPanicOnInvocationPolicy),
}

impl Policy for TrafficControlPolicy {
    fn handle_tally(&mut self, tally: TrafficTally) -> PolicyResponse {
        match self {
            TrafficControlPolicy::NoOp(policy) => policy.handle_tally(tally),
            TrafficControlPolicy::TestNConnIP(policy) => policy.handle_tally(tally),
            TrafficControlPolicy::TestInspectIp(policy) => policy.handle_tally(tally),
            TrafficControlPolicy::TestPanicOnInvocation(policy) => policy.handle_tally(tally),
        }
    }

    fn policy_config(&self) -> &PolicyConfig {
        match self {
            TrafficControlPolicy::NoOp(policy) => policy.policy_config(),
            TrafficControlPolicy::TestNConnIP(policy) => policy.policy_config(),
            TrafficControlPolicy::TestInspectIp(policy) => policy.policy_config(),
            TrafficControlPolicy::TestPanicOnInvocation(policy) => policy.policy_config(),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct PolicyConfig {
    #[serde(default = "default_connection_blocklist_ttl_sec")]
    pub connection_blocklist_ttl_sec: u64,
    #[serde(default)]
    pub proxy_blocklist_ttl_sec: u64,
    #[serde(default)]
    pub spam_policy_type: PolicyType,
    #[serde(default)]
    pub error_policy_type: PolicyType,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

pub fn default_connection_blocklist_ttl_sec() -> u64 {
    60
}
pub fn default_channel_capacity() -> usize {
    100
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
            PolicyType::TestNConnIP(n) => {
                TrafficControlPolicy::TestNConnIP(TestNConnIPPolicy::new(self.clone(), *n))
            }
            PolicyType::TestInspectIp => {
                TrafficControlPolicy::TestInspectIp(TestInspectIpPolicy::new(self.clone()))
            }
            PolicyType::TestPanicOnInvocation => TrafficControlPolicy::TestPanicOnInvocation(
                TestPanicOnInvocationPolicy::new(self.clone()),
            ),
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
        PolicyResponse::default()
    }

    fn policy_config(&self) -> &PolicyConfig {
        &self.config
    }
}

////////////// *** Test policies below this point *** //////////////

#[derive(Clone)]
pub struct TestNConnIPPolicy {
    config: PolicyConfig,
    frequencies: HashMap<IpAddr, u64>,
    threshold: u64,
}

impl TestNConnIPPolicy {
    pub fn new(config: PolicyConfig, threshold: u64) -> Self {
        Self {
            config,
            frequencies: HashMap::new(),
            threshold,
        }
    }

    fn handle_tally(&mut self, tally: TrafficTally) -> PolicyResponse {
        // increment the count for the IP
        let ip = tally.connection_ip.unwrap();
        let count = self.frequencies.entry(ip).or_insert(0);
        *count += 1;
        PolicyResponse {
            block_connection_ip: if *count >= self.threshold {
                Some(ip)
            } else {
                None
            },
            block_proxy_ip: false,
        }
    }

    fn policy_config(&self) -> &PolicyConfig {
        &self.config
    }
}

#[derive(Clone)]
pub struct TestInspectIpPolicy {
    config: PolicyConfig,
}

impl TestInspectIpPolicy {
    pub fn new(config: PolicyConfig) -> Self {
        Self { config }
    }

    fn handle_tally(&mut self, tally: TrafficTally) -> PolicyResponse {
        assert!(tally.proxy_ip.is_some(), "Expected proxy_ip to be present");
        PolicyResponse {
            block_connection_ip: None,
            block_proxy_ip: None,
        }
    }

    fn policy_config(&self) -> &PolicyConfig {
        &self.config
    }
}

#[derive(Clone)]
pub struct TestPanicOnInvocationPolicy {
    config: PolicyConfig,
}

impl TestPanicOnInvocationPolicy {
    pub fn new(config: PolicyConfig) -> Self {
        Self { config }
    }

    fn handle_tally(&mut self, _: TrafficTally) -> PolicyResponse {
        panic!("Tally for this policy should never be invoked")
    }

    fn policy_config(&self) -> &PolicyConfig {
        &self.config
    }
}
