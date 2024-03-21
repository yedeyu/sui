// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! NB: Tests in this module expect real network connections and interactions, thus they
//! should all be tokio::test rather than simtest. Any deviation from this should be well
//! understood and justified.

use jsonrpsee::{
    core::{client::ClientT, RpcResult},
    rpc_params,
};
use sui_core::traffic_controller::nodefw_test_server::NodeFwTestServer;
use sui_json_rpc_types::{
    SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse, SuiTransactionBlockResponseOptions,
};
use sui_swarm_config::network_config_builder::ConfigBuilder;
use sui_test_transaction_builder::batch_make_transfer_transactions;
use sui_types::{
    quorum_driver_types::ExecuteTransactionRequestType,
    traffic_control::{PolicyConfig, PolicyType, RemoteFirewallConfig},
};
use test_cluster::{TestCluster, TestClusterBuilder};

#[tokio::test]
async fn test_validator_traffic_control_ok() -> Result<(), anyhow::Error> {
    let policy_config = PolicyConfig {
        connection_blocklist_ttl_sec: 1,
        proxy_blocklist_ttl_sec: 5,
        // Test that IP forwarding works through this policy
        spam_policy_type: PolicyType::TestInspectIp,
        // This should never be invoked when set as an error policy
        // as we are not sending requests that error
        error_policy_type: PolicyType::TestPanicOnInvocation,
        channel_capacity: 100,
    };
    let network_config = ConfigBuilder::new_with_temp_dir()
        .with_policy_config(Some(policy_config))
        .build();
    let test_cluster = TestClusterBuilder::new()
        .set_network_config(network_config)
        .build()
        .await;

    assert_traffic_control_ok(test_cluster).await
}

#[tokio::test]
async fn test_fullnode_traffic_control_ok() -> Result<(), anyhow::Error> {
    let policy_config = PolicyConfig {
        connection_blocklist_ttl_sec: 1,
        proxy_blocklist_ttl_sec: 5,
        // This should never be invoked when set as an error policy
        // as we are not sending requests that error
        error_policy_type: PolicyType::TestPanicOnInvocation,
        channel_capacity: 100,
        ..Default::default()
    };
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_policy_config(Some(policy_config))
        .build()
        .await;
    assert_traffic_control_ok(test_cluster).await
}

#[tokio::test]
async fn test_validator_traffic_control_spam_blocked() -> Result<(), anyhow::Error> {
    let n = 5;
    let policy_config = PolicyConfig {
        connection_blocklist_ttl_sec: 1,
        // Test that any N requests will cause an IP to be added to the blocklist.
        spam_policy_type: PolicyType::TestNConnIP(n - 1),
        channel_capacity: 100,
        ..Default::default()
    };
    let network_config = ConfigBuilder::new_with_temp_dir()
        .with_policy_config(Some(policy_config))
        .build();
    let test_cluster = TestClusterBuilder::new()
        .set_network_config(network_config)
        .build()
        .await;
    assert_traffic_control_spam_blocked(test_cluster, n as usize).await
}

#[tokio::test]
async fn test_fullnode_traffic_control_spam_blocked() -> Result<(), anyhow::Error> {
    let n = 10;
    let policy_config = PolicyConfig {
        connection_blocklist_ttl_sec: 3,
        // Test that any N requests will cause an IP to be added to the blocklist.
        spam_policy_type: PolicyType::TestNConnIP(n - 1),
        channel_capacity: 100,
        ..Default::default()
    };
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_policy_config(Some(policy_config))
        .build()
        .await;
    assert_traffic_control_spam_blocked(test_cluster, n as usize).await
}

#[tokio::test]
async fn test_validator_traffic_control_spam_delegated() -> Result<(), anyhow::Error> {
    let n = 4;
    let policy_config = PolicyConfig {
        connection_blocklist_ttl_sec: 3,
        // Test that any N - 1 requests will cause an IP to be added to the blocklist.
        spam_policy_type: PolicyType::TestNConnIP(n - 1),
        channel_capacity: 100,
        ..Default::default()
    };
    // enable remote firewall delegation
    let firewall_config = RemoteFirewallConfig {
        remote_fw_url: String::from("http://127.0.0.1:65000"),
        delegate_spam_blocking: true,
        delegate_error_blocking: false,
        destination_port: 8080,
    };
    let network_config = ConfigBuilder::new_with_temp_dir()
        .with_policy_config(Some(policy_config))
        .with_firewall_config(firewall_config.clone())
        .build();
    let test_cluster = TestClusterBuilder::new()
        .set_network_config(network_config)
        .build()
        .await;
    assert_traffic_control_spam_delegated(test_cluster, n as usize, 65000).await
}

#[tokio::test]
async fn test_fullnode_traffic_control_spam_delegated() -> Result<(), anyhow::Error> {
    let n = 10;
    let policy_config = PolicyConfig {
        connection_blocklist_ttl_sec: 3,
        // Test that any N - 1 requests will cause an IP to be added to the blocklist.
        spam_policy_type: PolicyType::TestNConnIP(n - 1),
        channel_capacity: 100,
        ..Default::default()
    };
    // enable remote firewall delegation
    let firewall_config = RemoteFirewallConfig {
        remote_fw_url: String::from("http://127.0.0.1:65000"),
        delegate_spam_blocking: true,
        delegate_error_blocking: false,
        destination_port: 9000,
    };
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_policy_config(Some(policy_config))
        .with_fullnode_fw_config(Some(firewall_config.clone()))
        .build()
        .await;
    assert_traffic_control_spam_delegated(test_cluster, n as usize, 65000).await
}

async fn assert_traffic_control_ok(mut test_cluster: TestCluster) -> Result<(), anyhow::Error> {
    let context = &mut test_cluster.wallet;
    let jsonrpc_client = &test_cluster.fullnode_handle.rpc_client;

    let txn_count = 4;
    let mut txns = batch_make_transfer_transactions(context, txn_count).await;
    assert!(
        txns.len() >= txn_count,
        "Expect at least {} txns. Do we generate enough gas objects during genesis?",
        txn_count,
    );

    let txn = txns.swap_remove(0);
    let tx_digest = txn.digest();

    // Test request with ExecuteTransactionRequestType::WaitForLocalExecution
    let (tx_bytes, signatures) = txn.to_tx_bytes_and_signatures();
    let params = rpc_params![
        tx_bytes,
        signatures,
        SuiTransactionBlockResponseOptions::new(),
        ExecuteTransactionRequestType::WaitForLocalExecution
    ];
    let response: SuiTransactionBlockResponse = jsonrpc_client
        .request("sui_executeTransactionBlock", params)
        .await
        .unwrap();

    let SuiTransactionBlockResponse {
        digest,
        confirmed_local_execution,
        ..
    } = response;
    assert_eq!(&digest, tx_digest);
    assert!(confirmed_local_execution.unwrap());

    let _response: SuiTransactionBlockResponse = jsonrpc_client
        .request("sui_getTransactionBlock", rpc_params![*tx_digest])
        .await
        .unwrap();

    // Test request with ExecuteTransactionRequestType::WaitForEffectsCert
    let (tx_bytes, signatures) = txn.to_tx_bytes_and_signatures();
    let params = rpc_params![
        tx_bytes,
        signatures,
        SuiTransactionBlockResponseOptions::new().with_effects(),
        ExecuteTransactionRequestType::WaitForEffectsCert
    ];
    let response: SuiTransactionBlockResponse = jsonrpc_client
        .request("sui_executeTransactionBlock", params)
        .await
        .unwrap();

    let SuiTransactionBlockResponse {
        effects,
        confirmed_local_execution,
        ..
    } = response;
    assert_eq!(effects.unwrap().transaction_digest(), tx_digest);
    assert!(!confirmed_local_execution.unwrap());

    Ok(())
}

async fn assert_traffic_control_spam_blocked(
    mut test_cluster: TestCluster,
    txn_count: usize,
) -> Result<(), anyhow::Error> {
    let context = &mut test_cluster.wallet;
    let jsonrpc_client = &test_cluster.fullnode_handle.rpc_client;

    let mut txns = batch_make_transfer_transactions(context, txn_count).await;
    assert!(
        txns.len() >= txn_count,
        "Expect at least {} txns. Do we generate enough gas objects during genesis?",
        txn_count,
    );

    let txn = txns.swap_remove(0);
    let (tx_bytes, signatures) = txn.to_tx_bytes_and_signatures();
    let params = rpc_params![
        tx_bytes,
        signatures,
        SuiTransactionBlockResponseOptions::new(),
        ExecuteTransactionRequestType::WaitForLocalExecution
    ];

    // it should take no more than 4 requests to be added to the blocklist
    for i in 0..txn_count {
        let response: RpcResult<SuiTransactionBlockResponse> = jsonrpc_client
            .request("sui_executeTransactionBlock", params.clone())
            .await;
        if let Err(err) = response {
            assert!(
                i > 1 || err.to_string().contains("Too many requests"),
                "Error not due to spam policy"
            );
            // TODO: fix error handling such that the error message is not misleading. The
            // full error message currently is the following:
            // For validator blocking:
            //  Transaction execution failed due to issues with transaction inputs, please
            //  review the errors and try again: Too many requests.
            // For fullnode blocking:
            //  Networking or low-level protocol error: Malformed request
            // Once fixed, check that the error message is as expected.
            return Ok(());
        } else {
            response.unwrap();
        }
    }
    panic!("Expected spam policy to trigger within {txn_count} requests");
}

async fn assert_traffic_control_spam_delegated(
    mut test_cluster: TestCluster,
    txn_count: usize,
    listen_port: u16,
) -> Result<(), anyhow::Error> {
    // start test firewall server
    let mut server = NodeFwTestServer::new();
    server.start(listen_port).await;
    // await for the server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let context = &mut test_cluster.wallet;
    let jsonrpc_client = &test_cluster.fullnode_handle.rpc_client;
    let mut txns = batch_make_transfer_transactions(context, txn_count).await;
    assert!(
        txns.len() >= txn_count,
        "Expect at least {} txns. Do we generate enough gas objects during genesis?",
        txn_count,
    );

    let txn = txns.swap_remove(0);
    let (tx_bytes, signatures) = txn.to_tx_bytes_and_signatures();
    let params = rpc_params![
        tx_bytes,
        signatures,
        SuiTransactionBlockResponseOptions::new(),
        ExecuteTransactionRequestType::WaitForLocalExecution
    ];

    // it should take no more than 4 requests to be added to the blocklist
    for _ in 0..txn_count {
        let response: RpcResult<SuiTransactionBlockResponse> = jsonrpc_client
            .request("sui_executeTransactionBlock", params.clone())
            .await;
        assert!(response.is_ok(), "Expected request to succeed");
    }
    let fw_blocklist = server.list_addresses_rpc().await;
    assert!(
        !fw_blocklist.is_empty(),
        "Expected blocklist to be non-empty"
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(6)).await;
    let fw_blocklist = server.list_addresses_rpc().await;
    assert!(
        fw_blocklist.is_empty(),
        "Expected blocklist to now be empty after TTL"
    );
    server.stop().await;
    Ok(())
}
