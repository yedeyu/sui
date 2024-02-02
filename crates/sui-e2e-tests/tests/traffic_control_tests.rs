// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! NB: Tests in this module expect real network connections and interactions, thus they
//! should all be tokio::test rather than simtest. Any deviation from this should be well
//! understood and justified.

use jsonrpsee::{core::client::ClientT, rpc_params};
use sui_json_rpc_types::{
    SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse, SuiTransactionBlockResponseOptions,
};
use sui_keys::keystore::AccountKeystore;
use sui_swarm_config::network_config_builder::ConfigBuilder;
use sui_test_transaction_builder::batch_make_transfer_transactions;
use sui_types::{
    crypto::{get_key_pair, SuiKeyPair},
    quorum_driver_types::ExecuteTransactionRequestType,
    traffic_control::{PolicyConfig, PolicyType},
};
use test_cluster::TestClusterBuilder;

#[tokio::test]
async fn test_traffic_control_execute_tx_with_serialized_signature() -> Result<(), anyhow::Error> {
    let policy_config = PolicyConfig {
        // TODO: Add some error codes
        tallyable_error_codes: vec![],
        remote_blocklist_ttl_sec: 1,
        end_user_blocklist_ttl_sec: 5,
        spam_policy_type: PolicyType::SimpleErrorTest,
        error_policy_type: PolicyType::SimpleErrorTest,
        channel_capacity: 100,
    };
    let network_config = ConfigBuilder::new_with_temp_dir()
        .with_traffic_control_config(Some(policy_config))
        .build();
    let mut test_cluster = TestClusterBuilder::new()
        .set_network_config(network_config)
        .build()
        .await;

    let context = &mut test_cluster.wallet;
    context
        .config
        .keystore
        .add_key(None, SuiKeyPair::Secp256k1(get_key_pair().1))?;
    context
        .config
        .keystore
        .add_key(None, SuiKeyPair::Ed25519(get_key_pair().1))?;

    let jsonrpc_client = &test_cluster.fullnode_handle.rpc_client;

    let txn_count = 4;
    let txns = batch_make_transfer_transactions(context, txn_count).await;
    for txn in txns {
        let tx_digest = txn.digest();
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
        assert_eq!(digest, *tx_digest);
        assert!(confirmed_local_execution.unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn test_traffic_control_full_node_transaction_orchestrator_rpc_ok(
) -> Result<(), anyhow::Error> {
    let policy_config = PolicyConfig {
        // TODO: Add some error codes
        tallyable_error_codes: vec![],
        remote_blocklist_ttl_sec: 1,
        end_user_blocklist_ttl_sec: 5,
        spam_policy_type: PolicyType::SimpleErrorTest,
        error_policy_type: PolicyType::SimpleErrorTest,
        channel_capacity: 100,
    };
    let network_config = ConfigBuilder::new_with_temp_dir()
        .with_traffic_control_config(Some(policy_config))
        .build();
    let mut test_cluster = TestClusterBuilder::new()
        .set_network_config(network_config)
        .build()
        .await;

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
