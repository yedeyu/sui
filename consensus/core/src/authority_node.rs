// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::{
    sync::Arc,
    time::{Duration, Instant},
    vec,
};

use async_trait::async_trait;
use bytes::Bytes;
use consensus_config::{AuthorityIndex, Committee, NetworkKeyPair, Parameters, ProtocolKeyPair};
use parking_lot::RwLock;
use prometheus::Registry;
use sui_protocol_config::ProtocolConfig;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::{
    block::{timestamp_utc_ms, BlockAPI, BlockRef, SignedBlock, VerifiedBlock},
    block_manager::BlockManager,
    block_verifier::{BlockVerifier, SignedBlockVerifier},
    broadcaster::Broadcaster,
    commit_observer::CommitObserver,
    context::Context,
    core::{Core, CoreSignals},
    core_thread::{ChannelCoreThreadDispatcher, CoreThreadDispatcher, CoreThreadHandle},
    dag_state::DagState,
    error::{ConsensusError, ConsensusResult},
    leader_timeout::{LeaderTimeoutTask, LeaderTimeoutTaskHandle},
    metrics::initialise_metrics,
    network::{
        anemo_network::AnemoManager, tonic_network::TonicManager, NetworkManager, NetworkService,
    },
    storage::rocksdb_store::RocksDBStore,
    synchronizer::{Synchronizer, SynchronizerHandle},
    transaction::{TransactionClient, TransactionConsumer, TransactionVerifier},
    CommitConsumer, Round,
};

/// ConsensusAuthority is used by Sui to manage the lifetime of AuthorityNode.
/// It hides the details of the implementation from the caller, MysticetiManager.
#[allow(private_interfaces)]
pub enum ConsensusAuthority {
    WithAnemo(AuthorityNode<AnemoManager>),
    WithTonic(AuthorityNode<TonicManager>),
}

// Type of network used by the authority node.
#[derive(Clone, Copy)]
pub enum NetworkType {
    Anemo,
    Tonic,
}

impl ConsensusAuthority {
    pub async fn start(
        network_type: NetworkType,
        own_index: AuthorityIndex,
        committee: Committee,
        parameters: Parameters,
        protocol_config: ProtocolConfig,
        protocol_keypair: ProtocolKeyPair,
        network_keypair: NetworkKeyPair,
        transaction_verifier: Arc<dyn TransactionVerifier>,
        commit_consumer: CommitConsumer,
        registry: Registry,
    ) -> Self {
        match network_type {
            NetworkType::Anemo => {
                let authority = AuthorityNode::start(
                    own_index,
                    committee,
                    parameters,
                    protocol_config,
                    protocol_keypair,
                    network_keypair,
                    transaction_verifier,
                    commit_consumer,
                    registry,
                )
                .await;
                Self::WithAnemo(authority)
            }
            NetworkType::Tonic => {
                let authority = AuthorityNode::start(
                    own_index,
                    committee,
                    parameters,
                    protocol_config,
                    protocol_keypair,
                    network_keypair,
                    transaction_verifier,
                    commit_consumer,
                    registry,
                )
                .await;
                Self::WithTonic(authority)
            }
        }
    }

    pub async fn stop(self) {
        match self {
            Self::WithAnemo(authority) => authority.stop().await,
            Self::WithTonic(authority) => authority.stop().await,
        }
    }

    pub fn transaction_client(&self) -> Arc<TransactionClient> {
        match self {
            Self::WithAnemo(authority) => authority.transaction_client(),
            Self::WithTonic(authority) => authority.transaction_client(),
        }
    }

    #[cfg(test)]
    fn context(&self) -> &Arc<Context> {
        match self {
            Self::WithAnemo(authority) => &authority.context,
            Self::WithTonic(authority) => &authority.context,
        }
    }
}

pub(crate) struct AuthorityNode<N>
where
    N: NetworkManager<AuthorityService<ChannelCoreThreadDispatcher>>,
{
    context: Arc<Context>,
    start_time: Instant,
    transaction_client: Arc<TransactionClient>,
    synchronizer: Arc<SynchronizerHandle>,
    leader_timeout_handle: LeaderTimeoutTaskHandle,
    core_thread_handle: CoreThreadHandle,
    broadcaster: Broadcaster,
    network_manager: N,
}

impl<N> AuthorityNode<N>
where
    N: NetworkManager<AuthorityService<ChannelCoreThreadDispatcher>>,
{
    pub(crate) async fn start(
        own_index: AuthorityIndex,
        committee: Committee,
        parameters: Parameters,
        protocol_config: ProtocolConfig,
        // To avoid accidentally leaking the private key, the protocol key pair should only be
        // kept in Core.
        protocol_keypair: ProtocolKeyPair,
        network_keypair: NetworkKeyPair,
        transaction_verifier: Arc<dyn TransactionVerifier>,
        commit_consumer: CommitConsumer,
        registry: Registry,
    ) -> Self {
        info!(
            "Starting authority {}\n{:#?}\n{:#?}\n{:?}",
            own_index, committee, parameters, protocol_config.version
        );
        assert!(committee.is_valid_index(own_index));
        let context = Arc::new(Context::new(
            own_index,
            committee,
            parameters,
            protocol_config,
            initialise_metrics(registry),
        ));
        let start_time = Instant::now();

        let (tx_client, tx_receiver) = TransactionClient::new(context.clone());
        let tx_consumer = TransactionConsumer::new(tx_receiver, context.clone(), None);

        let (core_signals, signals_receivers) = CoreSignals::new(context.clone());

        let mut network_manager = N::new(context.clone());
        let network_client = network_manager.client();

        // REQUIRED: Broadcaster must be created before Core, to start listen on block broadcasts.
        let broadcaster =
            Broadcaster::new(context.clone(), network_client.clone(), &signals_receivers);

        let store = Arc::new(RocksDBStore::new(&context.parameters.db_path_str_unsafe()));
        let dag_state = Arc::new(RwLock::new(DagState::new(context.clone(), store.clone())));

        let block_verifier = Arc::new(SignedBlockVerifier::new(
            context.clone(),
            transaction_verifier,
        ));

        let block_manager =
            BlockManager::new(context.clone(), dag_state.clone(), block_verifier.clone());

        let commit_observer =
            CommitObserver::new(context.clone(), commit_consumer, dag_state.clone(), store);

        let core = Core::new(
            context.clone(),
            tx_consumer,
            block_manager,
            commit_observer,
            core_signals,
            protocol_keypair,
            dag_state.clone(),
        );

        let (core_dispatcher, core_thread_handle) =
            ChannelCoreThreadDispatcher::start(core, context.clone());
        let core_dispatcher = Arc::new(core_dispatcher);
        let leader_timeout_handle =
            LeaderTimeoutTask::start(core_dispatcher.clone(), &signals_receivers, context.clone());

        let synchronizer = Synchronizer::start(
            network_client,
            context.clone(),
            core_dispatcher.clone(),
            block_verifier.clone(),
        );

        let network_service = Arc::new(AuthorityService {
            context: context.clone(),
            block_verifier,
            core_dispatcher,
            synchronizer: synchronizer.clone(),
            dag_state,
        });
        network_manager
            .install_service(network_keypair, network_service)
            .await;

        Self {
            context,
            start_time,
            transaction_client: Arc::new(tx_client),
            synchronizer,
            leader_timeout_handle,
            core_thread_handle,
            broadcaster,
            network_manager,
        }
    }

    pub(crate) async fn stop(mut self) {
        info!(
            "Stopping authority. Total run time: {:?}",
            self.start_time.elapsed()
        );

        self.network_manager.stop().await;
        self.broadcaster.stop();
        self.core_thread_handle.stop().await;
        self.leader_timeout_handle.stop().await;
        self.synchronizer.stop().await;

        self.context
            .metrics
            .node_metrics
            .uptime
            .observe(self.start_time.elapsed().as_secs_f64());
    }

    pub(crate) fn transaction_client(&self) -> Arc<TransactionClient> {
        self.transaction_client.clone()
    }
}

/// Authority's network interface.
pub(crate) struct AuthorityService<C: CoreThreadDispatcher> {
    context: Arc<Context>,
    block_verifier: Arc<dyn BlockVerifier>,
    core_dispatcher: Arc<C>,
    synchronizer: Arc<SynchronizerHandle>,
    dag_state: Arc<RwLock<DagState>>,
}

#[async_trait]
impl<C: CoreThreadDispatcher> NetworkService for AuthorityService<C> {
    async fn handle_send_block(
        &self,
        peer: AuthorityIndex,
        serialized_block: Bytes,
    ) -> ConsensusResult<()> {
        // TODO: dedup block verifications, here and with fetched blocks.
        let signed_block: SignedBlock =
            bcs::from_bytes(&serialized_block).map_err(ConsensusError::MalformedBlock)?;

        // Reject blocks not produced by the peer.
        if peer != signed_block.author() {
            self.context
                .metrics
                .node_metrics
                .invalid_blocks
                .with_label_values(&[&peer.to_string(), "send_block"])
                .inc();
            let e = ConsensusError::UnexpectedAuthority(signed_block.author(), peer);
            info!("Block with wrong authority from {}: {}", peer, e);
            return Err(e);
        }

        // Reject blocks failing validations.
        if let Err(e) = self.block_verifier.verify(&signed_block) {
            self.context
                .metrics
                .node_metrics
                .invalid_blocks
                .with_label_values(&[&peer.to_string(), "send_block"])
                .inc();
            info!("Invalid block from {}: {}", peer, e);
            return Err(e);
        }
        let verified_block = VerifiedBlock::new_verified(signed_block, serialized_block);

        // Reject block with timestamp too far in the future.
        let forward_time_drift = Duration::from_millis(
            verified_block
                .timestamp_ms()
                .saturating_sub(timestamp_utc_ms()),
        );
        if forward_time_drift > self.context.parameters.max_forward_time_drift {
            return Err(ConsensusError::BlockTooFarInFuture {
                block_timestamp: verified_block.timestamp_ms(),
                forward_time_drift,
            });
        }

        // Wait until the block's timestamp is current.
        if forward_time_drift > Duration::ZERO {
            self.context
                .metrics
                .node_metrics
                .block_timestamp_drift_wait_ms
                .with_label_values(&[&peer.to_string()])
                .inc_by(forward_time_drift.as_millis() as u64);
            sleep(forward_time_drift).await;
        }

        let missing_ancestors = self
            .core_dispatcher
            .add_blocks(vec![verified_block])
            .await
            .map_err(|_| ConsensusError::Shutdown)?;

        if !missing_ancestors.is_empty() {
            // schedule the fetching of them from this peer
            if let Err(err) = self
                .synchronizer
                .fetch_blocks(missing_ancestors, peer)
                .await
            {
                warn!("Errored while trying to fetch missing ancestors via synchronizer: {err}");
            }
        }

        Ok(())
    }

    async fn handle_fetch_blocks(
        &self,
        peer: AuthorityIndex,
        block_refs: Vec<BlockRef>,
        highest_accepted_rounds: Vec<Round>,
    ) -> ConsensusResult<Vec<Bytes>> {
        const MAX_ALLOWED_FETCH_BLOCKS: usize = 200;

        if block_refs.len() > MAX_ALLOWED_FETCH_BLOCKS {
            return Err(ConsensusError::TooManyFetchBlocksRequested(peer));
        }

        // Some quick validation of the requested block refs
        for block in &block_refs {
            if !self.context.committee.is_valid_index(block.author) {
                return Err(ConsensusError::InvalidAuthorityIndex {
                    index: block.author,
                    max: self.context.committee.size(),
                });
            }
            if block.round == 0 {
                return Err(ConsensusError::UnexpectedGenesisBlockRequested);
            }
        }

        // For now ask dag state directly
        let mut blocks = self.dag_state.read().get_blocks(&block_refs);

        // Get the block parents
        let all_ancestors = blocks
            .iter()
            .flatten()
            .flat_map(|block| block.ancestors().to_vec())
            .collect::<BTreeSet<BlockRef>>();

        // Now check if an ancestor's round is higher than the one that the peer has. If yes, then serve
        // that ancestor block as well
        let all_ancestors = all_ancestors
            .into_iter()
            .filter(|block_ref| highest_accepted_rounds[block_ref.author] < block_ref.round)
            .collect::<Vec<_>>();

        if !all_ancestors.is_empty() {
            let additional_blocks = self.dag_state.read().get_blocks(&all_ancestors);
            blocks.extend(additional_blocks);
        }

        // Return the serialised blocks
        let result = blocks
            .into_iter()
            .flatten()
            .map(|block| block.serialized().clone())
            .collect::<Vec<_>>();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use async_trait::async_trait;
    use consensus_config::{local_committee_and_keys, Parameters};
    use parking_lot::Mutex;
    use prometheus::Registry;
    use rstest::rstest;
    use sui_protocol_config::ProtocolConfig;
    use tempfile::TempDir;
    use tokio::{sync::mpsc::unbounded_channel, time::sleep};

    use super::*;
    use crate::{
        authority_node::AuthorityService,
        block::{timestamp_utc_ms, BlockRef, Round, TestBlock, VerifiedBlock},
        block_verifier::NoopBlockVerifier,
        context::Context,
        core_thread::{CoreError, CoreThreadDispatcher},
        network::NetworkClient,
        storage::mem_store::MemStore,
        transaction::NoopTransactionVerifier,
    };

    struct FakeCoreThreadDispatcher {
        blocks: Mutex<Vec<VerifiedBlock>>,
    }

    impl FakeCoreThreadDispatcher {
        fn new() -> Self {
            Self {
                blocks: Mutex::new(vec![]),
            }
        }

        fn get_blocks(&self) -> Vec<VerifiedBlock> {
            self.blocks.lock().clone()
        }
    }

    #[async_trait]
    impl CoreThreadDispatcher for FakeCoreThreadDispatcher {
        async fn add_blocks(
            &self,
            blocks: Vec<VerifiedBlock>,
        ) -> Result<BTreeSet<BlockRef>, CoreError> {
            let block_refs = blocks.iter().map(|b| b.reference()).collect();
            self.blocks.lock().extend(blocks);
            Ok(block_refs)
        }

        async fn force_new_block(&self, _round: Round) -> Result<(), CoreError> {
            unimplemented!()
        }

        async fn get_missing_blocks(&self) -> Result<BTreeSet<BlockRef>, CoreError> {
            unimplemented!()
        }

        async fn get_highest_accepted_rounds(&self) -> Result<Vec<Round>, CoreError> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct FakeNetworkClient {}

    #[async_trait]
    impl NetworkClient for FakeNetworkClient {
        async fn send_block(
            &self,
            _peer: AuthorityIndex,
            _block: &VerifiedBlock,
            _timeout: Duration,
        ) -> ConsensusResult<()> {
            unimplemented!("Unimplemented")
        }

        async fn fetch_blocks(
            &self,
            _peer: AuthorityIndex,
            _block_refs: Vec<BlockRef>,
            _highest_accepted_rounds: Vec<Round>,
            _timeout: Duration,
        ) -> ConsensusResult<Vec<Bytes>> {
            unimplemented!("Unimplemented")
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_authority_start_and_stop(
        #[values(NetworkType::Anemo, NetworkType::Tonic)] network_type: NetworkType,
    ) {
        let (committee, keypairs) = local_committee_and_keys(0, vec![1]);
        let registry = Registry::new();

        let temp_dir = TempDir::new().unwrap();
        let parameters = Parameters {
            db_path: Some(temp_dir.into_path()),
            ..Default::default()
        };
        let txn_verifier = NoopTransactionVerifier {};

        let own_index = committee.to_authority_index(0).unwrap();
        let protocol_keypair = keypairs[own_index].1.clone();
        let network_keypair = keypairs[own_index].0.clone();

        let (sender, _receiver) = unbounded_channel();
        let commit_consumer = CommitConsumer::new(sender, 0, 0);

        let authority = ConsensusAuthority::start(
            network_type,
            own_index,
            committee,
            parameters,
            ProtocolConfig::get_for_max_version_UNSAFE(),
            protocol_keypair,
            network_keypair,
            Arc::new(txn_verifier),
            commit_consumer,
            registry,
        )
        .await;

        assert_eq!(authority.context().own_index, own_index);
        assert_eq!(authority.context().committee.epoch(), 0);
        assert_eq!(authority.context().committee.size(), 1);

        authority.stop().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_authority_service() {
        let (context, _keys) = Context::new_for_test(4);
        let context = Arc::new(context);
        let block_verifier = Arc::new(NoopBlockVerifier {});
        let core_dispatcher = Arc::new(FakeCoreThreadDispatcher::new());
        let network_client = Arc::new(FakeNetworkClient::default());
        let store = Arc::new(MemStore::new());
        let dag_state = Arc::new(RwLock::new(DagState::new(context.clone(), store)));
        let synchronizer = Synchronizer::start(
            network_client,
            context.clone(),
            core_dispatcher.clone(),
            block_verifier.clone(),
        );
        let authority_service = Arc::new(AuthorityService {
            context: context.clone(),
            block_verifier,
            core_dispatcher: core_dispatcher.clone(),
            synchronizer,
            dag_state,
        });

        // Test delaying blocks with time drift.
        let now = timestamp_utc_ms();
        let max_drift = context.parameters.max_forward_time_drift;
        let input_block = VerifiedBlock::new_for_test(
            TestBlock::new(9, 0)
                .set_timestamp_ms(now + max_drift.as_millis() as u64)
                .build(),
        );

        let service = authority_service.clone();
        let serialized = input_block.serialized().clone();
        tokio::spawn(async move {
            service
                .handle_send_block(context.committee.to_authority_index(0).unwrap(), serialized)
                .await
                .unwrap();
        });

        sleep(max_drift / 2).await;
        assert!(core_dispatcher.get_blocks().is_empty());

        sleep(max_drift).await;
        let blocks = core_dispatcher.get_blocks();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0], input_block);
    }

    // TODO: build AuthorityFixture.
    #[rstest]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_authority_committee(
        #[values(NetworkType::Anemo, NetworkType::Tonic)] network_type: NetworkType,
    ) {
        let (committee, keypairs) = local_committee_and_keys(0, vec![1, 1, 1, 1]);
        let mut output_receivers = vec![];
        let mut authorities = vec![];
        for (index, _authority_info) in committee.authorities() {
            let registry = Registry::new();

            let temp_dir = TempDir::new().unwrap();
            let parameters = Parameters {
                db_path: Some(temp_dir.into_path()),
                ..Default::default()
            };
            let txn_verifier = NoopTransactionVerifier {};

            let protocol_keypair = keypairs[index].1.clone();
            let network_keypair = keypairs[index].0.clone();

            let (sender, receiver) = unbounded_channel();
            let commit_consumer = CommitConsumer::new(sender, 0, 0);
            output_receivers.push(receiver);

            let authority = ConsensusAuthority::start(
                network_type,
                index,
                committee.clone(),
                parameters,
                ProtocolConfig::get_for_max_version_UNSAFE(),
                protocol_keypair,
                network_keypair,
                Arc::new(txn_verifier),
                commit_consumer,
                registry,
            )
            .await;
            authorities.push(authority);
        }

        const NUM_TRANSACTIONS: u8 = 15;
        let mut submitted_transactions = BTreeSet::<Vec<u8>>::new();
        for i in 0..NUM_TRANSACTIONS {
            let txn = vec![i; 16];
            submitted_transactions.insert(txn.clone());
            authorities[i as usize % authorities.len()]
                .transaction_client()
                .submit(txn)
                .await
                .unwrap();
        }

        for mut receiver in output_receivers {
            let mut expected_transactions = submitted_transactions.clone();
            loop {
                let committed_subdag =
                    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                        .await
                        .unwrap()
                        .unwrap();
                for b in committed_subdag.blocks {
                    for txn in b.transactions().iter().map(|t| t.data().to_vec()) {
                        assert!(
                            expected_transactions.remove(&txn),
                            "Transaction not submitted or already seen: {:?}",
                            txn
                        );
                    }
                }
                if expected_transactions.is_empty() {
                    break;
                }
            }
        }

        for authority in authorities {
            authority.stop().await;
        }
    }
}
