// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, sync::Arc, time::Duration};

use consensus_config::AuthorityIndex;
use futures::{stream::FuturesOrdered, StreamExt as _};
use parking_lot::{Mutex, RwLock};
use tokio::{
    task::JoinSet,
    time::{Interval, MissedTickBehavior},
};

use crate::{
    block::{Round, VerifiedBlock},
    commit::{CommitRef, TrustedCommit},
    context::Context,
    dag_state::DagState, error::ConsensusError,
};

pub(crate) struct CommitSyncer {
    context: Arc<Context>,
    dag_state: Arc<RwLock<DagState>>,
    inner: Arc<Mutex<Inner>>,
    tasks: Arc<Mutex<JoinSet<()>>>,
}

struct Inner {
    highest_commit_vote_rounds: Vec<Round>,
    peer_highest_committed_round: Round,
    local_highest_committed_round: Round,
    pending_fetch_commits: VecDeque<PendingFetchCommits>,
}

struct PendingFetchCommits {
    start: Round,
    end: Round,
    handle: tokio::task::JoinHandle<()>,
}

impl CommitSyncer {
    pub(crate) fn new(context: Arc<Context>, dag_state: Arc<RwLock<DagState>>) -> Self {
        let highest_commit_vote_rounds = vec![0; context.committee.size()];
        CommitSyncer {
            context,
            dag_state,
            inner: Arc::new(Mutex::new(Inner {
                highest_commit_vote_rounds,
                peer_highest_committed_round: 0,
                local_highest_committed_round: 0,
                pending_fetch_commits: VecDeque::new(),
            })),
            tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    pub(crate) fn observe_commit_votes(&self, peer: AuthorityIndex, commit_votes: &[CommitRef]) {
        for vote in commit_votes {
            let round = vote.round;
            let mut inner = self.inner.lock();
            inner.highest_commit_vote_rounds[peer] =
                inner.highest_commit_vote_rounds[peer].max(round);
        }
    }

    fn start(&self) {
        // let requests = FuturesOrdered::new();
    }

    async fn fetch_commit_loop(
        context: Arc<Context>,
        dag_state: Arc<RwLock<DagState>>,
        inner: Arc<Mutex<Inner>>,
        tx_certified_commits: tokio::sync::mpsc::Sender<TrustedCommit>,
    ) {
        const COMMIT_LAG_THRESHOLD: Round = 10;

        let mut pending_fetch_commits_tasks = FuturesOrdered::new();

        // let requests = VecDeque::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut inner = inner.lock();
                    let highest_commit_vote_rounds = inner.highest_commit_vote_rounds.clone();
                    let mut highest_commit_vote_rounds = context.committee.authorities().zip(highest_commit_vote_rounds.into_iter()).map(|(a, r)| (a.0, a.1.stake, r)).collect::<Vec<_>>();
                    highest_commit_vote_rounds.sort_by(|a, b| a.1.cmp(&b.1).reverse());
                    let mut total_stake = 0;
                    for (_, stake, round) in highest_commit_vote_rounds {
                        total_stake += stake;
                        if total_stake > context.committee.validity_threshold() {
                            inner.peer_highest_committed_round = round;
                            break;
                        }
                    }
                    inner.local_highest_committed_round = dag_state.read().last_commit_round();
                }
                result = pending_fetch_commits_tasks.next(), if !pending_fetch_commits_tasks.is_empty() => {
                    let mut inner = inner.lock();
                    let pending_fetch_commits = inner.pending_fetch_commits.pop_front().unwrap();
                    // if let Some(result) = result {
                    //     let (start, end, handle) = result;
                    //     if let Err(e) = handle.await {
                    //         log::warn!("Failed to fetch commits from {} to {}: {}", start, end, e);
                    //     }
                    // }
                }
            }

            pending_fetch_commits_tasks.push_(tokio::spawn(async move {
                Err(ConsensusError::UnexpectedGenesisBlock)
            }));

            // let commit_round_lower_bound = std::cmp::max(dag_state.read().last_commit_round(), pending_fetch_commits_tasks.back().map(|t| t.end).unwrap_or(0));
            // if commit_round_lower_bound + COMMIT_LAG_THRESHOLD < peer_highest_committed_round {
            // }
        }
    }
}
