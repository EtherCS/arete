use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{OBlock, QC, TC};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};
use types::{CBlock, CBlockMeta, CrossTransactionVote, ShardInfo, VoteResult};

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    CleanupCBlockMeta(Vec<CBlockMeta>),
    Cleanup(Vec<VoteResult>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    shard_info: ShardInfo,
    signature_service: SignatureService,
    // rx_mempool: Receiver<Digest>,
    rx_cblock: Receiver<CBlock>,
    rx_message: Receiver<ProposerMessage>,
    rx_ctx_vote: Receiver<CrossTransactionVote>,
    tx_loopback: Sender<OBlock>,
    // buffer: HashSet<Digest>,
    shard_cblocks: HashMap<u32, HashSet<CBlockMeta>>, // buffer for CBlocks
    vote_aggregation_trace: HashMap<u64, HashMap<Digest, u8>>, // <consensus_round, vote_results>
    aggregation_results: HashMap<u64, HashMap<Digest, u8>>, // vote results ready for making blocks
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        shard_info: ShardInfo,
        signature_service: SignatureService,
        // rx_mempool: Receiver<Digest>,
        rx_cblock: Receiver<CBlock>,
        rx_message: Receiver<ProposerMessage>,
        rx_ctx_vote: Receiver<CrossTransactionVote>,
        tx_loopback: Sender<OBlock>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                shard_info,
                signature_service,
                rx_cblock,
                rx_message,
                rx_ctx_vote,
                tx_loopback,
                shard_cblocks: HashMap::new(),
                vote_aggregation_trace: HashMap::new(),
                aggregation_results: HashMap::new(),
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn clean_aggregators(&mut self, commit_rounds: Vec<VoteResult>) {
        debug!("Clean {} vote aggregations", commit_rounds.len());
        for vote_result in commit_rounds {
            self.aggregation_results.remove(&vote_result.round);
        }
    }

    async fn aggregate_execution(&mut self, ctx_vote: CrossTransactionVote) {
        let order_round = ctx_vote.order_round;
        if self.vote_aggregation_trace.contains_key(&order_round) {
            // ARETE TODO: support cross-shard execution
            // Current: assume all are successful
            if let Some(update_aggregation) = self.vote_aggregation_trace.get_mut(&order_round) {
                for (_, value) in update_aggregation.iter_mut() {
                    *value = 1;
                }
                // Now it receive all vote, and are ready for commit
                self.aggregation_results
                    .insert(order_round, update_aggregation.clone());
                debug!("ARETE trace: recieve all votes from shard for round {}", order_round);
                self.vote_aggregation_trace.remove(&order_round);
            }
        } else {
            self.vote_aggregation_trace
                .insert(order_round, ctx_vote.votes.clone());
        }
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // Generate a new block.
        let mut merge_cblockmeta = Vec::new();
        for vec_cblockmeta in self.shard_cblocks.values() {
            merge_cblockmeta.extend(vec_cblockmeta.clone());
        }
        // Get vote results that have ready aggregated
        // ARETE TODO: current only consider execution shard 0 and shard 1
        let relevant_shards: Vec<u32> = vec![0, 1];
        let mut temp_aggregators = Vec::new();
        for (temp_round, temp_vote_results) in self.aggregation_results.clone() {
            // debug!("ARETE trace: now have vote results for round {}", temp_round);
            let temp_vote_result =
                VoteResult::new(temp_round, relevant_shards.clone(), temp_vote_results).await;
            temp_aggregators.push(temp_vote_result);
        }
        
        let block = OBlock::new(
            qc,
            tc,
            self.name,
            round,
            /* payload */ merge_cblockmeta,
            temp_aggregators.clone(), // TODO: cross-shard execution
            self.signature_service.clone(),
        )
        .await;

        debug!("ARETE trace: for oblock {:?} add vote results {}", block, temp_aggregators.clone().len());

        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", block, x);
            }
        }
        debug!("Created {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        // Control system: Wait for 2f+1 nodes to acknowledge our block before continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    async fn run(&mut self) {
        debug!(
            "Shard information is: id {}, shard amount {}",
            self.shard_info.id, self.shard_info.number
        );
        loop {
            tokio::select! {
                // ARETE: here, every node receive the transaction (CBlock) due to mempool's broadcast
                // Update its local Map[shard_id, if_receive_CBlock]
                Some(cblock) = self.rx_cblock.recv() => {
                    debug!("Consensus proposer receive CBlock {:?}", cblock);
                    let cblm = CBlockMeta::new(
                        cblock.shard_id,
                        cblock.author,
                        cblock.round,
                        cblock.ebhash,
                        cblock.ctx_hashes,
                    ).await;
                    if self.shard_cblocks.contains_key(&cblock.shard_id) {
                        if let Some(vec_cbmeta) = self.shard_cblocks.get_mut(&cblock.shard_id) {
                            vec_cbmeta.insert(cblm);
                        }
                        else {
                            debug!("Cannot find cbmeta for shard {}", cblock.shard_id);
                        }
                    }
                    else {
                        let mut temp_vec = HashSet::new();
                        temp_vec.insert(cblm);
                        self.shard_cblocks.insert(cblock.shard_id, temp_vec);
                    }
                },
                // Receive a proposer message, becoming the leader of this round
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => {
                        self.make_block(round, qc.clone(), tc.clone()).await;
                    },
                    ProposerMessage::CleanupCBlockMeta(cblock_metas) => {
                        for cblock_meta in &cblock_metas {
                            if let Some(clean_cbmeta) = self.shard_cblocks.get_mut(&cblock_meta.shard_id) {
                                clean_cbmeta.remove(cblock_meta);
                            }
                        }
                        debug!("Clean shard CBlockMeta");
                    },
                    ProposerMessage::Cleanup(_vote_results) => {
                        self.clean_aggregators(_vote_results).await;
                        debug!("Clean shard vote aggregation");
                    }
                },
                Some(ctx_vote) = self.rx_ctx_vote.recv() => {
                    // debug!("ARETE trace: receive vote from shard {} for round {}", ctx_vote.shard_id, ctx_vote.order_round);
                    self.aggregate_execution(ctx_vote).await;
                }
            }
        }
    }
}
