use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{OBlock, QC, TC};
use bytes::Bytes;
use crypto::{Hash, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use types::{CBlock, CBlockMeta, ShardInfo};
use std::convert::TryFrom;

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    // Cleanup(Vec<Digest>),
    Cleanup(Vec<CBlockMeta>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    shard_info: ShardInfo,
    signature_service: SignatureService,
    // rx_mempool: Receiver<Digest>,
    rx_cblock: Receiver<CBlock>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<OBlock>,
    // buffer: HashSet<Digest>,
    shard_rounds: HashMap<u32, CBlockMeta>, // maintain a map recording [execution_shard_id, max_received_round]
    max_shard_rounds: HashMap<u32, u64>,    // use it to avoid receiving old CBlocks
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
                tx_loopback,
                shard_rounds: HashMap::new(),
                max_shard_rounds: HashMap::new(),
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

    async fn update_map(&mut self, payload: Vec<CBlockMeta>) {
        for cbmeta in payload {
            if let Some(max_round) = self.max_shard_rounds.get(&cbmeta.shard_id).copied() {
                if max_round < cbmeta.round {
                    self.max_shard_rounds.insert(cbmeta.shard_id, cbmeta.round);
                }
            }
            if let Some(s_round) = self.shard_rounds.get(&cbmeta.shard_id) {
                if s_round.round <= cbmeta.round {
                    self.shard_rounds.remove(&cbmeta.shard_id);
                }
            }
        }
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        let shard_num = usize::try_from(self.shard_info.number).unwrap();
        loop {
            // wait
            if self.max_shard_rounds.len() <= shard_num {
                break;
            }
        }
        // Generate a new block.
        let block = OBlock::new(
            qc,
            tc,
            self.name,
            round,
            /* payload */ self.shard_rounds.values().cloned().collect(),
            HashMap::new(), // TODO: cross-shard execution
            self.signature_service.clone(),
        )
        .await;

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
        // debug!("Broadcasting {:?}", block);
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
                    // debug!("Consensus proposer receive CBlock {:?}", cblock);
                    if !self.max_shard_rounds.contains_key(&cblock.shard_id) {
                        // debug!("Receive the first or a larger CBlock from shard {}, new round {}", cblock.shard_id, cblock.round);
                        let cblm = CBlockMeta::new(
                            cblock.shard_id,
                            cblock.round,
                            cblock.digest(),
                        ).await;
                        self.shard_rounds.insert(cblock.shard_id, cblm);
                        self.max_shard_rounds.insert(cblock.shard_id, cblock.round);
                    }
                    else if let Some(tp_round) = self.max_shard_rounds.get(&cblock.shard_id).copied() {
                        if tp_round < cblock.round {
                            // debug!("Receive the first or a larger CBlock from shard {}, new round {}", cblock.shard_id, cblock.round);
                            let cblm = CBlockMeta::new(
                                cblock.shard_id,
                                cblock.round,
                                cblock.digest(),
                            ).await;
                            self.shard_rounds.insert(cblock.shard_id, cblm);
                            self.max_shard_rounds.insert(cblock.shard_id, cblock.round);
                        }
                    }
                },
                // Receive a proposer message, becoming the leader of this round
                // Check if receiving at least CBlock from every execution shard
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => {
                        self.make_block(round, qc.clone(), tc.clone()).await;
                    },
                    ProposerMessage::Cleanup(_cbmetas) => {
                        self.update_map(_cbmetas).await;
                        // debug!("Clean shard CBlockMeta");
                    }
                }
            }
        }
    }
}
