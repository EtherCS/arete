use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{OBlock, QC, TC};
use bytes::Bytes;
use crypto::{PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use types::{CBlock, CBlockMeta};
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    // Cleanup(Vec<Digest>),
    Cleanup(Vec<CBlockMeta>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    // rx_mempool: Receiver<Digest>,
    rx_cblock: Receiver<CBlock>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<OBlock>,
    // buffer: HashSet<Digest>,
    shard_rounds: HashSet<CBlockMeta>,  // maintain a map recording [execution_shard_id, max_received_round]
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
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
                signature_service,
                rx_cblock,
                rx_message,
                tx_loopback,
                shard_rounds: HashSet::new(),
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

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // Generate a new block.
        let block = OBlock::new(
            qc,
            tc,
            self.name,
            round,
            /* payload */ self.shard_rounds.drain().collect(),
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
        loop {
            tokio::select! {
                // Some(digest) = self.rx_mempool.recv() => {
                //     //if self.buffer.len() < 155 {
                //         self.buffer.insert(digest);
                //     //}
                // },
                
                // ARETE: here, every node receive the transaction (CBlock) due to mempool's broadcast
                // Update its local Map[shard_id, if_receive_CBlock]
                Some(cblock) = self.rx_cblock.recv() => {
                    // TODO: update map
                    debug!("Consensus proposer receive CBlock {:?}", cblock);
                },
                // Receive a proposer message, becoming the leader of this round
                // Check if receiving at least CBlock from every execution shard
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(cbmetas) => {
                        for x in &cbmetas {
                            self.shard_rounds.remove(x);
                        }
                    }
                }
            }
        }
    }
}
