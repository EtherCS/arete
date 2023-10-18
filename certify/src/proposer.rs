// use crate::config::{ExecutionCommittee, Stake};
// use crate::consensus::{ConsensusMessage, Round};
// use crate::messages::{QC, TC};
// use bytes::Bytes;
// use crypto::{Digest, PublicKey, SignatureService};
// use futures::stream::futures_unordered::FuturesUnordered;
// use futures::stream::StreamExt as _;
// use log::{debug, info};
// use network::{CancelHandler, ReliableSender};
// use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};
use types::{CBlock, CertifyMessage};

// #[derive(Debug)]
// pub enum ProposerMessage {
//     Make(Round, QC, Option<TC>),
//     Cleanup(Vec<Digest>),
// }

/// Proposer module: forward a certified CBlock to the ordering shard
pub struct Proposer {
    // name: PublicKey,
    // committee: ExecutionCommittee,
    // shard_info: ShardInfo,
    // signature_service: SignatureService,
    rx_mempool: Receiver<CBlock>, // receive certificate block from execpool
    tx_certify: Sender<CertifyMessage>, // send the received certificate block to executor.analyze_block()
                                        // rx_message: Receiver<ProposerMessage>,
                                        // tx_loopback: Sender<EBlock>,
                                        // buffer: HashSet<Digest>,
                                        // network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        // name: PublicKey,
        // committee: ExecutionCommittee,
        // shard_info: ShardInfo,
        // signature_service: SignatureService,
        rx_mempool: Receiver<CBlock>,
        tx_certify: Sender<CertifyMessage>,
        // rx_message: Receiver<ProposerMessage>,
        // tx_loopback: Sender<EBlock>,
    ) {
        tokio::spawn(async move {
            Self {
                // name,
                // committee,
                // shard_info,
                // signature_service,
                rx_mempool,
                // rx_message,
                // tx_loopback,
                // buffer: HashSet::new(),
                // network: ReliableSender::new(),
                tx_certify,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    // async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
    //     let _ = wait_for.await;
    //     deliver
    // }

    // async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
    //     info!("The number of transaction hash in a block is {}", self.buffer.capacity());
    //     // Generate a new block.
    //     let block = EBlock::new(
    //         self.shard_info.id,
    //         qc,
    //         tc,
    //         self.name,
    //         round,
    //         /* payload */ self.buffer.drain().collect(),
    //         self.signature_service.clone(),
    //     )
    //     .await;

    //     if !block.payload.is_empty() {
    //         info!("Created {}", block);

    //         #[cfg(feature = "benchmark")]
    //         for x in &block.payload {
    //             // NOTE: This log entry is used to compute performance.
    //             info!("Shard {} Created {} -> {:?}", self.shard_info.id, block, x);
    //         }
    //     }
    //     debug!("Created {:?}", block);

    //     // Broadcast our new block.
    //     debug!("Broadcasting {:?}", block);
    //     let (names, addresses): (Vec<_>, _) = self
    //         .committee
    //         .broadcast_addresses(&self.name)
    //         .iter()
    //         .cloned()
    //         .unzip();
    //     let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
    //         .expect("Failed to serialize block");
    //     let handles = self
    //         .network
    //         .broadcast(addresses, Bytes::from(message))
    //         .await;

    //     // Send our block to the core for processing.
    //     self.tx_loopback
    //         .send(block)
    //         .await
    //         .expect("Failed to send block");

    //     // Control system: Wait for f+1 nodes to acknowledge our block before continuing.
    //     let mut wait_for_quorum: FuturesUnordered<_> = names
    //         .into_iter()
    //         .zip(handles.into_iter())
    //         .map(|(name, handler)| {
    //             let stake = self.committee.stake(&name);
    //             Self::waiter(handler, stake)
    //         })
    //         .collect();

    //     let mut total_stake = self.committee.stake(&self.name);
    //     while let Some(stake) = wait_for_quorum.next().await {
    //         total_stake += stake;
    //         if total_stake >= self.committee.quorum_threshold() {
    //             break;
    //         }
    //     }
    // }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(cblock) = self.rx_mempool.recv() => {
                    //if self.buffer.len() < 155 {
                        // self.buffer.insert(digest);
                    //}
                    self.tx_certify.send(CertifyMessage::CBlock(cblock.clone())).await.expect("Failed send cblock");
                    // debug!("Send a certificate block {:?} to the ordering shard", cblock);
                },
                // I am the leader now
                // Some(message) = self.rx_message.recv() => match message {
                //     ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                //     ProposerMessage::Cleanup(digests) => {
                //         for x in &digests {
                //             self.buffer.remove(x);
                //         }
                //     }
                // }
            }
        }
    }
}
