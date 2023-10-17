use std::time::Duration;

use crate::config::{ExecutionCommittee, Stake};
use crate::processor::SerializedEBlockMessage;
use crypto::{Digest, Hash, PublicKey, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use std::convert::TryInto;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::sleep,
};
use types::{CBlock, EBlock, NodeSignature, ShardInfo};

// #[cfg(test)]
// #[path = "tests/quorum_waiter_tests.rs"]
// pub mod quorum_waiter_tests;

/// Extra batch dissemination time for the f last nodes (in ms).
const DISSEMINATION_DEADLINE: u64 = 500;
/// Bounds the queue handling the extra dissemination.
const DISSEMINATION_QUEUE_MAX: usize = 10_000;

#[derive(Debug)]
pub struct EBlockRespondMessage {
    /// The (publcKey, signagure) of the sender on the EBlock.
    pub certificate: NodeSignature,
    /// The stake of the sender.
    pub stake: Stake,
}

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `MempoolMessage::Batch` message.
    pub batch: SerializedEBlockMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The node
    name: PublicKey,
    /// The execution shard information
    shard_info: ShardInfo,
    /// The signature service
    signature_service: SignatureService,
    /// The committee information.
    committee: ExecutionCommittee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Send EBlock to processor for storing
    tx_processor: Sender<EBlock>,
    /// Channel to deliver CBlock for which we have enough acknowledgements.
    tx_cblock: Sender<CBlock>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        name: PublicKey,
        shard_info: ShardInfo,
        signature_service: SignatureService,
        committee: ExecutionCommittee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_processor: Sender<EBlock>,
        tx_cblock: Sender<CBlock>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                shard_info,
                signature_service,
                committee,
                stake,
                rx_message,
                tx_processor,
                tx_cblock,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> EBlockRespondMessage {
        let byte_certificate = wait_for.await.expect("error handler");
        let certificate: NodeSignature =
            bincode::deserialize(&byte_certificate).expect("fail to deserialize certificate");
        EBlockRespondMessage {
            certificate: certificate,
            stake: deliver,
        }
    }

    // async fn empty_buffer()

    /// Main loop.
    async fn run(&mut self) {
        // Hold the dissemination handlers of the f slower nodes.
        let mut pending = FuturesUnordered::new();
        let mut pending_counter = 0;

        loop {
            tokio::select! {
                Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv() => {
                    let eblock: EBlock = bincode::deserialize(&batch).expect("fail to deserialize eblock");
                    // Send this EBlock to processor for storing
                    self.tx_processor.send(eblock.clone()).await.expect("Failed to send EBlock to processor");
                    // Collect signatures to create CBlock
                    let mut hash_ctxs: Vec<Digest> = Vec::new();
                    for ctx in &eblock.crosstxs {
                        hash_ctxs.push(Digest(Sha512::digest(&ctx).as_slice()[..32].try_into().unwrap()));
                    }
                    let mut wait_for_quorum: FuturesUnordered<_> = handlers
                        .into_iter()
                        .map(|(name, handler)| {
                            let stake = self.committee.stake(&name);
                            Self::waiter(handler, stake)
                        })
                        .collect();

                    // Wait for the first (1-f_L) nodes to send back an Ack. Then we consider the batch
                    // delivered and we send its digest to the consensus (that will include it into
                    // the dag). This should reduce the amount of synching.
                    let my_cblock = CBlock::new(
                        eblock.shard_id,
                        eblock.author,
                        eblock.round,
                        eblock.digest(),
                        hash_ctxs.clone(),
                        Vec::new(),
                    ).await;
                    // create my signature
                    let mut signature_service = self.signature_service.clone();
                    let signature = signature_service.request_signature(my_cblock.digest()).await;
                    let node_signature = NodeSignature::new(self.name, signature).await;
                    let mut total_stake = self.stake;
                    let mut multisignatures: Vec<NodeSignature> = Vec::new();
                    multisignatures.push(node_signature);
                    while let Some(EBlockRespondMessage { certificate, stake }) = wait_for_quorum.next().await {
                        multisignatures.push(certificate);
                        total_stake += stake;
                        if total_stake >= self.committee.quorum_threshold() {
                            let cblock = CBlock::new(
                                self.shard_info.id,
                                self.name,
                                0,
                                eblock.digest(),
                                hash_ctxs.clone(),
                                multisignatures.clone(),
                            ).await;
                            self.tx_cblock
                                .send(cblock)
                                .await
                                .expect("Failed to deliver cblock");
                            break;
                        }
                    }

                    // Give a bit of extra time to disseminate the batch to slower nodes rather than
                    // immediately dropping the handles.
                    // TODO: We should allocate resource per peer (not in total).
                    if pending_counter >= DISSEMINATION_QUEUE_MAX {
                        pending.push(async move {
                            tokio::select! {
                                _ = async move {while let Some(_) = wait_for_quorum.next().await {}} => (),
                                () = sleep(Duration::from_millis(DISSEMINATION_DEADLINE)) => ()
                            }
                        });
                        pending_counter += 1;
                    }
                },
                Some(_) = pending.next() =>  {
                    if pending_counter > 0 {
                        pending_counter -= 1;
                    }
                }
            }
        }
    }
}
