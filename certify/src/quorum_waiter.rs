use std::time::Duration;

use crate::config::{ExecutionCommittee, Stake};
use crypto::{Hash, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::sleep,
};
use types::{CertifyMessage, CrossTransactionVote, NodeSignature};

/// Extra batch dissemination time for the f last nodes (in ms).
const DISSEMINATION_DEADLINE: u64 = 500;
/// Bounds the queue handling the extra dissemination.
const DISSEMINATION_QUEUE_MAX: usize = 10_000;

pub type SerializedVoteMessage = Vec<u8>;

#[derive(Debug)]
pub struct VoteRespondMessage {
    /// The (publcKey, signagure) of the sender on the vote results.
    pub certificate: NodeSignature,
    /// The stake of the sender.
    pub stake: Stake,
}

#[derive(Debug)]
pub struct QuorumVoteMessage {
    /// A serialized `MempoolMessage::Batch` message.
    pub vote: SerializedVoteMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The node
    name: PublicKey,
    /// The signature service
    signature_service: SignatureService,
    /// The committee information.
    committee: ExecutionCommittee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumVoteMessage>,
    /// Channel to deliver vote results to the ordering shard for which we have enough acknowledgements.
    tx_certify: Sender<CertifyMessage>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        name: PublicKey,
        signature_service: SignatureService,
        committee: ExecutionCommittee,
        stake: Stake,
        rx_message: Receiver<QuorumVoteMessage>,
        tx_certify: Sender<CertifyMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                committee,
                stake,
                rx_message,
                tx_certify,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> VoteRespondMessage {
        let byte_certificate = wait_for.await.expect("error handler");
        let certificate: NodeSignature =
            bincode::deserialize(&byte_certificate).expect("fail to deserialize certificate");
        VoteRespondMessage {
            certificate: certificate,
            stake: deliver,
        }
    }

    /// Main loop.
    async fn run(&mut self) {
        // Hold the dissemination handlers of the f slower nodes.
        let mut pending = FuturesUnordered::new();
        let mut pending_counter = 0;

        loop {
            tokio::select! {
                Some(QuorumVoteMessage { vote, handlers }) = self.rx_message.recv() => {
                    let ctx_vote: CrossTransactionVote = bincode::deserialize(&vote).expect("fail to deserialize vote result");
                    // Collect signatures to create the vote results
                    let mut wait_for_quorum: FuturesUnordered<_> = handlers
                        .into_iter()
                        .map(|(name, handler)| {
                            let stake = self.committee.stake(&name);
                            Self::waiter(handler, stake)
                        })
                        .collect();
                    // Create my signature
                    let mut signature_service = self.signature_service.clone();
                    let signature = signature_service.request_signature(ctx_vote.digest()).await;
                    let node_signature = NodeSignature::new(self.name, signature).await;
                    // Wait for the first (1-f_L) nodes to send back a Signature. Then we consider the vote results
                    // certified and we send it to the ordering shard
                    let mut total_stake = self.stake;
                    let mut multisignatures: Vec<NodeSignature> = Vec::new();
                    multisignatures.push(node_signature);
                    while let Some(VoteRespondMessage { certificate, stake }) = wait_for_quorum.next().await {
                        multisignatures.push(certificate);
                        total_stake += stake;
                        if total_stake >= self.committee.quorum_threshold() {
                            let certified_vote = CrossTransactionVote::new(
                                ctx_vote.shard_id,
                                ctx_vote.order_round,
                                ctx_vote.votes.clone(),
                                multisignatures.clone(),
                            ).await;
                            self.tx_certify
                                .send(CertifyMessage::CtxVote(certified_vote))
                                .await
                                .expect("Failed to deliver certified vote results");
                            break;
                        }
                    }

                    // Give a bit of extra time to disseminate the batch to slower nodes rather than
                    // immediately dropping the handles.
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
