use crate::quorum_waiter::QuorumVoteMessage;
use crate::{config::ExecutionCommittee, consensus::ConsensusMessage};
use bytes::Bytes;
use crypto::PublicKey;
use network::ReliableSender;
use tokio::sync::mpsc::{Receiver, Sender};
use types::CrossTransactionVote;

/// Assemble clients transactions into batches.
pub struct VoteMaker {
    /// The node
    name: PublicKey,
    /// The committee information.
    committee: ExecutionCommittee,
    /// Channel receive vote results (CrossTransactionVote) from core
    rx_order_ctx: Receiver<CrossTransactionVote>,
    /// Output channel to deliver sealed vote results to the `QuorumWaiter`.
    tx_message: Sender<QuorumVoteMessage>,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl VoteMaker {
    pub fn spawn(
        name: PublicKey,
        committee: ExecutionCommittee,
        rx_order_ctx: Receiver<CrossTransactionVote>,
        tx_message: Sender<QuorumVoteMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                rx_order_ctx,
                tx_message,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming vote results of cross-shard transactions from core
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(ctx_vote) = self.rx_order_ctx.recv() => {
                    self.seal(ctx_vote).await;
                },
            }
        }
    }

    /// Seal and broadcast the vote results.
    async fn seal(&mut self, ctx_vote: CrossTransactionVote) {
        let message = ConsensusMessage::CrossTransactionVote(ctx_vote.clone());
        // Serialize the vote results.
        let serialized = bincode::serialize(&message).expect("Failed to serialize vote results");

        // Broadcast the vote results through the (consensus) network.
        let (names, consensus_addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(consensus_addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        let vote_serialized =
            bincode::serialize(&ctx_vote).expect("Failed to serialize vote results");
        self.tx_message
            .send(QuorumVoteMessage {
                vote: vote_serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
