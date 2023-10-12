use crate::config::ExecutionCommittee;
use crate::quorum_waiter::QuorumVoteMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::{PublicKey, SignatureService};
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use tokio::sync::mpsc::{Receiver, Sender};
use types::{Transaction, ShardInfo, Round, CrossTransactionVote};

// pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

// ARETE: TODO: if need round?
pub const EXECUTION_ROUND: Round = 1;

/// Assemble clients transactions into batches.
pub struct VoteMaker {
    /// The node
    name: PublicKey,
    /// The committee information.
    committee: ExecutionCommittee,
    /// The execution shard information
    shard_info: ShardInfo,
    /// The signagure service
    signature_service: SignatureService,
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
        shard_info: ShardInfo,
        signature_service: SignatureService,
        rx_order_ctx: Receiver<CrossTransactionVote>,
        tx_message: Sender<QuorumVoteMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                shard_info,
                signature_service,
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
        // Serialize the vote results.
        let serialized = bincode::serialize(&ctx_vote).expect("Failed to serialize vote results");

        // Broadcast the vote results through the network.
        let (names, consensus_addresses): (Vec<_>, _) = self.committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(consensus_addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumVoteMessage {
                vote: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
