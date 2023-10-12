use crate::config::{CertifyParameters, ExecutionCommittee};
use crate::confirm_executor::ConfirmExecutor;
use crate::core::Core;
use crate::error::ConsensusError;
use crate::helper::Helper;
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
// use crate::messages::{Timeout, Vote, TC};
use crate::proposer::Proposer;
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use crate::vote_maker::VoteMaker;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use execpool::ExecutionMempoolMessage;
use futures::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use types::{CBlock, ConfirmMessage, EBlock, ShardInfo, CertifyMessage};

// #[cfg(test)]
// #[path = "tests/consensus_tests.rs"]
// pub mod consensus_tests;

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    ExecutionBlock(EBlock),
    ConfirmMsg(ConfirmMessage),
}

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: ExecutionCommittee,
        parameters: CertifyParameters,
        shard_info: ShardInfo,
        signature_service: SignatureService,
        store: Store,
        rx_mempool: Receiver<CBlock>, // receive channel from execpool
        tx_mempool: Sender<ExecutionMempoolMessage>, // send to execpool for synchronizing missing eblock
        tx_certify: Sender<CertifyMessage>,
        rx_confirm: Receiver<ConfirmMessage>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_loopback, rx_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_order_ctx, rx_order_ctx) = channel(CHANNEL_CAPACITY);
        let (tx_vote_quorum_waiter, rx_vote_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);

        // Spawn the network receiver.
        let mut address = committee
            .address(&name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            ConsensusReceiverHandler {
                tx_consensus,
                tx_helper,
            },
        );
        info!(
            "Node {} listening to consensus messages on {}",
            name, address
        );

        info!("Liveness threshold is: {}", committee.liveness_threshold);

        // Make the leader election module.
        // let leader_elector = LeaderElector::new(committee.clone());

        // // Make the mempool driver.
        let mempool_driver = MempoolDriver::new(store.clone(), tx_mempool, tx_loopback.clone());

        // // Make the synchronizer.
        let synchronizer = Synchronizer::new(
            name,
            committee.clone(),
            store.clone(),
            tx_loopback.clone(),
            parameters.certify_sync_retry_delay,
        );

        // Spawn the consensus core.
        Core::spawn(
            name,
            committee.clone(),
            shard_info.clone(),
            signature_service.clone(),
            store.clone(),
            mempool_driver,
            synchronizer,
            rx_consensus,
            rx_loopback,
            tx_order_ctx,
            tx_certify.clone(),
        );

        // Spawn the vote_maker
        VoteMaker::spawn(
            name,
            committee.clone(),
            shard_info.clone(),
            signature_service.clone(),
            rx_order_ctx,
            tx_vote_quorum_waiter,
        );

        /// Spawn the quorum_waiter
        QuorumWaiter::spawn(
            name,
            shard_info.clone(),
            committee.clone(),
            committee.stake(&name),
            rx_vote_quorum_waiter,
            tx_certify.clone(),
        );
        
        // Spawn the block proposer.
        Proposer::spawn(
            // name,
            // committee.clone(),
            // shard_info.clone(),
            // signature_service.clone(),
            rx_mempool, tx_certify,
            // /* rx_message */ rx_proposer,
            // tx_loopback,
        );

        // Spawn the confirm executor.
        ConfirmExecutor::spawn(name, committee.clone(), rx_confirm, tx_consensus);

        // Spawn the helper module.
        Helper::spawn(committee, store, /* rx_requests */ rx_helper);
    }
}

/// Defines how the network receiver handles incoming messages from other executors.
#[derive(Clone)]
struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
    tx_helper: Sender<(Digest, PublicKey)>,
}

#[async_trait]
impl MessageHandler for ConsensusReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(ConsensusError::SerializationError)? {
            // ConsensusMessage::SyncRequest(missing, origin) => self
            //     .tx_helper
            //     .send((missing, origin))
            //     .await
            //     .expect("Failed to send consensus message"),
            message @ ConsensusMessage::ConfirmMsg(..) => {
                // receive a consensus msg (a block is confirm) from other executors
                // Reply with an ACK.
                let _ = writer.send(Bytes::from("Ack")).await;

                self.tx_consensus
                    .send(message)
                    .await
                    .expect("Failed to send confirm message to core")
            }
            message @ ConsensusMessage::ExecutionBlock(..) => {
                // self.
            }
            message => self
                .tx_consensus
                .send(message)
                .await
                .expect("Failed to consensus message"),
        }
        Ok(())
    }
}
