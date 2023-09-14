use crate::{config::{ExecutionCommittee, Stake}, consensus::ConsensusMessage};
use bytes::Bytes;
use crypto::PublicKey;
use types::ConfirmMessage;
use network::{CancelHandler, ReliableSender};
use tokio::sync::mpsc::Receiver;
use log::debug;
use std::net::SocketAddr;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;


pub struct ConfirmExecutor {
    name: PublicKey,
    committee: ExecutionCommittee,
    rx_confirm_message: Receiver<ConfirmMessage>,
    network: ReliableSender,
}

impl ConfirmExecutor {
    pub fn spawn(
        name: PublicKey,
        committee: ExecutionCommittee,
        rx_confirm_message: Receiver<ConfirmMessage>,   // receive from mempool
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                rx_confirm_message,
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

    /// Broadcast the confirmation message to other executors.
    async fn broadcast_confirm_msg(&mut self, confirm_msg: ConfirmMessage) {
        debug!("Broadcasting confirmation msg {:?}", confirm_msg);
        let (names, addresses): (Vec<_>, Vec<SocketAddr>) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::ConfirmMsg(confirm_msg.clone()))
            .expect("Failed to serialize confirmation latency");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;
        // Control system: Wait for f+1 nodes to acknowledge our confirmation message before continuing.
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
        while let Some(message) = self.rx_confirm_message.recv().await {
            self.broadcast_confirm_msg(message).await;
        }
    }
}