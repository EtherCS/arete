use crate::batch_maker::{Batch, BatchMaker};
use crate::config::{Committee, Parameters};
use crate::helper::Helper;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use types::{CBlock, CertifyMessage, CrossTransactionVote};

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

/// The message exchanged between the nodes' mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum MempoolMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
    CBlock(CBlock),
}

/// The messages sent by the consensus and the mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConsensusMempoolMessage {
    /// The consensus notifies the mempool that it need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The consensus notifies the mempool of a round update.
    Cleanup(Round),
}

pub struct Mempool {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
    /// Send messages to consensus.
    // tx_consensus: Sender<Digest>,
    /// Send vote results to consensus
    // tx_ctx_vote: Sender<CrossTransactionVote>,
    /// Send cblock (consensus) to consensus.
    tx_cblock: Sender<CBlock>,
}

impl Mempool {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        rx_consensus: Receiver<ConsensusMempoolMessage>,
        tx_ctx_vote: Sender<CrossTransactionVote>,
        tx_cblock: Sender<CBlock>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        // Define a mempool instance.
        let mempool = Self {
            name,
            committee,
            parameters,
            store,
            // tx_ctx_vote,
            tx_cblock,
        };

        // Spawn all mempool tasks.
        mempool.handle_consensus_messages(rx_consensus);
        mempool.handle_clients_transactions(tx_ctx_vote);
        mempool.handle_mempool_messages();

        info!(
            "Mempool successfully booted on {}",
            mempool
                .committee
                .mempool_address(&mempool.name)
                .expect("Our public key is not in the committee")
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from the consensus.
    fn handle_consensus_messages(&self, rx_consensus: Receiver<ConsensusMempoolMessage>) {
        // The `Synchronizer` is responsible to keep the mempool in sync with the others. It handles the commands
        // it receives from the consensus (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_consensus,
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, tx_ctx_vote: Sender<CrossTransactionVote>) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        // let (tx_vote_maker, _rx_vote_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .transactions_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            TxReceiverHandler {
                tx_batch_maker,
                tx_ctx_vote,
            },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other mempools that share the same `id` as us. Finally,
        // it gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* mempool_addresses */
            self.committee.broadcast_addresses(&self.name),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the consensus.
        Processor::spawn(
            self.store.clone(),
            /* rx_batch */ rx_processor,
            // /* tx_digest */ self.tx_consensus.clone(),
            /* tx_cblock */
            self.tx_cblock.clone(),
        );

        info!("Mempool listening to client transactions on {}", address);
    }

    /// Spawn all tasks responsible to handle messages from other mempools.
    fn handle_mempool_messages(&self) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other mempools.
        let mut address = self
            .committee
            .mempool_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            MempoolReceiverHandler {
                tx_helper,
                tx_processor,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other mempools.
        Helper::spawn(
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the batches we receive from the other mempools. It then forwards the
        // batch's digest to the consensus.
        Processor::spawn(
            self.store.clone(),
            /* rx_batch */ rx_processor,
            // /* tx_digest */ self.tx_consensus.clone(),
            /* tx_cblock */
            self.tx_cblock.clone(),
        );

        info!("Mempool listening to mempool messages on {}", address);
    }
}

/// Defines how the network receiver handles incoming transactions (cblocks).
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<CBlock>,
    // tx_vote_maker: Sender<CrossTransactionVote>,
    tx_ctx_vote: Sender<CrossTransactionVote>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized) {
            // Receive Batch: send it to processor
            // serialized is SerializedBatchMessage (i.e., serialized Vec<Transaction>)), which is generated by QuorumWaiter
            Ok(CertifyMessage::CBlock(cblock)) => {
                debug!(
                    "Ordering receive cblock from shard {}, cblock {:?}, num of signatures {}",
                    cblock.round,
                    cblock,
                    cblock.multisignatures.len()
                );
                self.tx_batch_maker
                    .send(cblock)
                    .await
                    .expect("Failed to send batch")
            }
            Ok(CertifyMessage::CtxVote(votes)) => {
                debug!(
                    "Ordering receive vote from shard {}, order round {}, num of signatures {}",
                    votes.shard_id,
                    votes.order_round,
                    votes.multisignatures.len()
                );
                self.tx_ctx_vote
                    .send(votes)
                    .await
                    .expect("Failed to send batch request")
            }
            Err(e) => warn!("Serialization error: {}", e),
        }

        // let rec_block: CBlock = bincode::deserialize(&message.to_vec())
        //     .expect("fail to deserialize the CBlock");
        // info!("node receives msg {:?}, get shard id is {}", rec_block, rec_block.shard_id);
        // // Send the transaction to the batch maker.
        // self.tx_batch_maker
        //     .send(rec_block)
        //     .await
        //     .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming mempool messages.
#[derive(Clone)]
struct MempoolReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for MempoolReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            // Receive Batch: send it to processor
            // serialized is SerializedBatchMessage (i.e., serialized Vec<Transaction>)), which is generated by QuorumWaiter
            Ok(MempoolMessage::Batch(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
            Ok(MempoolMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Ok(MempoolMessage::CBlock(_cblock)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}
