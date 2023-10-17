use crate::batch_maker::BatchMaker;
use crate::config::{CertifyParameters, ExecutionCommittee};
use crate::helper::Helper;
use crate::processor::Processor;
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, Hash, PublicKey, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use types::{CBlock, ConfirmMessage, EBlock, NodeSignature, ShardInfo, Transaction};

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// ARETE TODO: support resolve transaction to calculate the ratio of cross-shard txs
pub const RATIO_OF_CTX: f32 = 0.3;

/// The consensus round number.
pub type Round = u64;

/// The message exchanged between the nodes' mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum MempoolMessage {
    EBlock(EBlock),
    EBlockRequest(Digest, /* origin */ PublicKey),
    // Batch(Batch),
    // BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

/// The messages sent by the consensus and the mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutionMempoolMessage {
    /// The execution notifies the mempool that it need to sync the target missing EBlock.
    Synchronize(Digest, /* target */ PublicKey),
    /// The consensus notifies the mempool of a round update.
    Cleanup(Round),
}

pub struct Mempool {
    /// The public key of this authority.
    name: PublicKey,
    /// The execution shard information
    shard_info: ShardInfo,
    /// The signature service
    signature_service: SignatureService,
    /// The committee information.
    committee: ExecutionCommittee,
    /// The configuration parameters.
    parameters: CertifyParameters,
    /// The persistent storage.
    store: Store,
    /// Send messages to certify.
    tx_certify: Sender<CBlock>,
    /// Send confirmation messages to consensus
    tx_confirm: Sender<ConfirmMessage>,
}

impl Mempool {
    pub fn spawn(
        name: PublicKey,
        shard_info: ShardInfo,
        signature_service: SignatureService,
        committee: ExecutionCommittee,
        parameters: CertifyParameters,
        store: Store,
        rx_consensus: Receiver<ExecutionMempoolMessage>,
        tx_certify: Sender<CBlock>,
        tx_confirm: Sender<ConfirmMessage>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        // Define a mempool instance.
        let mempool = Self {
            name,
            shard_info,
            signature_service,
            committee,
            parameters,
            store,
            tx_certify,
            tx_confirm,
        };

        // Spawn all mempool tasks.
        mempool.handle_execution_messages(rx_consensus);
        mempool.handle_clients_transactions();
        mempool.handle_mempool_messages();
        mempool.handle_ordering_messages();

        info!(
            "Mempool successfully booted on {}",
            mempool
                .committee
                .mempool_address(&mempool.name)
                .expect("Our public key is not in the committee")
                .ip()
        );
        info!(
            "Confirmation address is {}",
            mempool
                .committee
                .confirmation_address(&mempool.name)
                .expect("Our public key is not in the committee")
        );
    }

    /// Spawn all tasks responsible to handle messages from the consensus.
    fn handle_execution_messages(&self, rx_consensus: Receiver<ExecutionMempoolMessage>) {
        // The `Synchronizer` is responsible to keep the mempool in sync with the others. It handles the commands
        // it receives from the consensus (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.certify_gc_depth,
            self.parameters.certify_sync_retry_delay,
            self.parameters.certify_sync_retry_nodes,
            /* rx_message */ rx_consensus,
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
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
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other mempools that share the same `id` as us. Finally,
        // it gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.name,
            self.shard_info.clone(),
            self.signature_service.clone(),
            RATIO_OF_CTX,
            self.parameters.certify_batch_size,
            self.parameters.certify_max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* mempool_addresses */
            self.committee.broadcast_addresses(&self.name),
        );

        // The `QuorumWaiter` waits for (1-f_L) authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.name,
            self.shard_info.clone(),
            self.signature_service.clone(),
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            tx_processor,
            self.tx_certify.clone(),
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the consensus.
        // ARETE: 'Processor' will send this to the ordering shard instead of the consensus (certify)
        Processor::spawn(
            self.store.clone(),
            /* rx_batch */ rx_processor,
            // /* tx_digest */ self.tx_certify.clone(),
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
                name: self.name,
                signature_service: self.signature_service.clone(),
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
            // /* tx_digest */ self.tx_certify.clone(),
        );

        info!("Mempool listening to mempool messages on {}", address);
    }

    /// Spawn all tasks responsible to handle confirmation messages from the ordering shard.
    fn handle_ordering_messages(&self) {
        // Receive incoming messages from the ordering shard.
        let mut address = self
            .committee
            .confirmation_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        debug!("Executor listen confirmation address {:?}", address);
        NetworkReceiver::spawn(
            address,
            /* handler */
            ConfirmationMsgReceiverHandler {
                tx_confirm: self.tx_confirm.clone(),
            },
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming confirmation messages.
#[derive(Clone)]
struct ConfirmationMsgReceiverHandler {
    tx_confirm: Sender<ConfirmMessage>,
}

#[async_trait]
impl MessageHandler for ConfirmationMsgReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        let confirm_msg: ConfirmMessage =
            bincode::deserialize(&message).expect("Failed to deserialize confirmation message");
        self.tx_confirm
            .send(confirm_msg.clone())
            .await
            .expect("Failed to send confirmation message");
        #[cfg(feature = "benchmark")]
        {
            info!("executor receives confirm msg {:?}", confirm_msg);
            
            info!(
                "ARETE shard {} commit blocks for ordering round {}",
                confirm_msg.shard_id, confirm_msg.order_round
            );
        }
        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming mempool messages.
#[derive(Clone)]
struct MempoolReceiverHandler {
    tx_helper: Sender<(Digest, PublicKey)>,
    tx_processor: Sender<EBlock>,
    name: PublicKey,
    signature_service: SignatureService,
}

#[async_trait]
impl MessageHandler for MempoolReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        // ARETE: parse the received EBlock, sign it, send the signature back
        // let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(MempoolMessage::EBlock(eblock)) => {
                // Get digests of ctxs
                let mut hash_ctxs: Vec<Digest> = Vec::new();
                for ctx in &eblock.crosstxs {
                    hash_ctxs.push(Digest(
                        Sha512::digest(&ctx).as_slice()[..32].try_into().unwrap(),
                    ));
                }
                // Send cblock to processor for storing
                self.tx_processor
                    .send(eblock.clone())
                    .await
                    .expect("Failed to send EBlock to store");
                let cblock = CBlock::new(
                    eblock.shard_id,
                    eblock.author,
                    eblock.round,
                    eblock.digest(),
                    hash_ctxs.clone(),
                    Vec::new(),
                )
                .await;
                // Send signature to the EBlock creator
                let mut signature_service = self.signature_service.clone();
                let signature = signature_service.request_signature(cblock.digest()).await;
                let node_signature = NodeSignature::new(self.name, signature).await;

                let cblock_serialized = bincode::serialize(&node_signature.clone())
                    .expect("Failed to serialize received CBlock");
                let cblock_bytes = Bytes::from(cblock_serialized.clone());
                let _ = writer.send(cblock_bytes).await;
            }
            Ok(MempoolMessage::EBlockRequest(missing, requestor)) => {
                self.tx_helper
                    .send((missing, requestor))
                    .await
                    .expect("Failed to send batch request");
                let _ = writer.send(Bytes::from("Ack")).await;
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}
