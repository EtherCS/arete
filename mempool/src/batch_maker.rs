use crate::mempool::MempoolMessage;
use crate::quorum_waiter::QuorumWaiterMessage;
use bytes::Bytes;
use crypto::PublicKey;
// use log::info;
use network::ReliableSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use types::{CBlock, CrossTransactionVote};

// pub type Transaction = Vec<u8>;
pub type CBlockBatch = Vec<CBlock>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions (cblock) from the network.
    rx_transaction: Receiver<CBlock>,
    /// receive vote from transaction channel
    rx_vote_tx: Receiver<CrossTransactionVote>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: CBlockBatch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<CBlock>,
        rx_vote_tx: Receiver<CrossTransactionVote>,
        tx_message: Sender<QuorumWaiterMessage>,
        mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                rx_vote_tx,
                tx_message,
                mempool_addresses,
                current_batch: CBlockBatch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Receive transaction (CBlock).
                Some(cblock) = self.rx_transaction.recv() => {
                    // Broadcast to other peers and send it to consensus for further processing
                    // let serialize_cblock = bincode::serialize(&transaction)
                    //     .expect("Fail to serialize transaction");
                    // info!("ARETE trace: start cblock size {}", cblock.size());
                    self.current_batch_size += cblock.size();
                    self.current_batch.push(cblock);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                    
                },
                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
                Some(vote) = self.rx_vote_tx.recv() => {
                    self.broadcast_vote(vote).await;
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        // info!("ARETE trace: start batch {} cblocks", self.current_batch.len());
        // Serialize the batch.
        self.current_batch_size = 0;
        // let batch: Vec<_> = self.current_batch.drain(..).collect();
        // ARETE: replace 'batch' with 'Transaction'
        // Only send the first transaction of the batch
        // let tx = batch[0].clone();
        let cblock_batch: Vec<_> = self.current_batch.drain(..).collect();
        // let cblock: CBlock = bincode::deserialize(&tx).expect("fail to deserialize the CBlock");
        let message = MempoolMessage::Batch(cblock_batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        // Broadcast the batch through the network.
        // A batch is a Vec<Transaction>
        let (names, addresses): (Vec<_>, _) = self.mempool_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }

    async fn broadcast_vote(&mut self, vote: CrossTransactionVote) {
        // info!("ARETE trace: start send vote");
        let message = MempoolMessage::CrossTransactionVote(vote);
        let serialized = bincode::serialize(&message).expect("Failed to serialize vote");

        // Broadcast the batch through the network.
        // A batch is a Vec<Transaction>
        let (_, addresses): (Vec<_>, _) = self.mempool_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let _ = self.network.broadcast(addresses, bytes).await;
    }
}
