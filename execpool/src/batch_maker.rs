use crate::mempool::MempoolMessage;
use crate::quorum_waiter::QuorumWaiterMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Hash;
use crypto::{PublicKey, SignatureService};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use types::{EBlock, Round, ShardInfo, Transaction};

// pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

// ARETE: TODO: if need round?
pub const EXECUTION_ROUND: Round = 1;

// Limit the number of cross-shard transactions per EBlock
pub const CTX_NUM_PER_EBLOCK: usize = 10;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The node
    name: PublicKey,
    /// The execution shard information
    shard_info: ShardInfo,
    /// The signagure service
    signature_service: SignatureService,
    /// The ratio of cross-shard txs (TODO: remove)
    ratio_cross_shard_txs: f32,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed EBlock to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl BatchMaker {
    pub fn spawn(
        name: PublicKey,
        shard_info: ShardInfo,
        signature_service: SignatureService,
        ratio_cross_shard_txs: f32,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                shard_info,
                signature_service,
                ratio_cross_shard_txs,
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                mempool_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
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
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
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
                    // Control the sending rate to the ordering shard
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let ctx_num = CTX_NUM_PER_EBLOCK.min((batch.len() as f32 * self.ratio_cross_shard_txs).floor() as usize);
        let intratxs = &batch[ctx_num..batch.len()].to_vec();
        let crosstxs = &batch[0..ctx_num].to_vec();
        // ARETE: use Transactions to create a EBlock
        let eblock = EBlock::new(
            self.shard_info.id,
            self.name,
            EXECUTION_ROUND,
            intratxs.clone(),
            crosstxs.clone(),
            self.signature_service.clone(),
        )
        .await;
        let message = MempoolMessage::EBlock(eblock.clone());
        let serialized = bincode::serialize(&message)
            .expect("Failed to serialize our own MempoolMessage EBlock");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = eblock.digest();
            let id_ctx_num = (tx_ids.len() as f32 * self.ratio_cross_shard_txs).floor() as usize;
            for id in tx_ids[id_ctx_num..tx_ids.len()].to_vec() {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }
            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.mempool_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        let eblock_serialized =
            bincode::serialize(&eblock).expect("Failed to serialize our own EBlock");
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: eblock_serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
