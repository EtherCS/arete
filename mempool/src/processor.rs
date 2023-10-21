use crate::mempool::MempoolMessage;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use types::CBlock;
use log::warn;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        // tx_digest: Sender<Digest>,
        // ARETE: replace 'tx_digest: Sender<Digest>' with tx_CBlock
        tx_cblock: Sender<CBlock>,
    ) {
        tokio::spawn(async move {
            // receive CBlock from its own mempool or peers' mempool
            while let Some(batch) = rx_batch.recv().await {
                // ARETE: send cblock to consensus
                match bincode::deserialize(&batch) {
                    Ok(MempoolMessage::Batch(..)) => {},
                    Ok(MempoolMessage::BatchRequest(_missing, _requestor)) => {},
                    Ok(MempoolMessage::CBlock(tx)) => {
                        // debug!("Mempool processor get a CBlock {:?}", tx);
                        // Hash the batch.
                        let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                        // // Store the batch.
                        store.write(digest.to_vec(), batch).await;

                        tx_cblock.send(tx).await.expect("Failed to send cblock");
                    },
                    Err(e) => warn!("Serialization error: {}", e),
                }
                

                // tx_digest.send(digest).await.expect("Failed to send digest");
            }
        });
    }
}
