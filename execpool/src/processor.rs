use crypto::{Digest, Hash};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use types::CBlock;

// #[cfg(test)]
// #[path = "tests/processor_tests.rs"]
// pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::SerializedEBlockMessage` message.
pub type SerializedEBlockMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<CBlock>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            // Receive cblock after certifying (get f+1 signatures)
            while let Some(cblock) = rx_batch.recv().await {
                // Hash the cblock.
                let digest = cblock.digest();
                let value = bincode::serialize(&cblock).expect("Failed to serialize cblock");
                // Store the cblock.
                store.write(digest.to_vec(), value).await;

                tx_digest.send(digest).await.expect("Failed to send digest");
            }
        });
    }
}
