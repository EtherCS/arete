use crypto::PublicKey;
use crypto::{Digest, Hash};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use types::EBlock;

// #[cfg(test)]
// #[path = "tests/processor_tests.rs"]
// pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::SerializedEBlockMessage` message.
pub type SerializedEBlockMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The name of the executor
        name: PublicKey,
        // The persistent storage.
        // Store <Hash, EBlock>
        mut store: Store,
        // Input channel to receive CBlock.
        mut rx_cblock: Receiver<EBlock>,
        // Output channel to send out batches' digests.
        // tx_to_certify_digest: Sender<CBlock>,
    ) {
        tokio::spawn(async move {
            // Receive cblock after certifying (get f+1 signatures)
            while let Some(eblock) = rx_cblock.recv().await {
                // Hash the cblock.
                let digest = eblock.digest();
                let value = bincode::serialize(&eblock).expect("Failed to serialize cblock");

                // Store the cblock.
                store.write(digest.to_vec(), value).await;

                // Send to channel, wait to send to the ordering shard
                // if cblock.author == name {
                //     tx_to_certify_digest.send(cblock).await.expect("Failed to send cblock to certify");
                // }
            }
        });
    }
}
