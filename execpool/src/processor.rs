use crypto::Hash;
use store::Store;
use tokio::sync::mpsc::Receiver;
use types::EBlock;

/// Indicates a serialized `MempoolMessage::SerializedEBlockMessage` message.
pub type SerializedEBlockMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        // Store <Hash, EBlock>
        mut store: Store,
        // Input channel to receive CBlock.
        mut rx_eblock: Receiver<EBlock>,
    ) {
        tokio::spawn(async move {
            // Receive cblock after certifying (get f+1 signatures)
            while let Some(eblock) = rx_eblock.recv().await {
                // Hash the cblock.
                let digest = eblock.digest();
                let value = bincode::serialize(&eblock).expect("Failed to serialize cblock");

                // Store the cblock.
                store.write(digest.to_vec(), value).await;
            }
        });
    }
}
